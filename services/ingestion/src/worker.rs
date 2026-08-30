//! Authenticated file-command contract for the long-running ingestion worker
//! (SPEC-067). This module deliberately owns validation and canonical key
//! derivation; runtime transports must not reconstruct scope or object paths.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
const SCHEMA_MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 1024;
const REQUIRED_SCOPE_HEADERS: [&str; 3] = ["eci-tenant-id", "eci-repository", "eci-acl-group"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandErrorKind {
    InvalidPayload,
    InvalidScope,
    InvalidPath,
    SourceTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError {
    kind: CommandErrorKind,
}

impl CommandError {
    fn new(kind: CommandErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(&self) -> CommandErrorKind {
        self.kind
    }
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never echo payload/header/path values at this boundary.
        write!(f, "invalid ingestion command ({:?})", self.kind)
    }
}

impl std::error::Error for CommandError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedCommitScope {
    tenant_id: String,
    repository: String,
    acl_group: String,
}

impl AuthenticatedCommitScope {
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn repository(&self) -> &str {
        &self.repository
    }

    pub fn acl_group(&self) -> &str {
        &self.acl_group
    }

    pub(crate) fn ingestion_scope(&self) -> crate::IngestionScope {
        // Construction cannot fail: the exact same scalar predicate is
        // enforced while parsing authenticated headers below.
        crate::IngestionScope::new(&self.tenant_id, &self.repository, &self.acl_group)
            .expect("authenticated scope was already validated")
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IngestionFileCommand {
    schema_version: String,
    command_id: Uuid,
    commit_sha: String,
    path: String,
    source_sha256: String,
    source_size_bytes: u64,
}

impl IngestionFileCommand {
    pub fn command_id(&self) -> Uuid {
        self.command_id
    }

    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn source_sha256(&self) -> &str {
        &self.source_sha256
    }

    pub fn source_size_bytes(&self) -> u64 {
        self.source_size_bytes
    }
}

pub fn parse_authenticated_command(
    payload: &[u8],
    headers: &[(String, Vec<u8>)],
    max_source_bytes: u64,
) -> Result<(AuthenticatedCommitScope, IngestionFileCommand), CommandError> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(CommandError::new(CommandErrorKind::InvalidPayload));
    }

    let mut values: [Option<String>; 3] = [None, None, None];
    for (name, raw) in headers {
        if let Some(index) = REQUIRED_SCOPE_HEADERS
            .iter()
            .position(|required| name == required)
        {
            if values[index].is_some() {
                return Err(CommandError::new(CommandErrorKind::InvalidScope));
            }
            let value = std::str::from_utf8(raw)
                .map_err(|_| CommandError::new(CommandErrorKind::InvalidScope))?;
            if !valid_scope_value(value) {
                return Err(CommandError::new(CommandErrorKind::InvalidScope));
            }
            values[index] = Some(value.to_owned());
        }
    }
    let [Some(tenant_id), Some(repository), Some(acl_group)] = values else {
        return Err(CommandError::new(CommandErrorKind::InvalidScope));
    };

    let command: IngestionFileCommand = serde_json::from_slice(payload)
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidPayload))?;
    if command.schema_version != "1"
        || !lower_hex(&command.commit_sha, 40)
        || !lower_hex(&command.source_sha256, 64)
    {
        return Err(CommandError::new(CommandErrorKind::InvalidPayload));
    }
    validate_path(&command.path)?;
    let effective_limit = max_source_bytes.min(SCHEMA_MAX_SOURCE_BYTES);
    if command.source_size_bytes > effective_limit {
        return Err(CommandError::new(CommandErrorKind::SourceTooLarge));
    }

    Ok((
        AuthenticatedCommitScope {
            tenant_id,
            repository,
            acl_group,
        },
        command,
    ))
}

fn valid_scope_value(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= 256
        && !value.chars().any(char::is_control)
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_path(path: &str) -> Result<(), CommandError> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || !matches!(
            std::path::Path::new(path)
                .extension()
                .and_then(|value| value.to_str()),
            Some("go" | "js" | "ts")
        )
    {
        return Err(CommandError::new(CommandErrorKind::InvalidPath));
    }
    Ok(())
}

pub fn source_object_key(
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
) -> String {
    let scope_hash = digest_components(&[scope.tenant_id(), scope.repository(), scope.acl_group()]);
    format!(
        "sources/v1/{scope_hash}/{}/{}",
        command.commit_sha(),
        percent_encode_path(command.path())
    )
}

pub fn command_message_key(
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
) -> String {
    digest_components(&[scope.tenant_id(), scope.repository(), command.path()])
}

pub fn command_message_key_matches(
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
    actual: Option<&[u8]>,
) -> bool {
    actual.is_some_and(|value| value == command_message_key(scope, command).as_bytes())
}

pub fn command_fingerprint(
    scope: &AuthenticatedCommitScope,
    command: &IngestionFileCommand,
) -> String {
    digest_components(&[
        scope.tenant_id(),
        scope.repository(),
        scope.acl_group(),
        command.commit_sha(),
        command.path(),
        command.source_sha256(),
        &command.source_size_bytes().to_string(),
    ])
}

fn digest_components(components: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for component in components {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn percent_encode_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}
