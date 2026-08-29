use ingestion::{scoped_node_id, IngestionScope};

#[test]
fn ingestion_scope_requires_non_blank_security_labels() {
    let valid = IngestionScope::new("tenant-a", "repo-a", "engineering")
        .expect("valid scope");
    assert_eq!(valid.tenant_id(), "tenant-a");
    assert_eq!(valid.repo(), "repo-a");
    assert_eq!(valid.acl_group(), "engineering");

    for (tenant, repo, group) in [
        ("", "repo-a", "engineering"),
        ("tenant-a", " ", "engineering"),
        ("tenant-a", "repo-a", "\t"),
        ("tenant-a\r\nforged", "repo-a", "engineering"),
    ] {
        assert!(IngestionScope::new(tenant, repo, group).is_err());
    }
}

#[test]
fn persisted_node_identity_is_owned_by_tenant_and_repository_not_acl() {
    let tenant_a = IngestionScope::new("tenant-a", "repo", "developers").unwrap();
    let tenant_b = IngestionScope::new("tenant-b", "repo", "developers").unwrap();
    let repo_b = IngestionScope::new("tenant-a", "repo-b", "developers").unwrap();
    let changed_acl = IngestionScope::new("tenant-a", "repo", "security").unwrap();

    let a = scoped_node_id(&tenant_a, "parser-local-id");
    assert_eq!(a.len(), 64);
    assert_ne!(a, scoped_node_id(&tenant_b, "parser-local-id"));
    assert_ne!(a, scoped_node_id(&repo_b, "parser-local-id"));
    assert_eq!(a, scoped_node_id(&changed_acl, "parser-local-id"));
}

#[test]
fn persisted_node_identity_is_length_delimited() {
    let left = IngestionScope::new("a", "bc", "group").unwrap();
    let right = IngestionScope::new("ab", "c", "group").unwrap();
    assert_ne!(scoped_node_id(&left, "d"), scoped_node_id(&right, "d"));
}
