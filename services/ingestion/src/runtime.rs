//! Long-running Kafka/MinIO/PostgreSQL runtime for SPEC-067.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use minio::s3::creds::StaticProvider;
use minio::s3::error::{Error as MinioError, S3ServerError};
use minio::s3::http::BaseUrl;
use minio::s3::types::minio_error_response::MinioErrorCode;
use minio::s3::types::S3Api;
use minio::s3::MinioClient;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::worker::{command_message_key_matches, parse_authenticated_command, source_object_key};
use crate::{parse_file_full_private, persist_ingestion_command};

const INPUT_TOPIC: &str = "eci.ingestion.file.v1";
const DLQ_TOPIC: &str = "eci.ingestion.file.v1.DLQ";
const CONSUMER_GROUP: &str = "ingestion";

#[derive(Clone)]
struct Config {
    postgres_dsn: String,
    kafka_brokers: String,
    kafka_ca: String,
    kafka_cert: String,
    kafka_key: String,
    minio_bucket: String,
    max_source_bytes: u64,
    metrics_address: SocketAddr,
}

impl Config {
    fn from_env() -> Result<(Self, MinioClient), RuntimeError> {
        let required = |name: &str| {
            std::env::var(name).map_err(|_| RuntimeError::Config("required setting missing"))
        };
        if required("KAFKA_TOPIC")? != INPUT_TOPIC || required("KAFKA_GROUP_ID")? != CONSUMER_GROUP
        {
            return Err(RuntimeError::Config("Kafka topic/group override rejected"));
        }
        let max_source_bytes = required("ECI_INGESTION_MAX_SOURCE_BYTES")?
            .parse::<u64>()
            .map_err(|_| RuntimeError::Config("invalid source size limit"))?;
        if max_source_bytes == 0 || max_source_bytes > 16 * 1024 * 1024 {
            return Err(RuntimeError::Config("invalid source size limit"));
        }
        let metrics_address = required("ECI_METRICS_ADDRESS")?
            .parse()
            .map_err(|_| RuntimeError::Config("invalid metrics address"))?;
        let endpoint: BaseUrl = required("MINIO_ENDPOINT")?
            .parse()
            .map_err(|_| RuntimeError::Config("invalid MinIO endpoint"))?;
        let postgres_dsn = required("POSTGRES_DSN")?;
        postgres_config_with_deadlines(&postgres_dsn)
            .map_err(|_| RuntimeError::Config("invalid PostgreSQL configuration"))?;
        let access_key = required("MINIO_ACCESS_KEY")?;
        let secret_key = required("MINIO_SECRET_KEY")?;
        let credentials = StaticProvider::new(&access_key, &secret_key, None);
        let minio = MinioClient::new(endpoint, Some(credentials), None, None)
            .map_err(|_| RuntimeError::Config("invalid MinIO client configuration"))?;
        Ok((
            Self {
                postgres_dsn,
                kafka_brokers: required("KAFKA_BROKERS")?,
                kafka_ca: required("KAFKA_TLS_CA_FILE")?,
                kafka_cert: required("KAFKA_TLS_CERT_FILE")?,
                kafka_key: required("KAFKA_TLS_KEY_FILE")?,
                minio_bucket: required("MINIO_BUCKET")?,
                max_source_bytes,
                metrics_address,
            },
            minio,
        ))
    }
}

#[derive(Debug)]
pub enum RuntimeError {
    Config(&'static str),
    Startup(&'static str),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(message) => write!(f, "ingestion configuration error: {message}"),
            Self::Startup(message) => write!(f, "ingestion startup error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Clone)]
struct HealthState {
    live: Arc<AtomicBool>,
    kafka: Arc<AtomicBool>,
    postgres: Arc<AtomicBool>,
    minio: Arc<AtomicBool>,
    metrics: Metrics,
}

impl HealthState {
    fn new(metrics: Metrics) -> Self {
        Self {
            live: Arc::new(AtomicBool::new(true)),
            kafka: Arc::new(AtomicBool::new(false)),
            postgres: Arc::new(AtomicBool::new(false)),
            minio: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    fn ready(&self) -> bool {
        self.kafka.load(Ordering::Relaxed)
            && self.postgres.load(Ordering::Relaxed)
            && self.minio.load(Ordering::Relaxed)
    }

    fn set_dependency(&self, dependency: &'static str, ready: bool) {
        let value = if ready { 1 } else { 0 };
        match dependency {
            "kafka" => self.kafka.store(ready, Ordering::Relaxed),
            "postgres" => self.postgres.store(ready, Ordering::Relaxed),
            "minio" => self.minio.store(ready, Ordering::Relaxed),
            _ => return,
        }
        self.metrics
            .ready
            .with_label_values(&[dependency])
            .set(value);
    }
}

#[derive(Clone)]
struct Metrics {
    registry: Registry,
    commands: IntCounterVec,
    source_bytes: IntCounterVec,
    dlq: IntCounterVec,
    inflight: IntGauge,
    ready: IntGaugeVec,
    duration: HistogramVec,
}

impl Metrics {
    fn new() -> Result<Self, RuntimeError> {
        let registry = Registry::new();
        let commands = IntCounterVec::new(
            Opts::new("eci_ingestion_commands_total", "Ingestion command outcomes"),
            &["outcome", "reason"],
        )
        .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        let source_bytes = IntCounterVec::new(
            Opts::new(
                "eci_ingestion_source_bytes_total",
                "Verified source bytes processed, including retry attempts",
            ),
            &["outcome"],
        )
        .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        let dlq = IntCounterVec::new(
            Opts::new("eci_ingestion_dlq_total", "Sanitized DLQ publications"),
            &["reason", "outcome"],
        )
        .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        let inflight = IntGauge::new("eci_ingestion_inflight_commands", "Commands in flight")
            .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        let ready = IntGaugeVec::new(
            Opts::new("eci_ingestion_ready", "Dependency readiness"),
            &["dependency"],
        )
        .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "eci_ingestion_command_duration_seconds",
                "Command processing duration",
            ),
            &["outcome"],
        )
        .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        for collector in [
            Box::new(commands.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(source_bytes.clone()),
            Box::new(dlq.clone()),
            Box::new(inflight.clone()),
            Box::new(ready.clone()),
            Box::new(duration.clone()),
        ] {
            registry
                .register(collector)
                .map_err(|_| RuntimeError::Startup("metrics registration failed"))?;
        }
        Ok(Self {
            registry,
            commands,
            source_bytes,
            dlq,
            inflight,
            ready,
            duration,
        })
    }
}

pub async fn run() -> Result<(), RuntimeError> {
    let (config, minio) = Config::from_env()?;
    let metrics = Metrics::new()?;
    let health = HealthState::new(metrics.clone());
    let consumer: Arc<StreamConsumer> = Arc::new(
        kafka_client_config(&config)
            .create()
            .map_err(|_| RuntimeError::Startup("Kafka consumer creation failed"))?,
    );
    consumer
        .subscribe(&[INPUT_TOPIC])
        .map_err(|_| RuntimeError::Startup("Kafka subscription failed"))?;
    let producer: FutureProducer = kafka_client_config(&config)
        .create()
        .map_err(|_| RuntimeError::Startup("Kafka producer creation failed"))?;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let signal_tx = shutdown_tx.clone();
    let signal = tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal_tx.send(true);
    });
    let listener = tokio::net::TcpListener::bind(config.metrics_address)
        .await
        .map_err(|_| RuntimeError::Startup("metrics listener bind failed"))?;
    let server_health = health.clone();
    let mut server_shutdown = shutdown_rx.clone();
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route("/live", get(live))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics_endpoint))
            .with_state(server_health);
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = server_shutdown.wait_for(|shutdown| *shutdown).await;
            })
            .await;
    });
    let probe = tokio::spawn(probe_dependencies(
        config.clone(),
        minio.clone(),
        consumer.clone(),
        health.clone(),
        shutdown_rx.clone(),
    ));

    let mut backoff = Duration::from_millis(250);
    let mut buffered = VecDeque::<OwnedMessage>::new();
    'consume: loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let message = if let Some(message) = buffered.pop_front() {
            message
        } else {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break 'consume;
                    }
                    continue;
                }
                received = consumer.recv() => match received {
                    Ok(message) => message.detach(),
                    Err(_) => {
                        health.set_dependency("kafka", false);
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {},
                            _ = shutdown_rx.changed() => break 'consume,
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                },
            }
        };
        if consumer
            .assignment()
            .and_then(|assignment| consumer.pause(&assignment))
            .is_err()
        {
            health.set_dependency("kafka", false);
        }
        health.metrics.inflight.set(1);
        loop {
            let started = Instant::now();
            let span = tracing::info_span!(
                "ingestion.command.consume",
                messaging.system = "kafka",
                messaging.destination.name = INPUT_TOPIC,
                messaging.kafka.partition = message.partition(),
            );
            if let Some(link) = trace_link(&message) {
                span.add_link(link);
            }
            let action = process_message(&config, &minio, &message, &health.metrics)
                .instrument(span)
                .await;
            let completed = match action {
                ProcessAction::Commit { outcome, reason } => {
                    if commit_message_offset(&consumer, &message).is_ok() {
                        health
                            .metrics
                            .commands
                            .with_label_values(&[outcome, reason])
                            .inc();
                        health
                            .metrics
                            .duration
                            .with_label_values(&[outcome])
                            .observe(started.elapsed().as_secs_f64());
                        backoff = Duration::from_millis(250);
                        true
                    } else {
                        health.set_dependency("kafka", false);
                        false
                    }
                }
                ProcessAction::Dlq { reason } => {
                    if publish_dlq(&producer, &message, reason).await
                        && commit_message_offset(&consumer, &message).is_ok()
                    {
                        health
                            .metrics
                            .dlq
                            .with_label_values(&[reason, "published"])
                            .inc();
                        health
                            .metrics
                            .commands
                            .with_label_values(&["failed", reason])
                            .inc();
                        health
                            .metrics
                            .duration
                            .with_label_values(&["failed"])
                            .observe(started.elapsed().as_secs_f64());
                        backoff = Duration::from_millis(250);
                        true
                    } else {
                        health
                            .metrics
                            .dlq
                            .with_label_values(&[reason, "failed"])
                            .inc();
                        false
                    }
                }
                ProcessAction::Retry { dependency } => {
                    health.set_dependency(dependency, false);
                    false
                }
            };
            if completed {
                if buffered.is_empty()
                    && consumer
                        .assignment()
                        .and_then(|assignment| consumer.resume(&assignment))
                        .is_err()
                {
                    health.set_dependency("kafka", false);
                }
                break;
            }
            health
                .metrics
                .duration
                .with_label_values(&["retry"])
                .observe(started.elapsed().as_secs_f64());
            if matches!(
                poll_during_backoff(&consumer, backoff, &mut shutdown_rx, &mut buffered).await,
                BackoffOutcome::Shutdown
            ) {
                health.metrics.inflight.set(0);
                break 'consume;
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
        health.metrics.inflight.set(0);
    }

    health.live.store(false, Ordering::Relaxed);
    let _ = shutdown_tx.send(true);
    signal.abort();
    probe.abort();
    let _ = server.await;
    Ok(())
}

fn kafka_client_config(config: &Config) -> ClientConfig {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", &config.kafka_ca)
        .set("ssl.certificate.location", &config.kafka_cert)
        .set("ssl.key.location", &config.kafka_key)
        .set("group.id", CONSUMER_GROUP)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("max.poll.interval.ms", "300000")
        .set("queued.min.messages", "1")
        .set("queued.max.messages.kbytes", "256")
        .set("message.timeout.ms", "5000");
    client
}

fn commit_message_offset<M: Message>(
    consumer: &StreamConsumer,
    message: &M,
) -> rdkafka::error::KafkaResult<()> {
    let mut offsets = TopicPartitionList::new();
    let next_offset =
        message
            .offset()
            .checked_add(1)
            .ok_or(rdkafka::error::KafkaError::OffsetFetch(
                rdkafka::types::RDKafkaErrorCode::InvalidArgument,
            ))?;
    offsets.add_partition_offset(
        message.topic(),
        message.partition(),
        Offset::Offset(next_offset),
    )?;
    consumer.commit(&offsets, CommitMode::Sync)
}

enum BackoffOutcome {
    Elapsed,
    Shutdown,
}

async fn poll_during_backoff(
    consumer: &StreamConsumer,
    duration: Duration,
    shutdown: &mut watch::Receiver<bool>,
    buffered: &mut VecDeque<OwnedMessage>,
) -> BackoffOutcome {
    let delay = tokio::time::sleep(duration);
    tokio::pin!(delay);
    loop {
        tokio::select! {
            _ = &mut delay => return BackoffOutcome::Elapsed,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return BackoffOutcome::Shutdown;
                }
            }
            message = consumer.recv() => {
                if let Ok(message) = message {
                    buffered.push_back(message.detach());
                }
            }
        }
    }
}

fn postgres_config_with_deadlines(dsn: &str) -> Result<postgres::Config, postgres::Error> {
    let mut config: postgres::Config = dsn.parse()?;
    config
        .connect_timeout(Duration::from_secs(5))
        .tcp_user_timeout(Duration::from_secs(10))
        .keepalives(true)
        .keepalives_idle(Duration::from_secs(5))
        .keepalives_interval(Duration::from_secs(2))
        .keepalives_retries(3)
        .options(
            "-c statement_timeout=10000 -c lock_timeout=5000 \
             -c idle_in_transaction_session_timeout=10000",
        );
    Ok(config)
}

fn connect_postgres_with_deadlines(dsn: &str) -> Result<postgres::Client, postgres::Error> {
    postgres_config_with_deadlines(dsn)?.connect(postgres::NoTls)
}

enum ProcessAction {
    Commit {
        outcome: &'static str,
        reason: &'static str,
    },
    Dlq {
        reason: &'static str,
    },
    Retry {
        dependency: &'static str,
    },
}

async fn process_message<M: Message>(
    config: &Config,
    minio: &MinioClient,
    message: &M,
    metrics: &Metrics,
) -> ProcessAction {
    let payload = match message.payload() {
        Some(payload) => payload,
        None => {
            return ProcessAction::Dlq {
                reason: "invalid_payload",
            }
        }
    };
    let mut headers = Vec::new();
    if let Some(message_headers) = message.headers() {
        for header in message_headers.iter() {
            headers.push((
                header.key.to_owned(),
                header.value.unwrap_or_default().to_vec(),
            ));
        }
    }
    let (scope, command) =
        match parse_authenticated_command(payload, &headers, config.max_source_bytes) {
            Ok(value) => value,
            Err(error) => {
                let reason = match error.kind() {
                    crate::worker::CommandErrorKind::InvalidScope => "invalid_scope",
                    crate::worker::CommandErrorKind::InvalidPath => "invalid_path",
                    crate::worker::CommandErrorKind::SourceTooLarge => "source_too_large",
                    crate::worker::CommandErrorKind::InvalidPayload => "invalid_payload",
                };
                return ProcessAction::Dlq { reason };
            }
        };
    if !command_message_key_matches(&scope, &command, message.key()) {
        return ProcessAction::Dlq {
            reason: "invalid_message_key",
        };
    }
    let source_span = tracing::info_span!("ingestion.source.fetch", storage.system = "s3");
    let source = match fetch_source(config, minio, &scope, &command)
        .instrument(source_span)
        .await
    {
        Ok(source) => source,
        Err(SourceError::Permanent(reason)) => return ProcessAction::Dlq { reason },
        Err(SourceError::Transient) => {
            return ProcessAction::Retry {
                dependency: "minio",
            }
        }
    };
    metrics
        .source_bytes
        .with_label_values(&["verified"])
        .inc_by(source.len() as u64);
    let source_text = match std::str::from_utf8(&source) {
        Ok(source) => source,
        Err(_) => {
            return ProcessAction::Dlq {
                reason: "source_not_utf8",
            }
        }
    };
    let parsed = std::panic::catch_unwind(AssertUnwindSafe(|| {
        parse_file_full_private(command.path(), source_text)
    }));
    let (nodes, relations, chunks) = match parsed {
        Ok(value) => value,
        Err(_) => {
            return ProcessAction::Dlq {
                reason: "parse_failed",
            }
        }
    };
    let dsn = config.postgres_dsn.clone();
    let persisted = tokio::task::block_in_place(|| {
        let mut client = connect_postgres_with_deadlines(&dsn)?;
        persist_ingestion_command(&mut client, &scope, &command, nodes, relations, &chunks)
    });
    match persisted {
        Ok(crate::CommandOutcome::Applied(_)) => ProcessAction::Commit {
            outcome: "applied",
            reason: "none",
        },
        Ok(crate::CommandOutcome::Duplicate) => ProcessAction::Commit {
            outcome: "duplicate",
            reason: "none",
        },
        Err(error) if error.is_command_id_conflict() => ProcessAction::Dlq {
            reason: "command_id_conflict",
        },
        Err(_) => ProcessAction::Retry {
            dependency: "postgres",
        },
    }
}

struct TraceparentExtractor<'a> {
    traceparent: &'a str,
}

impl Extractor for TraceparentExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        (key == "traceparent").then_some(self.traceparent)
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

fn trace_link<M: Message>(message: &M) -> Option<opentelemetry::trace::SpanContext> {
    let headers = message.headers()?;
    let mut value = None;
    for header in headers.iter() {
        if header.key == "traceparent" {
            if value.is_some() {
                return None;
            }
            value = Some(std::str::from_utf8(header.value?).ok()?);
        }
    }
    let extractor = TraceparentExtractor {
        traceparent: value?,
    };
    let context = TraceContextPropagator::new().extract(&extractor);
    let span_context = context.span().span_context().clone();
    span_context.is_valid().then_some(span_context)
}

enum SourceError {
    Permanent(&'static str),
    Transient,
}

async fn fetch_source(
    config: &Config,
    minio: &MinioClient,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
) -> Result<Vec<u8>, SourceError> {
    let key = source_object_key(scope, command);
    let request = minio
        .get_object(&config.minio_bucket, key.as_str())
        .map_err(|_| SourceError::Permanent("invalid_object_key"))?
        .build();
    let bytes = tokio::time::timeout(Duration::from_secs(10), async {
        let response = match request.send().await {
            Err(error) if object_not_found(&error) => {
                return Err(SourceError::Permanent("source_not_found"))
            }
            Err(_) => return Err(SourceError::Transient),
            Ok(response) => response,
        };
        let object_size = response.object_size().map_err(|_| SourceError::Transient)?;
        if object_size != command.source_size_bytes() || object_size > config.max_source_bytes {
            return Err(SourceError::Permanent("source_size_mismatch"));
        }
        response
            .into_bytes()
            .await
            .map_err(|_| SourceError::Transient)
    })
    .await
    .map_err(|_| SourceError::Transient)??;
    if bytes.len() as u64 != command.source_size_bytes() {
        return Err(SourceError::Permanent("source_size_mismatch"));
    }
    let digest = Sha256::digest(&bytes);
    let actual: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    if actual != command.source_sha256() {
        return Err(SourceError::Permanent("source_digest_mismatch"));
    }
    Ok(bytes.to_vec())
}

fn object_not_found(error: &MinioError) -> bool {
    matches!(
        error,
        MinioError::S3Server(S3ServerError::S3Error(response))
            if matches!(response.code(), MinioErrorCode::NoSuchKey | MinioErrorCode::ResourceNotFound)
    )
}

async fn publish_dlq<M: Message>(
    producer: &FutureProducer,
    message: &M,
    reason: &'static str,
) -> bool {
    let payload = sanitized_dlq_payload(message.partition(), message.offset(), reason);
    let key = format!("{}:{}", message.partition(), message.offset());
    producer
        .send(
            FutureRecord::to(DLQ_TOPIC).key(&key).payload(&payload),
            Duration::from_secs(5),
        )
        .await
        .is_ok()
}

fn sanitized_dlq_payload(partition: i32, offset: i64, reason: &'static str) -> String {
    serde_json::json!({
        "schema_version": "1",
        "reason": reason,
        "source_topic": INPUT_TOPIC,
        "source_partition": partition,
        "source_offset": offset,
    })
    .to_string()
}

async fn probe_dependencies(
    config: Config,
    minio: MinioClient,
    consumer: Arc<StreamConsumer>,
    health: HealthState,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let kafka_ready = consumer
            .fetch_metadata(Some(INPUT_TOPIC), Duration::from_secs(2))
            .is_ok()
            && consumer
                .assignment()
                .map(|a| a.count() > 0)
                .unwrap_or(false);
        health.set_dependency("kafka", kafka_ready);
        let dsn = config.postgres_dsn.clone();
        let postgres_ready = tokio::task::block_in_place(|| {
            connect_postgres_with_deadlines(&dsn)
                .and_then(|mut client| client.simple_query("SELECT 1"))
                .is_ok()
        });
        health.set_dependency("postgres", postgres_ready);
        let minio_ready = tokio::time::timeout(Duration::from_secs(3), async {
            minio
                .bucket_exists(&config.minio_bucket)?
                .build()
                .send()
                .await
                .map(|response| response.exists())
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false);
        health.set_dependency("minio", minio_ready);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            _ = shutdown.changed() => break,
        }
    }
}

async fn live(State(state): State<HealthState>) -> StatusCode {
    if state.live.load(Ordering::Relaxed) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn ready(State(state): State<HealthState>) -> StatusCode {
    if state.ready() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_endpoint(State(state): State<HealthState>) -> Response {
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    if encoder
        .encode(&state.metrics.registry.gather(), &mut body)
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, encoder.format_type())],
        body,
    )
        .into_response()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[test]
    fn readiness_is_fail_closed_per_real_dependency() {
        let state = HealthState::new(Metrics::new().unwrap());
        assert!(!state.ready());
        state.set_dependency("kafka", true);
        state.set_dependency("postgres", true);
        assert!(!state.ready());
        state.set_dependency("minio", true);
        assert!(state.ready());
        state.set_dependency("postgres", false);
        assert!(!state.ready());
    }

    #[tokio::test]
    async fn health_and_metrics_handlers_are_real_and_non_disclosing() {
        let state = HealthState::new(Metrics::new().unwrap());
        assert_eq!(live(State(state.clone())).await, StatusCode::NO_CONTENT);
        assert_eq!(
            ready(State(state.clone())).await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        state.set_dependency("kafka", true);
        state.set_dependency("postgres", true);
        state.set_dependency("minio", true);
        assert_eq!(ready(State(state.clone())).await, StatusCode::NO_CONTENT);

        let response = metrics_endpoint(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            TextEncoder::new().format_type()
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("eci_ingestion_ready"));
        for forbidden in ["tenant", "repository", "acl_group", "command_id"] {
            assert!(!body.contains(forbidden));
        }
    }

    #[test]
    fn kafka_client_is_literal_manual_offset_and_tls_only() {
        let config = Config {
            postgres_dsn: "postgresql://ignored".into(),
            kafka_brokers: "broker:9093".into(),
            kafka_ca: "/tls/ca.crt".into(),
            kafka_cert: "/tls/user.crt".into(),
            kafka_key: "/tls/user.key".into(),
            minio_bucket: "eci-sources".into(),
            max_source_bytes: 1024,
            metrics_address: "127.0.0.1:9100".parse().unwrap(),
        };
        let client = kafka_client_config(&config);
        assert_eq!(client.get("security.protocol"), Some("ssl"));
        assert_eq!(client.get("group.id"), Some(CONSUMER_GROUP));
        assert_eq!(client.get("enable.auto.commit"), Some("false"));
        assert_eq!(client.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(client.get("ssl.ca.location"), Some("/tls/ca.crt"));
        assert_eq!(
            client.get("ssl.certificate.location"),
            Some("/tls/user.crt")
        );
        assert_eq!(client.get("ssl.key.location"), Some("/tls/user.key"));
    }

    #[test]
    fn postgres_client_deadlines_override_dsn_omissions() {
        let config =
            postgres_config_with_deadlines("postgresql://eci:secret@postgres.data-plane.svc/eci")
                .unwrap();
        assert_eq!(config.get_connect_timeout(), Some(&Duration::from_secs(5)));
        assert_eq!(
            config.get_tcp_user_timeout(),
            Some(&Duration::from_secs(10))
        );
        assert_eq!(config.get_keepalives_idle(), Duration::from_secs(5));
        assert_eq!(
            config.get_keepalives_interval(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(config.get_keepalives_retries(), Some(3));
        let options = config.get_options().unwrap();
        assert!(options.contains("statement_timeout=10000"));
        assert!(options.contains("lock_timeout=5000"));
        assert!(options.contains("idle_in_transaction_session_timeout=10000"));
    }

    #[test]
    fn dlq_envelope_contains_only_bounded_coordinates_and_reason() {
        let payload = sanitized_dlq_payload(3, 42, "invalid_scope");
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(
            value
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "reason",
                "schema_version",
                "source_offset",
                "source_partition",
                "source_topic",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        );
        assert!(!payload.contains("tenant"));
        assert!(!payload.contains("repository"));
        assert!(!payload.contains("path"));
        assert!(!payload.contains("source_sha"));
    }
}
