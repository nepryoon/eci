//! Long-running Kafka/MinIO/PostgreSQL runtime for SPEC-067.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use native_tls::{Certificate, Protocol, TlsConnector};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use postgres_native_tls::MakeTlsConnector;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::{BorrowedMessage, Headers, Message, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::worker::{command_message_key_matches, parse_authenticated_command, source_object_key};
use crate::{
    inspect_ingestion_command_receipt, parse_file_full_private, persist_ingestion_command,
    CommandReceiptStatus,
};

const INPUT_TOPIC: &str = "eci.ingestion.file.v1";
const DLQ_TOPIC: &str = "eci.ingestion.file.v1.DLQ";
const CONSUMER_GROUP: &str = "ingestion";
const MAX_RETRY_BUFFERED_MESSAGES: usize = 64;
const KAFKA_OFFSET_COMMIT_DEADLINE: Duration = Duration::from_secs(6);
const KAFKA_METADATA_PROBE_DEADLINE: Duration = Duration::from_secs(3);
const POSTGRES_PROBE_DEADLINE: Duration = Duration::from_secs(16);
const SHUTDOWN_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct Config {
    postgres_dsn: String,
    postgres_ca_pem: Vec<u8>,
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
        let endpoint = minio_https_endpoint(&required("MINIO_ENDPOINT")?)?;
        let postgres_dsn = required("POSTGRES_DSN")?;
        postgres_config_with_deadlines(&postgres_dsn)
            .map_err(|_| RuntimeError::Config("invalid PostgreSQL configuration"))?;
        let postgres_ca_pem = std::fs::read(required("POSTGRES_CA_FILE")?)
            .map_err(|_| RuntimeError::Config("PostgreSQL CA is unreadable"))?;
        postgres_tls_connector(&postgres_ca_pem)
            .map_err(|_| RuntimeError::Config("PostgreSQL CA is invalid"))?;
        let access_key = required("MINIO_ACCESS_KEY")?;
        let secret_key = required("MINIO_SECRET_KEY")?;
        let minio_ca_file = required("MINIO_CA_FILE")?;
        let credentials = StaticProvider::new(&access_key, &secret_key, None);
        let minio = MinioClient::new(
            endpoint,
            Some(credentials),
            Some(std::path::Path::new(&minio_ca_file)),
            Some(false),
        )
        .map_err(|_| RuntimeError::Config("invalid MinIO client configuration"))?;
        Ok((
            Self {
                postgres_dsn,
                postgres_ca_pem,
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

#[derive(Clone, Default)]
struct IngestionConsumerContext {
    assignment_epoch: Arc<AtomicU64>,
}

impl IngestionConsumerContext {
    fn epoch(&self) -> u64 {
        self.assignment_epoch.load(Ordering::Acquire)
    }

    fn invalidate_assignment(&self) {
        self.assignment_epoch.fetch_add(1, Ordering::AcqRel);
    }
}

impl ClientContext for IngestionConsumerContext {}

impl ConsumerContext for IngestionConsumerContext {
    fn pre_rebalance<'a>(&self, _base_consumer: &BaseConsumer<Self>, _rebalance: &Rebalance<'a>) {
        self.invalidate_assignment();
    }
}

type IngestionConsumer = StreamConsumer<IngestionConsumerContext>;

struct BufferedMessage {
    message: OwnedMessage,
    assignment_epoch: u64,
}

pub async fn run() -> Result<(), RuntimeError> {
    let (config, minio) = Config::from_env()?;
    let metrics = Metrics::new()?;
    let health = HealthState::new(metrics.clone());
    let consumer_context = IngestionConsumerContext::default();
    let consumer: Arc<IngestionConsumer> = Arc::new(
        kafka_client_config(&config)
            .create_with_context(consumer_context)
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
    let mut buffered = VecDeque::<BufferedMessage>::new();
    'consume: loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let buffered_message = if let Some(message) = buffered.pop_front() {
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
                    Ok(message) => BufferedMessage {
                        message: message.detach(),
                        assignment_epoch: consumer.context().epoch(),
                    },
                    Err(_) => {
                        health.set_dependency("kafka", false);
                        tokio::select! {
                            _ = tokio::time::sleep(jittered_backoff(backoff)) => {},
                            _ = shutdown_rx.changed() => break 'consume,
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                },
            }
        };
        let message = buffered_message.message;
        let message_epoch = buffered_message.assignment_epoch;
        if consumer
            .assignment()
            .and_then(|assignment| consumer.pause(&assignment))
            .is_err()
        {
            health.set_dependency("kafka", false);
        }
        health.metrics.inflight.set(1);
        loop {
            if !record_is_owned(&consumer, &message, message_epoch) {
                retain_owned_records(&consumer, &mut buffered);
                resume_if_buffer_empty(&consumer, &buffered, &health);
                break;
            }
            let started = Instant::now();
            let span = tracing::info_span!(
                "ingestion.command.consume",
                messaging.system = "kafka",
                messaging.destination.name = INPUT_TOPIC,
                messaging.kafka.partition = message.partition(),
                ingestion.outcome = tracing::field::Empty,
            );
            if let Some(link) = trace_link(&message) {
                span.add_link(link);
            }
            let processing_config = config.clone();
            let processing_minio = minio.clone();
            let processing_message = message.clone();
            let processing_metrics = health.metrics.clone();
            let mut processing = tokio::spawn(
                async move {
                    process_message(
                        &processing_config,
                        &processing_minio,
                        &processing_message,
                        &processing_metrics,
                    )
                    .await
                }
                .instrument(span.clone()),
            );
            let action = loop {
                tokio::select! {
                    result = &mut processing => match result {
                        Ok(action) => break action,
                        Err(_) => {
                            span.record("ingestion.outcome", "retry");
                            break 'consume;
                        }
                    },
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            let _ = await_task_with_deadline(
                                &mut processing,
                                SHUTDOWN_DRAIN_DEADLINE,
                            ).await;
                            // Once shutdown is latched, never start a new DLQ
                            // publication or offset commit. A transaction that
                            // completed during the drain is safe to replay via
                            // its receipt on the next owner.
                            span.record("ingestion.outcome", "retry");
                            break 'consume;
                        }
                    }
                    received = consumer.recv() => match received {
                        Ok(received) => {
                            if !buffer_or_rewind_polled_message(
                                &consumer,
                                &received,
                                &mut buffered,
                                &health,
                            ) {
                                processing.abort();
                                span.record("ingestion.outcome", "retry");
                                break 'consume;
                            }
                        }
                        Err(_) => health.set_dependency("kafka", false),
                    }
                }
            };
            if !record_is_owned(&consumer, &message, message_epoch) {
                span.record("ingestion.outcome", "retry");
                retain_owned_records(&consumer, &mut buffered);
                resume_if_buffer_empty(&consumer, &buffered, &health);
                break;
            }
            let completed = match action {
                ProcessAction::Commit { outcome, reason } => {
                    if shutdown_requested(&shutdown_rx) {
                        span.record("ingestion.outcome", "retry");
                        break 'consume;
                    }
                    if commit_message_offset(consumer.clone(), &message).await {
                        span.record("ingestion.outcome", outcome);
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
                        span.record("ingestion.outcome", "retry");
                        health.set_dependency("kafka", false);
                        false
                    }
                }
                ProcessAction::Dlq { reason } => {
                    if shutdown_requested(&shutdown_rx) {
                        span.record("ingestion.outcome", "retry");
                        break 'consume;
                    }
                    if publish_dlq(&producer, &message, reason).await {
                        health
                            .metrics
                            .dlq
                            .with_label_values(&[reason, "published"])
                            .inc();
                        if shutdown_requested(&shutdown_rx) {
                            span.record("ingestion.outcome", "retry");
                            break 'consume;
                        }
                        if !record_is_owned(&consumer, &message, message_epoch) {
                            span.record("ingestion.outcome", "retry");
                            retain_owned_records(&consumer, &mut buffered);
                            resume_if_buffer_empty(&consumer, &buffered, &health);
                            break;
                        }
                        if commit_message_offset(consumer.clone(), &message).await {
                            span.record("ingestion.outcome", "failed");
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
                            span.record("ingestion.outcome", "retry");
                            health.set_dependency("kafka", false);
                            false
                        }
                    } else {
                        span.record("ingestion.outcome", "retry");
                        health
                            .metrics
                            .dlq
                            .with_label_values(&[reason, "failed"])
                            .inc();
                        false
                    }
                }
                ProcessAction::Retry { dependency } => {
                    span.record("ingestion.outcome", "retry");
                    health.set_dependency(dependency, false);
                    false
                }
            };
            if completed {
                resume_if_buffer_empty(&consumer, &buffered, &health);
                break;
            }
            health
                .metrics
                .duration
                .with_label_values(&["retry"])
                .observe(started.elapsed().as_secs_f64());
            if matches!(
                poll_during_backoff(
                    &consumer,
                    jittered_backoff(backoff),
                    &mut shutdown_rx,
                    &mut buffered,
                    &health,
                )
                .await,
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
        .set("socket.timeout.ms", "5000")
        .set("queued.min.messages", "1")
        .set("queued.max.messages.kbytes", "256")
        .set("message.timeout.ms", "5000");
    client
}

async fn commit_message_offset<M: Message>(consumer: Arc<IngestionConsumer>, message: &M) -> bool {
    let mut offsets = TopicPartitionList::new();
    let Some(next_offset) = message.offset().checked_add(1) else {
        return false;
    };
    if offsets
        .add_partition_offset(
            message.topic(),
            message.partition(),
            Offset::Offset(next_offset),
        )
        .is_err()
    {
        return false;
    }
    matches!(
        run_blocking_with_deadline(KAFKA_OFFSET_COMMIT_DEADLINE, move || {
            consumer.commit(&offsets, CommitMode::Sync)
        })
        .await,
        Some(Ok(()))
    )
}

/// Execute a synchronous client operation without occupying a Tokio worker.
/// The outer deadline is deliberately just above librdkafka's configured
/// socket timeout so probes and shutdown remain bounded even if an offset
/// acknowledgement stalls.
async fn run_blocking_with_deadline<F, T>(deadline: Duration, operation: F) -> Option<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::time::timeout(deadline, tokio::task::spawn_blocking(operation))
        .await
        .ok()
        .and_then(Result::ok)
}

async fn await_task_with_deadline<T>(
    task: &mut tokio::task::JoinHandle<T>,
    deadline: Duration,
) -> Option<T> {
    match tokio::time::timeout(deadline, &mut *task).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(_)) => None,
        Err(_) => {
            task.abort();
            None
        }
    }
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn record_is_owned<M: Message>(
    consumer: &IngestionConsumer,
    message: &M,
    assignment_epoch: u64,
) -> bool {
    assignment_epoch == consumer.context().epoch()
        && consumer
            .assignment()
            .map(|assignment| {
                assignment.elements().iter().any(|element| {
                    element.topic() == message.topic() && element.partition() == message.partition()
                })
            })
            .unwrap_or(false)
}

fn retain_owned_records(consumer: &IngestionConsumer, buffered: &mut VecDeque<BufferedMessage>) {
    buffered.retain(|record| record_is_owned(consumer, &record.message, record.assignment_epoch));
}

fn retry_buffer_has_capacity(buffered_records: usize) -> bool {
    buffered_records < MAX_RETRY_BUFFERED_MESSAGES
}

fn buffer_or_rewind_polled_message(
    consumer: &IngestionConsumer,
    message: &BorrowedMessage<'_>,
    buffered: &mut VecDeque<BufferedMessage>,
    health: &HealthState,
) -> bool {
    if consumer
        .assignment()
        .and_then(|assignment| consumer.pause(&assignment))
        .is_err()
    {
        health.set_dependency("kafka", false);
    }
    if retry_buffer_has_capacity(buffered.len()) {
        buffered.push_back(BufferedMessage {
            message: message.detach(),
            assignment_epoch: consumer.context().epoch(),
        });
        true
    } else if consumer
        .seek(
            message.topic(),
            message.partition(),
            Offset::Offset(message.offset()),
            Duration::from_millis(100),
        )
        .is_ok()
    {
        true
    } else {
        health.set_dependency("kafka", false);
        false
    }
}

fn jittered_backoff(cap: Duration) -> Duration {
    let cap_ms = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX);
    let floor_ms = cap_ms / 2;
    Duration::from_millis(fastrand::u64(floor_ms..=cap_ms))
}

fn resume_if_buffer_empty(
    consumer: &IngestionConsumer,
    buffered: &VecDeque<BufferedMessage>,
    health: &HealthState,
) {
    if buffered.is_empty()
        && consumer
            .assignment()
            .and_then(|assignment| consumer.resume(&assignment))
            .is_err()
    {
        health.set_dependency("kafka", false);
    }
}

enum BackoffOutcome {
    Elapsed,
    Shutdown,
}

async fn poll_during_backoff(
    consumer: &IngestionConsumer,
    duration: Duration,
    shutdown: &mut watch::Receiver<bool>,
    buffered: &mut VecDeque<BufferedMessage>,
    health: &HealthState,
) -> BackoffOutcome {
    if *shutdown.borrow() {
        return BackoffOutcome::Shutdown;
    }
    let delay = tokio::time::sleep(duration);
    tokio::pin!(delay);
    loop {
        if !retry_buffer_has_capacity(buffered.len()) {
            tokio::select! {
                _ = &mut delay => return BackoffOutcome::Elapsed,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return BackoffOutcome::Shutdown;
                    }
                }
                message = consumer.recv() => {
                    match message {
                        Ok(message) => {
                            // The assignment is paused before entering backoff, but
                            // librdkafka can still expose records fetched before the
                            // pause. Polling remains mandatory for group liveness.
                            // Rewind the one record we had to drain so the bounded
                            // application buffer never drops or acknowledges it.
                            if !buffer_or_rewind_polled_message(
                                consumer,
                                &message,
                                buffered,
                                health,
                            ) {
                                return BackoffOutcome::Shutdown;
                            }
                        }
                        Err(_) => health.set_dependency("kafka", false),
                    }
                }
            }
            continue;
        }
        tokio::select! {
            _ = &mut delay => return BackoffOutcome::Elapsed,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return BackoffOutcome::Shutdown;
                }
            }
            message = consumer.recv() => {
                if let Ok(message) = message {
                    if !buffer_or_rewind_polled_message(consumer, &message, buffered, health) {
                        return BackoffOutcome::Shutdown;
                    }
                }
            }
        }
    }
}

fn postgres_config_with_deadlines(dsn: &str) -> Result<postgres::Config, postgres::Error> {
    let mut config: postgres::Config = dsn.parse()?;
    config
        .ssl_mode(postgres::config::SslMode::Require)
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

fn minio_https_endpoint(value: &str) -> Result<BaseUrl, RuntimeError> {
    let endpoint: BaseUrl = value
        .parse()
        .map_err(|_| RuntimeError::Config("invalid MinIO endpoint"))?;
    if !endpoint.https {
        return Err(RuntimeError::Config("MinIO HTTPS is required"));
    }
    Ok(endpoint)
}

fn postgres_tls_connector(ca_pem: &[u8]) -> Result<MakeTlsConnector, native_tls::Error> {
    let ca = Certificate::from_pem(ca_pem)?;
    let mut builder = TlsConnector::builder();
    builder
        .add_root_certificate(ca)
        .min_protocol_version(Some(Protocol::Tlsv12));
    Ok(MakeTlsConnector::new(builder.build()?))
}

fn connect_postgres_with_deadlines(dsn: &str, ca_pem: &[u8]) -> Result<postgres::Client, ()> {
    let config = postgres_config_with_deadlines(dsn).map_err(|_| ())?;
    let tls = postgres_tls_connector(ca_pem).map_err(|_| ())?;
    config.connect(tls).map_err(|_| ())
}

enum CommandPersistenceError {
    Conflict,
    Invalid,
    Transient,
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
    let dsn = config.postgres_dsn.clone();
    let ca_pem = config.postgres_ca_pem.clone();
    let receipt = tokio::task::block_in_place(|| {
        let mut client = connect_postgres_with_deadlines(&dsn, &ca_pem)
            .map_err(|_| CommandPersistenceError::Transient)?;
        let status = inspect_ingestion_command_receipt(&mut client, &scope, &command)
            .map_err(|_| CommandPersistenceError::Transient)?;
        Ok((status, client))
    });
    let mut postgres = match receipt {
        Ok((CommandReceiptStatus::Duplicate, _)) => {
            return ProcessAction::Commit {
                outcome: "duplicate",
                reason: "none",
            }
        }
        Ok((CommandReceiptStatus::Conflict, _)) => {
            return ProcessAction::Dlq {
                reason: "command_id_conflict",
            }
        }
        Ok((CommandReceiptStatus::New, client)) => client,
        Err(CommandPersistenceError::Transient) => {
            return ProcessAction::Retry {
                dependency: "postgres",
            }
        }
        Err(CommandPersistenceError::Invalid) => unreachable!("receipt lookup is read-only"),
        Err(CommandPersistenceError::Conflict) => unreachable!("receipt lookup is read-only"),
    };
    let source_span = tracing::info_span!(
        "ingestion.source.fetch",
        storage.system = "s3",
        ingestion.outcome = tracing::field::Empty,
        ingestion.source.bytes = tracing::field::Empty,
    );
    let source = match fetch_source(config, minio, &scope, &command)
        .instrument(source_span.clone())
        .await
    {
        Ok(source) => {
            source_span.record("ingestion.outcome", "verified");
            source_span.record("ingestion.source.bytes", source.len() as u64);
            source
        }
        Err(SourceError::Permanent(reason)) => {
            source_span.record("ingestion.outcome", "failed");
            return ProcessAction::Dlq { reason };
        }
        Err(SourceError::Transient) => {
            source_span.record("ingestion.outcome", "retry");
            return ProcessAction::Retry {
                dependency: "minio",
            };
        }
    };
    metrics
        .source_bytes
        .with_label_values(&["verified"])
        .inc_by(source.len() as u64);
    if let Err(reason) = validate_source_text(&source) {
        return ProcessAction::Dlq { reason };
    }
    let parse_path = command.path().to_owned();
    let parsed = spawn_blocking_with_current_span(move || {
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let source_text = std::str::from_utf8(&source)
                .expect("source UTF-8 was validated before entering blocking parser");
            parse_file_full_private(&parse_path, source_text)
        }))
    })
    .await;
    let (nodes, relations, chunks) = match parsed {
        Ok(Ok(value)) => value,
        Ok(Err(_)) | Err(_) => {
            return ProcessAction::Dlq {
                reason: "parse_failed",
            }
        }
    };
    let persisted: Result<crate::CommandOutcome, CommandPersistenceError> =
        tokio::task::block_in_place(|| {
            persist_ingestion_command(&mut postgres, &scope, &command, nodes, relations, &chunks)
                .map_err(|error| {
                    if error.is_command_id_conflict() {
                        CommandPersistenceError::Conflict
                    } else if error.is_invalid_command_data() {
                        CommandPersistenceError::Invalid
                    } else {
                        CommandPersistenceError::Transient
                    }
                })
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
        Err(CommandPersistenceError::Conflict) => ProcessAction::Dlq {
            reason: "command_id_conflict",
        },
        Err(CommandPersistenceError::Invalid) => ProcessAction::Dlq {
            reason: "parse_failed",
        },
        Err(CommandPersistenceError::Transient) => ProcessAction::Retry {
            dependency: "postgres",
        },
    }
}

fn spawn_blocking_with_current_span<F, R>(operation: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let parent = tracing::Span::current();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    tokio::task::spawn_blocking(move || {
        tracing::dispatcher::with_default(&dispatch, || parent.in_scope(operation))
    })
}

fn validate_source_text(source: &[u8]) -> Result<&str, &'static str> {
    let text = std::str::from_utf8(source).map_err(|_| "source_not_utf8")?;
    if text.contains('\0') {
        return Err("source_contains_nul");
    }
    Ok(text)
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
    consumer: Arc<IngestionConsumer>,
    health: HealthState,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let probe_consumer = consumer.clone();
        let kafka_ready = run_blocking_with_deadline(KAFKA_METADATA_PROBE_DEADLINE, move || {
            probe_consumer
                .fetch_metadata(Some(INPUT_TOPIC), Duration::from_secs(2))
                .is_ok()
                && probe_consumer
                    .assignment()
                    .map(|assignment| assignment.count() > 0)
                    .unwrap_or(false)
        })
        .await
        .unwrap_or(false);
        health.set_dependency("kafka", kafka_ready);
        let dsn = config.postgres_dsn.clone();
        let ca_pem = config.postgres_ca_pem.clone();
        let postgres_ready = run_blocking_with_deadline(POSTGRES_PROBE_DEADLINE, move || {
            match connect_postgres_with_deadlines(&dsn, &ca_pem) {
                Ok(mut client) => postgres_runtime_schema_ready(&mut client),
                Err(()) => false,
            }
        })
        .await
        .unwrap_or(false);
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

/// Readiness means that the runtime role can execute every table operation in
/// the atomic ingestion transaction, not merely that PostgreSQL accepts a TCP
/// connection. `to_regclass` keeps a missing migration a normal false result.
pub fn postgres_runtime_schema_ready(client: &mut postgres::Client) -> bool {
    client
        .query_one(
            "SELECT COALESCE(\
                has_table_privilege(current_user, to_regclass('ingestion_command_receipt'), 'SELECT')\
                AND has_table_privilege(current_user, to_regclass('ingestion_command_receipt'), 'INSERT')\
                AND has_table_privilege(current_user, to_regclass('code_node'), 'INSERT')\
                AND has_table_privilege(current_user, to_regclass('code_node'), 'UPDATE')\
                AND has_table_privilege(current_user, to_regclass('code_relation'), 'SELECT')\
                AND has_table_privilege(current_user, to_regclass('code_relation'), 'INSERT')\
                AND has_table_privilege(current_user, to_regclass('code_relation'), 'DELETE')\
                AND has_table_privilege(current_user, to_regclass('code_chunk'), 'SELECT')\
                AND has_table_privilege(current_user, to_regclass('code_chunk'), 'INSERT')\
                AND has_table_privilege(current_user, to_regclass('code_chunk'), 'DELETE')\
                AND has_table_privilege(current_user, to_regclass('outbox'), 'INSERT'),\
                false\
            )",
            &[],
        )
        .and_then(|row| row.try_get::<_, bool>(0))
        .unwrap_or(false)
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
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};
    use tracing_core::span::Current;

    thread_local! {
        static TEST_SPAN_STACK: RefCell<Vec<(Id, &'static Metadata<'static>)>> =
            const { RefCell::new(Vec::new()) };
    }

    #[derive(Default)]
    struct TestSubscriber {
        next_id: AtomicU64,
        metadata: Mutex<HashMap<u64, &'static Metadata<'static>>>,
        observed_consume_parent: Arc<AtomicBool>,
    }

    impl Subscriber for TestSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            let id = Id::from_u64(self.next_id.fetch_add(1, Ordering::Relaxed) + 1);
            self.metadata
                .lock()
                .expect("test subscriber metadata lock")
                .insert(id.clone().into_u64(), _span.metadata());
            id
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            TEST_SPAN_STACK.with(|stack| {
                if stack.borrow().last().is_some_and(|(_, metadata)| {
                    metadata.name() == "ingestion.command.consume.test"
                }) {
                    self.observed_consume_parent.store(true, Ordering::Relaxed);
                }
            });
        }
        fn enter(&self, span: &Id) {
            let metadata = self.metadata.lock().expect("test subscriber metadata lock")
                [&span.clone().into_u64()];
            TEST_SPAN_STACK.with(|stack| stack.borrow_mut().push((span.clone(), metadata)));
        }

        fn exit(&self, _span: &Id) {
            TEST_SPAN_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }

        fn current_span(&self) -> Current {
            TEST_SPAN_STACK.with(|stack| {
                stack
                    .borrow()
                    .last()
                    .map(|(id, metadata)| Current::new(id.clone(), metadata))
                    .unwrap_or_else(Current::none)
            })
        }
    }

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
            postgres_ca_pem: Vec::new(),
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
        assert_eq!(client.get("socket.timeout.ms"), Some("5000"));
        assert_eq!(client.get("ssl.ca.location"), Some("/tls/ca.crt"));
        assert_eq!(
            client.get("ssl.certificate.location"),
            Some("/tls/user.crt")
        );
        assert_eq!(client.get("ssl.key.location"), Some("/tls/user.key"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_kafka_operations_obey_async_deadline() {
        let result = run_blocking_with_deadline(Duration::from_millis(20), || {
            std::thread::sleep(Duration::from_millis(100));
            42
        })
        .await;

        assert!(result.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_parser_work_inherits_the_consume_span() {
        let subscriber = TestSubscriber::default();
        let observed_consume_parent = subscriber.observed_consume_parent.clone();
        let task = tracing::subscriber::with_default(subscriber, || {
            let consume_span = tracing::info_span!("ingestion.command.consume.test");
            let _entered = consume_span.enter();
            spawn_blocking_with_current_span(|| {
                tracing::info!("blocking parser test event");
            })
        });

        task.await.expect("blocking task completes");
        assert!(observed_consume_parent.load(Ordering::Relaxed));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drain_aborts_tasks_after_its_deadline() {
        let mut task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            42
        });

        let result = await_task_with_deadline(&mut task, Duration::from_millis(20)).await;

        assert!(result.is_none());
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn latched_shutdown_blocks_record_finalization() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        assert!(!shutdown_requested(&shutdown_rx));

        shutdown_tx.send(true).unwrap();

        assert!(shutdown_requested(&shutdown_rx));
    }

    #[test]
    fn postgres_client_deadlines_override_dsn_omissions() {
        let config =
            postgres_config_with_deadlines("postgresql://eci:secret@postgres.data-plane.svc/eci")
                .unwrap();
        assert_eq!(config.get_ssl_mode(), postgres::config::SslMode::Require);
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
    fn datastore_transport_rejects_plaintext_and_invalid_trust() {
        assert!(matches!(
            minio_https_endpoint("http://minio.data-plane.svc.cluster.local:9000"),
            Err(RuntimeError::Config("MinIO HTTPS is required"))
        ));
        assert!(minio_https_endpoint("https://minio.data-plane.svc.cluster.local:9000").is_ok());
        assert!(postgres_tls_connector(b"not a PEM certificate").is_err());
    }

    #[test]
    fn source_text_validation_rejects_postgres_nul_permanently() {
        assert_eq!(
            validate_source_text(b"package orders\nvar value = \"before\0after\"\n"),
            Err("source_contains_nul")
        );
        assert_eq!(
            validate_source_text(b"package orders\nvar value = \"valid\"\n"),
            Ok("package orders\nvar value = \"valid\"\n")
        );
        assert_eq!(validate_source_text(&[0xff, 0xfe]), Err("source_not_utf8"));
    }
    #[test]
    fn rebalance_context_invalidates_detached_record_epoch() {
        let context = IngestionConsumerContext::default();
        let detached_epoch = context.epoch();
        context.invalidate_assignment();
        assert_ne!(detached_epoch, context.epoch());
    }

    #[test]
    fn retry_buffer_has_a_hard_application_limit() {
        assert!(retry_buffer_has_capacity(MAX_RETRY_BUFFERED_MESSAGES - 1));
        assert!(!retry_buffer_has_capacity(MAX_RETRY_BUFFERED_MESSAGES));
        assert!(!retry_buffer_has_capacity(MAX_RETRY_BUFFERED_MESSAGES + 1));
    }

    #[test]
    fn dependency_backoff_uses_bounded_equal_jitter() {
        for cap in [Duration::from_millis(250), Duration::from_secs(30)] {
            for _ in 0..128 {
                let delay = jittered_backoff(cap);
                assert!(delay >= cap / 2);
                assert!(delay <= cap);
            }
        }
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
