use std::{
    collections::HashMap,
    future::ready,
    num::NonZeroUsize,
    panic,
    sync::{atomic::{AtomicUsize, Ordering}, Arc, LazyLock},
    time::{Duration, Instant},
};

use aws_sdk_s3::{Client as S3Client, operation::get_object::GetObjectError};
use aws_sdk_sqs::{
    Client as SqsClient,
    operation::{
        delete_message_batch::{DeleteMessageBatchError, DeleteMessageBatchOutput},
        receive_message::ReceiveMessageError,
        send_message_batch::{SendMessageBatchError, SendMessageBatchOutput},
    },
    types::{DeleteMessageBatchRequestEntry, Message, MessageAttributeValue, MessageSystemAttributeName, SendMessageBatchRequestEntry},
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_types::region::Region;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use smallvec::SmallVec;
use snafu::{ResultExt, Snafu};
use tokio::{pin, select};
use tokio_util::codec::FramedRead;
use tracing::Instrument;
use vector_lib::{
    codecs::decoding::FramingError,
    config::{LegacyKey, LogNamespace, log_schema},
    configurable::configurable_component,
    event::MaybeAsLogMut,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
    },
    lookup::{PathPrefix, metadata_path, path},
    source_sender::SendError,
};

use crate::{
    SourceSender,
    aws::AwsTimeout,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    config::{SourceAcknowledgementsConfig, SourceContext},
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, Event, LogEvent},
    internal_events::{
        EventsReceived, S3ObjectProcessingFailed, S3ObjectProcessingSucceeded,
        SqsMessageDeletePartialError, SqsMessageDeleteSucceeded,
        SqsMessageProcessingError, SqsMessageProcessingSucceeded, SqsMessageReceiveError,
        SqsMessageReceiveSucceeded, SqsMessageSentPartialError,
        SqsMessageSentSucceeded, SqsS3EventRecordInvalidEventIgnored, StreamClosedError,
    },
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    sources::aws_s3::AwsS3Config,
    tls::TlsConfig,
};

static SUPPORTED_S3_EVENT_VERSION: LazyLock<semver::VersionReq> =
    LazyLock::new(|| semver::VersionReq::parse("~2").unwrap());

/// Configuration for deferring events based on their age.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct DeferredConfig {
    /// The URL of the queue to forward events to when they are older than `max_age_secs`.
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    /// Event must have been emitted within the last `max_age_secs` seconds to be processed.
    ///
    /// If the event is older, it is forwarded to the `queue_url` for later processing.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 3600))]
    pub(super) max_age_secs: u64,
}

/// SQS configuration options.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The URL of the SQS queue to poll for bucket notifications.
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    /// How long to wait while polling the queue for new messages, in seconds.
    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u32,

    /// The visibility timeout to use for messages, in seconds.
    #[serde(default = "default_visibility_timeout_secs")]
    #[derivative(Default(value = "default_visibility_timeout_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Visibility Timeout"))]
    pub(super) visibility_timeout_secs: u32,

    /// Whether to delete the message once it is processed.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_message: bool,

    /// Whether to delete non-retryable messages.
    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_failed_message: bool,

    /// Number of concurrent tasks to create for polling the queue for messages.
    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 5))]
    pub(super) client_concurrency: Option<NonZeroUsize>,

    /// Maximum number of messages to poll from SQS in a batch
    #[serde(default = "default_max_number_of_messages")]
    #[derivative(Default(value = "default_max_number_of_messages()"))]
    #[configurable(metadata(docs::human_name = "Max Messages"))]
    #[configurable(metadata(docs::examples = 1))]
    pub(super) max_number_of_messages: u32,

    #[configurable(derived)]
    #[serde(default)]
    #[derivative(Default)]
    pub(super) tls_options: Option<TlsConfig>,

    #[configurable(derived)]
    #[derivative(Default)]
    #[serde(default)]
    #[serde(flatten)]
    pub(super) timeout: Option<AwsTimeout>,

    /// Configuration for deferring events to another queue based on their age.
    #[configurable(derived)]
    pub(super) deferred: Option<DeferredConfig>,

    /// Maximum number of files to process concurrently from a single SQS message.
    #[serde(default = "default_file_concurrency")]
    #[derivative(Default(value = "default_file_concurrency()"))]
    #[configurable(metadata(docs::type_unit = "files"))]
    #[configurable(metadata(docs::examples = 50))]
    pub(super) file_concurrency: usize,
}

const fn default_poll_secs() -> u32 {
    15
}

const fn default_file_concurrency() -> usize {
    10
}

const fn default_visibility_timeout_secs() -> u32 {
    300
}

const fn default_max_number_of_messages() -> u32 {
    10
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Snafu)]
pub(super) enum IngestorNewError {
    #[snafu(display("Invalid value for max_number_of_messages {}", messages))]
    InvalidNumberOfMessages { messages: u32 },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    #[snafu(display(
        "Could not parse SQS message with id {} as S3 notification: {}",
        message_id,
        source
    ))]
    InvalidSqsMessage {
        source: serde_json::Error,
        message_id: String,
    },
    #[snafu(display("Failed to fetch s3://{}/{}: {}", bucket, key, source))]
    GetObject {
        source: SdkError<GetObjectError, HttpResponse>,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to read all of s3://{}/{}: {}", bucket, key, source))]
    ReadObject {
        source: Box<dyn FramingError>,
        bucket: String,
        key: String,
    },
    #[snafu(display("Failed to flush all of s3://{}/{}: {}", bucket, key, source))]
    PipelineSend {
        source: vector_lib::source_sender::SendError,
        bucket: String,
        key: String,
    },
    #[snafu(display(
        "Object notification for s3://{}/{} is a bucket in another region: {}",
        bucket,
        key,
        region
    ))]
    WrongRegion {
        region: String,
        bucket: String,
        key: String,
    },
    #[snafu(display("Unsupported S3 event version: {}.", version,))]
    UnsupportedS3EventVersion { version: semver::Version },
    #[snafu(display(
        "Sink reported an error sending events for an s3 object in region {}: s3://{}/{}",
        region,
        bucket,
        key
    ))]
    ErrorAcknowledgement {
        region: String,
        bucket: String,
        key: String,
    },
    #[snafu(display(
        "File s3://{}/{} too old.  Forwarded to deferred queue {}",
        bucket,
        key,
        deferred_queue
    ))]
    FileTooOld {
        bucket: String,
        key: String,
        deferred_queue: String,
    },
}

pub(super) struct HandleResult {
    pub deferred_body: Option<String>,
    pub requeue_body: Option<String>,
}

enum RecordStatus<T> {
    Success,
    Ignored,
    Deferred(T),
    Failed(T, ProcessingError),
}

pub struct State {
    region: Region,

    s3_client: S3Client,
    sqs_client: SqsClient,

    multiline: Option<line_agg::Config>,
    compression: super::Compression,

    queue_url: String,
    poll_secs: i32,
    max_number_of_messages: i32,
    client_concurrency: usize,
    file_concurrency: usize,
    visibility_timeout_secs: i32,
    delete_message: bool,
    delete_failed_message: bool,
    decoder: Decoder,

    deferred: Option<DeferredConfig>,
}

pub(super) struct Ingestor {
    state: Arc<State>,
}

impl Ingestor {
    pub(super) async fn new(
        region: Region,
        sqs_client: SqsClient,
        s3_client: S3Client,
        config: Config,
        compression: super::Compression,
        multiline: Option<line_agg::Config>,
        decoder: Decoder,
    ) -> Result<Ingestor, IngestorNewError> {
        if config.max_number_of_messages < 1 || config.max_number_of_messages > 10 {
            return Err(IngestorNewError::InvalidNumberOfMessages {
                messages: config.max_number_of_messages,
            });
        }
        let state = Arc::new(State {
            region,

            s3_client,
            sqs_client,

            compression,
            multiline,

            queue_url: config.queue_url,
            poll_secs: config.poll_secs as i32,
            max_number_of_messages: config.max_number_of_messages as i32,
            client_concurrency: config
                .client_concurrency
                .map(|n| n.get())
                .unwrap_or_else(crate::num_threads),
            file_concurrency: config.file_concurrency,
            visibility_timeout_secs: config.visibility_timeout_secs as i32,
            delete_message: config.delete_message,
            delete_failed_message: config.delete_failed_message,
            decoder,

            deferred: config.deferred,
        });

        Ok(Ingestor { state })
    }

    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<(), ()> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let mut handles = Vec::new();
        for _ in 0..self.state.client_concurrency {
            let process = IngestorProcess::new(
                Arc::clone(&self.state),
                cx.out.clone(),
                cx.shutdown.clone(),
                log_namespace,
                acknowledgements,
            );
            let fut = process.run();
            let handle = tokio::spawn(fut.in_current_span());
            handles.push(handle);
        }

        for handle in handles.drain(..) {
            if let Err(e) = handle.await {
                if e.is_panic() {
                    panic::resume_unwind(e.into_panic());
                }
            }
        }

        Ok(())
    }
}

pub struct IngestorProcess {
    state: Arc<State>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    backoff: ExponentialBackoff,
}

impl IngestorProcess {
    pub fn new(
        state: Arc<State>,
        out: SourceSender,
        shutdown: ShutdownSignal,
        log_namespace: LogNamespace,
        acknowledgements: bool,
    ) -> Self {
        Self {
            state,
            out,
            shutdown,
            acknowledgements,
            log_namespace,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
            backoff: ExponentialBackoff::default().max_delay(Duration::from_secs(30)),
        }
    }

    async fn run(mut self) {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);
        
        tracing::info!("SQS ingestor worker started. Ready to poll for new messages.");

        loop {
            let messages = select! {
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received for SQS worker. Stopping polling.");
                    break;
                },
                result = self.receive_messages() => {
                    match result {
                        Ok(messages) => {
                            emit!(SqsMessageReceiveSucceeded {
                                count: messages.len(),
                            });
                            self.backoff.reset();
                            messages
                        }
                        Err(err) => {
                            emit!(SqsMessageReceiveError { error: &err });
                            let delay = self.backoff.next().expect("backoff never ends");
                            trace!(
                                delay_ms = delay.as_millis(),
                                "`receive_messages` failed, will retry after delay.",
                            );
                            select! {
                                _ = &mut shutdown => {
                                    tracing::info!("Shutdown signal received during backoff, exiting.");
                                    break;
                                },
                                _ = tokio::time::sleep(delay) => {}
                            }
                            continue;
                        }
                    }
                }
            };

            if messages.is_empty() {
                continue;
            }

            tracing::info!("Starting to process a batch of {} SQS messages.", messages.len());
            
            self.process_batch(messages).await;
            
            tracing::info!("Successfully completed processing the batch of SQS messages.");
        }
        
        tracing::info!("SQS ingestor worker has fully finalized and shut down. All queue processing for this worker is complete.");
    }

    async fn process_batch(&mut self, messages: Vec<Message>) {
        let total_messages = messages.len();
        
        let mut delete_entries = Vec::new();
        let mut deferred_entries = Vec::new();
        let mut requeue_entries = Vec::new();
        
        // Tracks message_id -> (receipt_handle, required_success_count, current_success_count)
        let mut tracker: HashMap<String, (String, u8, u8)> = HashMap::new();

        for (idx, message) in messages.into_iter().enumerate() {
            if self.shutdown.clone().now_or_never().is_some() {
                tracing::warn!(
                    "SHUTDOWN STATUS: Halting batch processing early. Skipping {} remaining SQS messages to exit promptly.",
                    total_messages - idx
                );
                break;
            }

            let receipt_handle = match message.receipt_handle {
                None => {
                    warn!(message = "Refusing to process message with no receipt_handle.", ?message.message_id);
                    continue;
                }
                Some(ref handle) => handle.to_owned(),
            };

            let message_id = message
                .message_id
                .clone()
                .unwrap_or_else(|| "unknown".to_owned());
                
            let retry_count = message.message_attributes.as_ref()
                .and_then(|attrs| attrs.get("VectorRetryCount"))
                .and_then(|attr| attr.string_value.as_deref())
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);

            // Create a safe alphanumeric ID for SQS batching limits (1-80 chars)
            let safe_batch_id = message_id.chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .take(80)
                .collect::<String>();
                
            let safe_batch_id = if safe_batch_id.is_empty() {
                format!("msg-{}", idx)
            } else {
                safe_batch_id
            };

            match self.handle_sqs_message(message, retry_count).await {
                Ok(result) => {
                    emit!(SqsMessageProcessingSucceeded {
                        message_id: &message_id
                    });
                    
                    let mut required_actions = 0;

                    if let Some(body) = result.deferred_body {
                        if self.state.deferred.is_some() {
                            deferred_entries.push((safe_batch_id.clone(), body));
                            required_actions += 1;
                        }
                    }
                    
                    if let Some(body) = result.requeue_body {
                        if !self.state.delete_failed_message {
                            if retry_count < 5 {
                                requeue_entries.push((safe_batch_id.clone(), body, retry_count + 1));
                                required_actions += 1;
                            } else {
                                tracing::error!(
                                    message_id = %message_id,
                                    "Message exceeded maximum Vector partial requeue attempts. Dropping to prevent infinite loop and to enforce DLQ exhaustion logic."
                                );
                            }
                        }
                    }

                    if self.state.delete_message {
                        if required_actions == 0 {
                            delete_entries.push((safe_batch_id.clone(), receipt_handle.clone()));
                        } else {
                            tracker.insert(safe_batch_id.clone(), (receipt_handle, required_actions, 0));
                        }
                    }
                }
                Err(err) => {
                    emit!(SqsMessageProcessingError {
                        message_id: &message_id,
                        error: &err,
                    });
                }
            }
        }

        if !deferred_entries.is_empty() {
            let deferred_url = self.state.deferred.as_ref().map(|d| d.queue_url.clone());
            if let Some(deferred_url) = deferred_url {
                for chunk in deferred_entries.chunks(10) {
                    let mut entries = Vec::new();
                    for (id, body) in chunk {
                        if let Ok(entry) = SendMessageBatchRequestEntry::builder()
                            .id(id.clone())
                            .message_body(body.clone())
                            .build() {
                            entries.push(entry);
                        } else {
                            tracing::error!("Failed to build SQS message entry for deferred routing: {}", id);
                        }
                    }

                    if !entries.is_empty() {
                        match self.send_messages(entries, deferred_url.clone()).await {
                            Ok(res) => {
                                if !res.successful.is_empty() {
                                    for success in &res.successful {
                                        if let Some(entry) = tracker.get_mut(success.id.as_str()) {
                                            entry.2 += 1;
                                            if entry.1 == entry.2 {
                                                delete_entries.push((success.id.clone(), entry.0.clone()));
                                            }
                                        }
                                    }
                                    emit!(SqsMessageSentSucceeded { message_ids: res.successful });
                                }
                                if !res.failed.is_empty() {
                                    emit!(SqsMessageSentPartialError { entries: res.failed });
                                }
                            }
                            Err(err) => {
                                tracing::error!("Failed to send deferred messages: {:?}", err);
                            }
                        }
                    }
                }
            }
        }

        if !requeue_entries.is_empty() {
            let queue_url = self.state.queue_url.clone();
            for chunk in requeue_entries.chunks(10) {
                let mut entries = Vec::new();
                for (id, body, next_retry) in chunk {
                    let attr_val = MessageAttributeValue::builder()
                        .data_type("Number")
                        .string_value(next_retry.to_string())
                        .build()
                        .unwrap(); 

                    if let Ok(entry) = SendMessageBatchRequestEntry::builder()
                        .id(id.clone())
                        .message_body(body.clone())
                        .delay_seconds(30)
                        .message_attributes("VectorRetryCount", attr_val)
                        .build() {
                        entries.push(entry);
                    } else {
                        tracing::error!("Failed to build SQS message entry for requeue routing: {}", id);
                    }
                }

                if !entries.is_empty() {
                    match self.send_messages(entries, queue_url.clone()).await {
                        Ok(res) => {
                            for success in res.successful {
                                if let Some(entry) = tracker.get_mut(success.id.as_str()) {
                                    entry.2 += 1;
                                    if entry.1 == entry.2 {
                                        delete_entries.push((success.id.clone(), entry.0.clone()));
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!("Failed to requeue failed messages: {:?}", err);
                        }
                    }
                }
            }
        }

        if !delete_entries.is_empty() {
            for chunk in delete_entries.chunks(10) {
                let mut entries = Vec::new();
                for (id, receipt) in chunk {
                    if let Ok(entry) = DeleteMessageBatchRequestEntry::builder()
                        .id(id.clone())
                        .receipt_handle(receipt.clone())
                        .build() {
                        entries.push(entry);
                    } else {
                        tracing::error!("Failed to build SQS message entry for deletion routing: {}", id);
                    }
                }

                if !entries.is_empty() {
                    match self.delete_messages(entries).await {
                        Ok(result) => {
                            if !result.successful.is_empty() {
                                emit!(SqsMessageDeleteSucceeded { message_ids: result.successful });
                            }
                            if !result.failed.is_empty() {
                                emit!(SqsMessageDeletePartialError { entries: result.failed });
                            }
                        }
                        Err(err) => {
                            tracing::error!("Failed to delete messages: {:?}", err);
                        }
                    }
                }
            }
        }
    }

    async fn handle_sqs_message(&mut self, message: Message, _retry_count: u32) -> Result<HandleResult, ProcessingError> {
        let sqs_body = message.body.clone().unwrap_or_default();

        tracing::info!(
            message_id = ?message.message_id,
            body = %sqs_body,
            "Received raw SQS message body"
        );

        let sent_timestamp = message.attributes.as_ref()
            .and_then(|attrs| attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::SentTimestamp))
            .and_then(|ts| ts.parse::<i64>().ok())
            .and_then(|ts| Utc.timestamp_millis_opt(ts).single());

        let sqs_body = serde_json::from_str::<SnsNotification>(sqs_body.as_ref())
            .map(|notification| notification.message)
            .unwrap_or(sqs_body);
            
        let s3_event: SqsEvent =
            serde_json::from_str(sqs_body.as_ref()).context(InvalidSqsMessageSnafu {
                message_id: message
                    .message_id
                    .clone()
                    .unwrap_or_else(|| "<empty>".to_owned()),
            })?;

        match s3_event {
            SqsEvent::TestEvent(_s3_test_event) => {
                debug!(?message.message_id, message = "Found S3 Test Event.");
                Ok(HandleResult { deferred_body: None, requeue_body: None })
            }
            SqsEvent::Event(s3_event) => self.handle_s3_event(s3_event).await,
            SqsEvent::CrowdStrike(fdr_event) => self.handle_crowdstrike_event(fdr_event, sent_timestamp).await,
        }
    }

    async fn handle_s3_event(&mut self, s3_event: S3Event) -> Result<HandleResult, ProcessingError> {
        let total_records = s3_event.records.len();
        let pending_files = Arc::new(AtomicUsize::new(total_records));
        
        let futures = s3_event.records.into_iter().map(|record| {
            let mut process = self.clone();
            let bucket = record.s3.bucket.name.clone();
            let key = record.s3.object.key.clone();
            let region = record.aws_region.clone();
            let rec_clone = record.clone();

            async move {
                let fut = async move {
                    let event_version: semver::Version = rec_clone.event_version.clone().into();
                    if !SUPPORTED_S3_EVENT_VERSION.matches(&event_version) {
                        return RecordStatus::Failed(rec_clone.clone(), ProcessingError::UnsupportedS3EventVersion {
                            version: event_version,
                        });
                    }

                    if rec_clone.event_name.kind != "ObjectCreated" {
                        emit!(SqsS3EventRecordInvalidEventIgnored {
                            bucket: &rec_clone.s3.bucket.name,
                            key: &rec_clone.s3.object.key,
                            kind: &rec_clone.event_name.kind,
                            name: &rec_clone.event_name.name,
                        });
                        return RecordStatus::Ignored;
                    }

                    match process.process_file(
                        rec_clone.s3.bucket.name.clone(),
                        rec_clone.s3.object.key.clone(),
                        Some(rec_clone.aws_region.clone()),
                        Some(rec_clone.event_time),
                        process.log_namespace,
                    ).await {
                        Ok(()) => RecordStatus::Success,
                        Err(ProcessingError::FileTooOld { .. }) => RecordStatus::Deferred(rec_clone),
                        Err(e) => RecordStatus::Failed(rec_clone, e),
                    }
                };
                
                match tokio::spawn(fut.in_current_span()).await {
                    Ok(res) => res,
                    Err(join_err) => {
                        if join_err.is_panic() {
                            panic::resume_unwind(join_err.into_panic());
                        } else {
                            tracing::error!(%bucket, %key, "S3 processing task was abruptly cancelled");
                            RecordStatus::Failed(record, ProcessingError::ErrorAcknowledgement {
                                region,
                                bucket,
                                key,
                            })
                        }
                    }
                }
            }
        });

        let mut stream = futures::stream::iter(futures).buffer_unordered(self.state.file_concurrency);
        let mut deferred_records = Vec::new();
        let mut failed_records = Vec::new();

        let shutdown_sig = self.shutdown.clone().fuse();
        pin!(shutdown_sig);
        let mut is_shutting_down = false;
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            select! {
                _ = &mut shutdown_sig, if !is_shutting_down => {
                    is_shutting_down = true;
                    let remaining = pending_files.load(Ordering::Relaxed);
                    tracing::warn!(
                        "GRACEFUL SHUTDOWN INITIATED: Vector is wrapping up {} active S3 files in this batch. \
                         Please DO NOT force kill the process...", remaining
                    );
                }
                _ = interval.tick(), if is_shutting_down => {
                    let remaining = pending_files.load(Ordering::Relaxed);
                    if remaining > 0 {
                        tracing::warn!("SHUTDOWN STATUS: Still wrapping up {} S3 files...", remaining);
                    }
                }
                result = stream.next() => {
                    match result {
                        Some(res) => {
                            let remaining = pending_files.fetch_sub(1, Ordering::Relaxed) - 1;
                            match res {
                                RecordStatus::Success | RecordStatus::Ignored => {}
                                RecordStatus::Deferred(r) => deferred_records.push(r),
                                RecordStatus::Failed(r, e) => {
                                    tracing::error!("File processing failed: {:?}", e);
                                    failed_records.push(r);
                                }
                            }
                            if is_shutting_down && remaining == 0 {
                                tracing::info!("SHUTDOWN STATUS: All files in current event successfully wrapped up.");
                            }
                        }
                        None => break, 
                    }
                }
            }
        }
        
        let deferred_body = if !deferred_records.is_empty() {
            Some(serde_json::to_string(&S3Event { records: deferred_records }).unwrap())
        } else { None };

        let requeue_body = if !failed_records.is_empty() {
            Some(serde_json::to_string(&S3Event { records: failed_records }).unwrap())
        } else { None };

        tracing::info!("All {} S3 files in the current event completed successfully.", total_records);
        Ok(HandleResult { deferred_body, requeue_body })
    }

    async fn handle_crowdstrike_event(
        &mut self, 
        fdr_event: CrowdStrikeFdrEvent,
        sent_timestamp: Option<DateTime<Utc>>,
    ) -> Result<HandleResult, ProcessingError> {
        let total_records = fdr_event.files.len();
        let pending_files = Arc::new(AtomicUsize::new(total_records));
        
        let futures = fdr_event.files.into_iter().map(|file| {
            let mut process = self.clone();
            let bucket = fdr_event.bucket.clone();
            let key = file.path.clone();
            let file_clone = file.clone();

            async move {
                let bucket_err = bucket.clone();
                let key_err = key.clone();
                let fut = async move {
                    match process.process_file(
                        bucket,
                        key,
                        None,
                        sent_timestamp,
                        process.log_namespace,
                    ).await {
                        Ok(()) => RecordStatus::Success,
                        Err(ProcessingError::FileTooOld { .. }) => RecordStatus::Deferred(file_clone),
                        Err(e) => RecordStatus::Failed(file_clone, e),
                    }
                };
                
                match tokio::spawn(fut.in_current_span()).await {
                    Ok(res) => res,
                    Err(join_err) => {
                        if join_err.is_panic() {
                            panic::resume_unwind(join_err.into_panic());
                        } else {
                            tracing::error!(bucket = %bucket_err, key = %key_err, "S3 processing task was abruptly cancelled");
                            RecordStatus::Failed(file.clone(), ProcessingError::ErrorAcknowledgement {
                                region: "unknown".to_string(),
                                bucket: bucket_err,
                                key: key_err,
                            })
                        }
                    }
                }
            }
        });

        let mut stream = futures::stream::iter(futures).buffer_unordered(self.state.file_concurrency);
        let mut deferred_files = Vec::new();
        let mut failed_files = Vec::new();

        let shutdown_sig = self.shutdown.clone().fuse();
        pin!(shutdown_sig);
        let mut is_shutting_down = false;
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            select! {
                _ = &mut shutdown_sig, if !is_shutting_down => {
                    is_shutting_down = true;
                    let remaining = pending_files.load(Ordering::Relaxed);
                    tracing::warn!(
                        "GRACEFUL SHUTDOWN INITIATED: Vector is wrapping up {} active S3 files in this batch. \
                         Please DO NOT force kill the process...", remaining
                    );
                }
                _ = interval.tick(), if is_shutting_down => {
                    let remaining = pending_files.load(Ordering::Relaxed);
                    if remaining > 0 {
                        tracing::warn!("SHUTDOWN STATUS: Still wrapping up {} S3 files...", remaining);
                    }
                }
                result = stream.next() => {
                    match result {
                        Some(res) => {
                            let remaining = pending_files.fetch_sub(1, Ordering::Relaxed) - 1;
                            match res {
                                RecordStatus::Success | RecordStatus::Ignored => {}
                                RecordStatus::Deferred(r) => deferred_files.push(r),
                                RecordStatus::Failed(r, e) => {
                                    tracing::error!("File processing failed: {:?}", e);
                                    failed_files.push(r);
                                }
                            }
                            if is_shutting_down && remaining == 0 {
                                tracing::info!("SHUTDOWN STATUS: All files in current event successfully wrapped up.");
                            }
                        }
                        None => break, 
                    }
                }
            }
        }
        
        let deferred_body = if !deferred_files.is_empty() {
            Some(serde_json::to_string(&CrowdStrikeFdrEvent { 
                bucket: fdr_event.bucket.clone(), 
                path_prefix: fdr_event.path_prefix.clone(), 
                files: deferred_files 
            }).unwrap())
        } else { None };

        let requeue_body = if !failed_files.is_empty() {
            Some(serde_json::to_string(&CrowdStrikeFdrEvent { 
                bucket: fdr_event.bucket.clone(), 
                path_prefix: fdr_event.path_prefix.clone(), 
                files: failed_files 
            }).unwrap())
        } else { None };

        tracing::info!("All {} S3 files in the current CrowdStrike event completed successfully.", total_records);
        Ok(HandleResult { deferred_body, requeue_body })
    }

    async fn process_file(
        &mut self,
        bucket: String,
        key: String,
        region: Option<String>,
        event_time: Option<DateTime<Utc>>,
        log_namespace: LogNamespace,
    ) -> Result<(), ProcessingError> {
        if let Some(ref r) = region {
            if self.state.region.as_ref() != r.as_str() {
                return Err(ProcessingError::WrongRegion {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    region: r.clone(),
                });
            }
        }

        if let Some(deferred) = &self.state.deferred {
            if let Some(time) = event_time {
                let delta = Utc::now() - time;
                if delta.num_seconds() > deferred.max_age_secs as i64 {
                    return Err(ProcessingError::FileTooOld {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        deferred_queue: deferred.queue_url.clone(),
                    });
                }
            }
        }

        let mut backoff = ExponentialBackoff::default().max_delay(Duration::from_secs(10));
        let max_retries = 3;
        let mut attempts = 0;
        let shutdown_sig = self.shutdown.clone();
        
        let actual_region = region.unwrap_or_else(|| self.state.region.as_ref().to_string());

        loop {
            attempts += 1;
            match self.download_and_process(&bucket, &key, &actual_region, event_time, log_namespace).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if let ProcessingError::PipelineSend { source: SendError::Closed, .. } = &err {
                        return Err(err);
                    }
                    if attempts > max_retries {
                        return Err(err);
                    }
                    let delay = backoff.next().unwrap_or(Duration::from_secs(10));
                    
                    select! {
                        _ = shutdown_sig.clone().fuse() => {
                            return Err(err);
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    async fn download_and_process(
        &mut self,
        bucket: &str,
        key: &str,
        region: &str,
        event_time: Option<DateTime<Utc>>,
        log_namespace: LogNamespace,
    ) -> Result<(), ProcessingError> {
        let download_start = Instant::now();

        tracing::info!(
            %bucket,
            %key,
            "Starting processing of S3 file"
        );

        let object_result = self
            .state
            .s3_client
            .get_object()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .send()
            .await
            .context(GetObjectSnafu {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            });

        let object = object_result?;

        debug!(
            message = "Got S3 object from SQS notification.",
            %bucket,
            %key,
        );

        let metadata = object.metadata;

        let timestamp = object.last_modified.map(|ts| {
            Utc.timestamp_opt(ts.secs(), ts.subsec_nanos())
                .single()
                .expect("invalid timestamp")
        }).or(event_time);

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);
        let object_reader = super::s3_object_decoder(
            self.state.compression,
            key,
            object.content_encoding.as_deref(),
            object.content_type.as_deref(),
            object.body,
        )
        .await;

        let mut read_error = None;
        let bytes_received = self.bytes_received.clone();
        let events_received = self.events_received.clone();
        
        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(object_reader, self.state.decoder.framer.clone())
                .map(|res| {
                    res.inspect(|bytes| {
                        bytes_received.emit(ByteSize(bytes.len()));
                    })
                    .map_err(|err| {
                        read_error = Some(err);
                    })
                    .ok()
                })
                .take_while(|res| ready(res.is_some()))
                .map(|r| r.expect("validated by take_while")),
        );

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = match &self.state.multiline {
            Some(config) => Box::new(
                LineAgg::new(
                    lines.map(|line| ((), line, ())),
                    line_agg::Logic::new(config.clone()),
                )
                .map(|(_src, line, _context, _lastline_context)| line),
            ),
            None => lines,
        };

        let mut stream = lines.flat_map(|line| {
            let events = match self.state.decoder.deserializer_parse(line) {
                Ok((events, _events_size)) => events,
                Err(_error) => SmallVec::new()
            };

            let events = events
                .into_iter()
                .map(|mut event: Event| {
                    event = event.with_batch_notifier_option(&batch);
                    if let Some(log_event) = event.maybe_as_log_mut() {
                        handle_single_log(
                            log_event,
                            log_namespace,
                            bucket,
                            key,
                            region,
                            &metadata,
                            timestamp,
                        );
                    }
                    events_received.emit(CountByteSize(1, event.estimated_json_encoded_size_of()));
                    event
                })
                .collect::<Vec<Event>>();
            futures::stream::iter(events)
        });

        let send_error = match self.out.send_event_stream(&mut stream).await {
            Ok(_) => None,
            Err(SendError::Closed) => {
                let (count, _) = stream.size_hint();
                emit!(StreamClosedError { count });
                Some(SendError::Closed)
            }
            Err(SendError::Timeout) => unreachable!("No timeout is configured here"),
        };

        drop(stream);

        let duration = download_start.elapsed();

        if read_error.is_some() {
            emit!(S3ObjectProcessingFailed { bucket, duration });
        } else {
            emit!(S3ObjectProcessingSucceeded { bucket, duration });
        }

        drop(batch);

        if let Some(error) = read_error {
            tracing::error!(
                %bucket,
                %key,
                ?error,
                "Finished processing of S3 file with read error"
            );
            Err(ProcessingError::ReadObject {
                source: error,
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            })
        } else if let Some(error) = send_error {
            tracing::error!(
                %bucket,
                %key,
                ?error,
                "Finished processing of S3 file with send error"
            );
            Err(ProcessingError::PipelineSend {
                source: error,
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            })
        } else {
            match receiver {
                None => {
                    tracing::info!(
                        %bucket,
                        %key,
                        "Finished processing of S3 file successfully (No acknowledgements)"
                    );
                    Ok(())
                },
                Some(receiver) => {
                    let result = receiver.await;
                    match result {
                        BatchStatus::Delivered => {
                            debug!(
                                message = "S3 object from SQS delivered.",
                                %bucket,
                                %key,
                            );
                            tracing::info!(
                                %bucket,
                                %key,
                                "Finished processing of S3 file successfully (Delivered)"
                            );
                            Ok(())
                        }
                        BatchStatus::Errored => {
                            tracing::error!(
                                %bucket,
                                %key,
                                "Finished processing of S3 file (Errored)"
                            );
                            Err(ProcessingError::ErrorAcknowledgement {
                                bucket: bucket.to_owned(),
                                key: key.to_owned(),
                                region: region.to_owned(),
                            })
                        },
                        BatchStatus::Rejected => {
                            tracing::error!(
                                %bucket,
                                %key,
                                "Finished processing of S3 file (Rejected)"
                            );
                            if self.state.delete_failed_message {
                                warn!(
                                    message = "S3 object from SQS was rejected. Deleting failed message.",
                                    %bucket,
                                    %key,
                                );
                                Ok(())
                            } else {
                                Err(ProcessingError::ErrorAcknowledgement {
                                    bucket: bucket.to_owned(),
                                    key: key.to_owned(),
                                    region: region.to_owned(),
                                })
                            }
                        }
                    }
                }
            }
        }
    }

    async fn receive_messages(
        &mut self,
    ) -> Result<Vec<Message>, SdkError<ReceiveMessageError, HttpResponse>> {
        self.state
            .sqs_client
            .receive_message()
            .queue_url(self.state.queue_url.clone())
            .max_number_of_messages(self.state.max_number_of_messages)
            .visibility_timeout(self.state.visibility_timeout_secs)
            .wait_time_seconds(self.state.poll_secs)
            .message_system_attribute_names(MessageSystemAttributeName::SentTimestamp)
            .message_attribute_names("VectorRetryCount")
            .send()
            .map_ok(|res| res.messages.unwrap_or_default())
            .await
    }

    async fn delete_messages(
        &mut self,
        entries: Vec<DeleteMessageBatchRequestEntry>,
    ) -> Result<DeleteMessageBatchOutput, SdkError<DeleteMessageBatchError, HttpResponse>> {
        self.state
            .sqs_client
            .delete_message_batch()
            .queue_url(self.state.queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await
    }

    async fn send_messages(
        &mut self,
        entries: Vec<SendMessageBatchRequestEntry>,
        queue_url: String,
    ) -> Result<SendMessageBatchOutput, SdkError<SendMessageBatchError, HttpResponse>> {
        self.state
            .sqs_client
            .send_message_batch()
            .queue_url(queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await
    }
}

impl Clone for IngestorProcess {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            out: self.out.clone(),
            shutdown: self.shutdown.clone(),
            acknowledgements: self.acknowledgements,
            log_namespace: self.log_namespace,
            bytes_received: self.bytes_received.clone(),
            events_received: self.events_received.clone(),
            backoff: ExponentialBackoff::default().max_delay(Duration::from_secs(30)),
        }
    }
}

fn handle_single_log(
    log: &mut LogEvent,
    log_namespace: LogNamespace,
    bucket: &str,
    key: &str,
    region: &str,
    metadata: &Option<HashMap<String, String>>,
    timestamp: Option<DateTime<Utc>>,
) {
    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("bucket"))),
        path!("bucket"),
        Bytes::from(bucket.to_owned()),
    );

    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("object"))),
        path!("object"),
        Bytes::from(key.to_owned()),
    );
    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        log,
        Some(LegacyKey::Overwrite(path!("region"))),
        path!("region"),
        Bytes::from(region.to_owned()),
    );

    if let Some(metadata) = metadata {
        for (key, value) in metadata {
            log_namespace.insert_source_metadata(
                AwsS3Config::NAME,
                log,
                Some(LegacyKey::Overwrite(path!(key))),
                path!("metadata", key.as_str()),
                value.clone(),
            );
        }
    }

    log_namespace.insert_vector_metadata(
        log,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(AwsS3Config::NAME.as_bytes()),
    );

    match log_namespace {
        LogNamespace::Vector => {
            if let Some(timestamp) = timestamp {
                log.insert(metadata_path!(AwsS3Config::NAME, "timestamp"), timestamp);
            }

            log.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                log.try_insert(
                    (PathPrefix::Event, timestamp_key),
                    timestamp.unwrap_or_else(Utc::now),
                );
            }
        }
    };
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnsNotification {
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum SqsEvent {
    Event(S3Event),
    TestEvent(S3TestEvent),
    CrowdStrike(CrowdStrikeFdrEvent),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrowdStrikeFdrEvent {
    pub bucket: String,
    pub path_prefix: String,
    pub files: Vec<CrowdStrikeFdrFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrowdStrikeFdrFile {
    pub path: String,
    pub size: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3TestEvent {
    pub service: String,
    pub event: S3EventName,
    pub bucket: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct S3Event {
    pub records: Vec<S3EventRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3EventRecord {
    pub event_version: S3EventVersion,
    pub event_source: String,
    pub aws_region: String,
    pub event_name: S3EventName,
    pub event_time: DateTime<Utc>,
    pub s3: S3Message,
}

#[derive(Clone, Debug)]
pub struct S3EventVersion {
    pub major: u64,
    pub minor: u64,
}

impl From<S3EventVersion> for semver::Version {
    fn from(v: S3EventVersion) -> semver::Version {
        semver::Version::new(v.major, v.minor, 0)
    }
}

impl<'de> Deserialize<'de> for S3EventVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let s = String::deserialize(deserializer)?;

        let mut parts = s.splitn(2, '.');

        let major = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing major version number"))?
            .parse::<u64>()
            .map_err(D::Error::custom)?;

        let minor = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing minor version number"))?
            .parse::<u64>()
            .map_err(D::Error::custom)?;

        Ok(S3EventVersion { major, minor })
    }
}

impl Serialize for S3EventVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}.{}", self.major, self.minor))
    }
}

#[derive(Clone, Debug)]
pub struct S3EventName {
    pub kind: String,
    pub name: String,
}

impl<'de> Deserialize<'de> for S3EventName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let s = String::deserialize(deserializer)?;

        let mut parts = s.splitn(2, ':');

        let kind = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing event kind"))?
            .parse::<String>()
            .map_err(D::Error::custom)?;

        let name = parts
            .next()
            .ok_or_else(|| D::Error::custom("Missing event name"))?
            .parse::<String>()
            .map_err(D::Error::custom)?;

        Ok(S3EventName { kind, name })
    }
}

impl Serialize for S3EventName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.kind, self.name))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Message {
    pub bucket: S3Bucket,
    pub object: S3Object,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Bucket {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Object {
    #[serde(with = "urlencoded_string")]
    pub key: String,
}

mod urlencoded_string {
    use percent_encoding::{percent_decode, utf8_percent_encode};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        use serde::de::Error;

        serde::de::Deserialize::deserialize(deserializer).and_then(|s: &[u8]| {
            let decoded = if s.contains(&b'+') {
                let s = s
                    .iter()
                    .map(|c| if *c == b'+' { b' ' } else { *c })
                    .collect::<Vec<_>>();
                percent_decode(&s).decode_utf8().map(Into::into)
            } else {
                percent_decode(s).decode_utf8().map(Into::into)
            };

            decoded
                .map_err(|err| D::Error::custom(format!("error url decoding S3 object key: {err}")))
        })
    }

    pub fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(
            &utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).collect::<String>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

   #[test]
    fn test_key_deserialize() {
        let value: S3Object = serde_json::from_str(r#"{"key": "noog+nork"}"#).unwrap();
        assert_eq!(
            S3Object {
                key: "noog nork".to_string(),
            },
            value
        );

        let value: S3Object = serde_json::from_str(r#"{"key": "noog%2bnork"}"#).unwrap();
        assert_eq!(
            S3Object {
                key: "noog+nork".to_string(),
            },
            value
        );
    }

    #[test]
    fn test_s3_testevent() {
        let value: S3TestEvent = serde_json::from_str(
            r#"{
            "Service":"Amazon S3",
            "Event":"s3:TestEvent",
            "Time":"2014-10-13T15:57:02.089Z",
            "Bucket":"bucketname",
            "RequestId":"5582815E1AEA5ADF",
            "HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"
         }"#,
        )
        .unwrap();

        assert_eq!(value.service, "Amazon S3".to_string());
        assert_eq!(value.bucket, "bucketname".to_string());
        assert_eq!(value.event.kind, "s3".to_string());
        assert_eq!(value.event.name, "TestEvent".to_string());
    }

    #[test]
    fn test_s3_sns_testevent() {
        let sns_value: SnsNotification = serde_json::from_str(
            r#"{
            "Type" : "Notification",
            "MessageId" : "63a3f6b6-d533-4a47-aef9-fcf5cf758c76",
            "TopicArn" : "arn:aws:sns:us-west-2:123456789012:MyTopic",
            "Subject" : "Testing publish to subscribed queues",
            "Message" : "{\"Bucket\":\"bucketname\",\"Event\":\"s3:TestEvent\",\"HostId\":\"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE\",\"RequestId\":\"5582815E1AEA5ADF\",\"Service\":\"Amazon S3\",\"Time\":\"2014-10-13T15:57:02.089Z\"}",
            "Timestamp" : "2012-03-29T05:12:16.901Z",
            "SignatureVersion" : "1",
            "Signature" : "EXAMPLEnTrFPa3...",
            "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem",
            "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:123456789012:MyTopic:c7fe3a54-ab0e-4ec2-88e0-db410a0f2bee"
         }"#,
        ).unwrap();

        assert_eq!(
            sns_value.timestamp,
            DateTime::parse_from_rfc3339("2012-03-29T05:12:16.901Z")
                .unwrap()
                .to_utc()
        );

        let value: S3TestEvent = serde_json::from_str(sns_value.message.as_ref()).unwrap();

        assert_eq!(value.service, "Amazon S3".to_string());
        assert_eq!(value.bucket, "bucketname".to_string());
        assert_eq!(value.event.kind, "s3".to_string());
        assert_eq!(value.event.name, "TestEvent".to_string());
    }

    #[test]
    fn test_crowdstrike_fdr_event() {
        let payload = r#"{
            "bucket": "fdr-your-company-bucket",
            "pathPrefix": "data/2026-03-31/",
            "files": [
                {
                    "path": "data/2026-03-31/fdr_telemetry.json.gz",
                    "size": 123456
                }
            ]
        }"#;

        let parsed: SqsEvent = serde_json::from_str(payload).unwrap();
        match parsed {
            SqsEvent::CrowdStrike(fdr) => {
                assert_eq!(fdr.bucket, "fdr-your-company-bucket");
                assert_eq!(fdr.files[0].path, "data/2026-03-31/fdr_telemetry.json.gz");
                assert_eq!(fdr.files[0].size, 123456);
            }
            _ => panic!("Did not parse as CrowdStrike event"),
        }
    }

    #[test]
    fn parse_sqs_config() {
        let config: Config = toml::from_str(
            r#"
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
            "#,
        )
        .unwrap();
        assert_eq!(
            config.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
        );
        assert!(config.deferred.is_none());

        let config: Config = toml::from_str(
            r#"
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
                [deferred]
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
                max_age_secs = 3600
            "#,
        )
        .unwrap();
        assert_eq!(
            config.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
        );
        let Some(deferred) = config.deferred else {
            panic!("Expected deferred config");
        };
        assert_eq!(
            deferred.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
        );
        assert_eq!(deferred.max_age_secs, 3600);

        let test: Result<Config, toml::de::Error> = toml::from_str(
            r#"
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
                [deferred]
                max_age_secs = 3600
            "#,
        );
        assert!(test.is_err());

        let test: Result<Config, toml::de::Error> = toml::from_str(
            r#"
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
                [deferred]
                queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyDeferredQueue"
            "#,
        );
        assert!(test.is_err());
    }
}