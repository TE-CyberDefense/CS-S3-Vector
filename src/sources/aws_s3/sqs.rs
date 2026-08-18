use std::{
    collections::HashMap,
    future::ready,
    num::NonZeroUsize,
    panic,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock, Mutex,
    },
    time::{Duration, Instant},
};

use aws_sdk_s3::{operation::get_object::GetObjectError, Client as S3Client};
use aws_sdk_sqs::{
    operation::{
        delete_message_batch::{DeleteMessageBatchError, DeleteMessageBatchOutput},
        receive_message::ReceiveMessageError,
        send_message_batch::{SendMessageBatchError, SendMessageBatchOutput},
    },
    types::{
        DeleteMessageBatchRequestEntry, Message, MessageAttributeValue,
        MessageSystemAttributeName, SendMessageBatchRequestEntry,
    },
    Client as SqsClient,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_types::region::Region;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use derivative::Derivative;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use snafu::{ResultExt, Snafu};
use tokio::{
    pin, select,
    io::BufReader,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tokio_util::codec::FramedRead;
use tracing::Instrument;
use vector_lib::{
    config::{log_schema, LegacyKey, LogNamespace},
    configurable::configurable_component,
    event::MaybeAsLogMut,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol, Registered,
    },
    lookup::{metadata_path, path, PathPrefix},
    source_sender::SendError,
};

use crate::{
    aws::AwsTimeout,
    codecs::Decoder,
    common::backoff::ExponentialBackoff,
    config::{SourceAcknowledgementsConfig, SourceContext},
    event::{BatchNotifier, BatchStatus, EstimatedJsonEncodedSizeOf, LogEvent},
    internal_events::{
        EventsReceived, S3ObjectProcessingFailed, S3ObjectProcessingSucceeded,
        SqsMessageProcessingError, SqsMessageProcessingSucceeded, SqsMessageReceiveError,
        SqsMessageReceiveSucceeded, SqsS3EventRecordInvalidEventIgnored, StreamClosedError,
    },
    line_agg::{self, LineAgg},
    shutdown::ShutdownSignal,
    sources::aws_s3::AwsS3Config,
    tls::TlsConfig,
    SourceSender,
};

static SUPPORTED_S3_EVENT_VERSION: LazyLock<semver::VersionReq> =
    LazyLock::new(|| semver::VersionReq::parse("~2").unwrap());

const DEFAULT_PARSE_CONCURRENCY_PER_FILE: usize = 2;
// Increased to reduce tokio task scheduling overhead
const DEFAULT_PARSE_CHUNK_SIZE: usize = 2500;
const DEFAULT_READ_BUFFER_BYTES: usize = 128 * 1024;
const MAX_REQUEUE_ATTEMPTS: u32 = 5;
const REQUEUE_DELAY_SECONDS: i32 = 30;

#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct DeferredConfig {
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::examples = 3600))]
    pub(super) max_age_secs: u64,
}

#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    #[configurable(metadata(
        docs::examples = "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
    ))]
    #[configurable(validation(format = "uri"))]
    pub(super) queue_url: String,

    #[serde(default = "default_poll_secs")]
    #[derivative(Default(value = "default_poll_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub(super) poll_secs: u32,

    #[serde(default = "default_visibility_timeout_secs")]
    #[derivative(Default(value = "default_visibility_timeout_secs()"))]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Visibility Timeout"))]
    pub(super) visibility_timeout_secs: u32,

    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_message: bool,

    #[serde(default = "default_true")]
    #[derivative(Default(value = "default_true()"))]
    pub(super) delete_failed_message: bool,

    #[configurable(metadata(docs::type_unit = "tasks"))]
    #[configurable(metadata(docs::examples = 5))]
    pub(super) client_concurrency: Option<NonZeroUsize>,

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

    #[configurable(derived)]
    pub(super) deferred: Option<DeferredConfig>,

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
        source: String,
        bucket: String,
        key: String,
    },

    #[snafu(display("Parser worker failed while processing s3://{}/{}: {}", bucket, key, error))]
    ParseTaskJoin {
        bucket: String,
        key: String,
        error: String,
    },

    #[snafu(display("Failed to flush all of s3://{}/{}: {}", bucket, key, source))]
    PipelineSend {
        source: SendError,
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

    #[snafu(display("Unsupported S3 event version: {}.", version))]
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
        "File s3://{}/{} too old. Forwarded to deferred queue {}",
        bucket,
        key,
        deferred_queue
    ))]
    FileTooOld {
        bucket: String,
        key: String,
        deferred_queue: String,
    },

    #[snafu(display(
        "Checksum mismatch for s3://{}/{}: expected {}, got {}",
        bucket,
        key,
        expected,
        actual
    ))]
    ChecksumMismatch {
        bucket: String,
        key: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Failed to serialize partial SQS retry/deferred message body: {}", source))]
    SerializePartialMessage { source: serde_json::Error },
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
    sqs_message_semaphore: Arc<Semaphore>,
    file_semaphore: Arc<Semaphore>,
    parse_concurrency_per_file: usize,
    parse_chunk_size: usize,
    read_buffer_bytes: usize,
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

        let client_concurrency = config
            .client_concurrency
            .map(|n| n.get())
            .unwrap_or_else(crate::num_threads)
            .max(1);

        let file_concurrency = config.file_concurrency.max(1);

        let state = Arc::new(State {
            region,
            s3_client,
            sqs_client,
            compression,
            multiline,
            queue_url: config.queue_url,
            poll_secs: config.poll_secs as i32,
            max_number_of_messages: config.max_number_of_messages as i32,
            client_concurrency,
            file_concurrency,
            visibility_timeout_secs: config.visibility_timeout_secs as i32,
            delete_message: config.delete_message,
            delete_failed_message: config.delete_failed_message,
            decoder,
            deferred: config.deferred,
            sqs_message_semaphore: Arc::new(Semaphore::new(client_concurrency)),
            file_semaphore: Arc::new(Semaphore::new(file_concurrency)),
            parse_concurrency_per_file: DEFAULT_PARSE_CONCURRENCY_PER_FILE,
            parse_chunk_size: DEFAULT_PARSE_CHUNK_SIZE,
            read_buffer_bytes: DEFAULT_READ_BUFFER_BYTES,
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

        let poller = SqsPoller {
            state: Arc::clone(&self.state),
            out: cx.out.clone(),
            shutdown: cx.shutdown.clone(),
            log_namespace,
            acknowledgements,
            bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            events_received: register!(EventsReceived),
            backoff: ExponentialBackoff::default().max_delay(Duration::from_secs(30)),
            tasks: JoinSet::new(),
        };

        poller.run().in_current_span().await;

        Ok(())
    }
}

struct SqsPoller {
    state: Arc<State>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    backoff: ExponentialBackoff,
    tasks: JoinSet<()>,
}

impl SqsPoller {
    async fn run(mut self) {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        tracing::info!("SQS ingestor poller started. Ready to poll for new messages.");

        loop {
            while let Some(result) = self.tasks.try_join_next() {
                self.handle_join_result(result);
            }

            let first_permit = select! {
                _ = &mut shutdown => {
                    tracing::info!("Shutdown signal received for SQS poller. Stopping receive loop.");
                    break;
                }
                permit = self.state.sqs_message_semaphore.clone().acquire_owned() => {
                    match permit {
                        Ok(permit) => permit,
                        Err(_) => break,
                    }
                }
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(result) = result {
                        self.handle_join_result(result);
                    }

                    continue;
                }
            };

            let available_after_first = self.state.sqs_message_semaphore.available_permits();

            let receive_count = (1 + available_after_first)
                .min(self.state.max_number_of_messages as usize)
                .max(1) as i32;

            let messages = select! {
                _ = &mut shutdown => {
                    drop(first_permit);
                    tracing::info!("Shutdown signal received before SQS receive completed.");
                    break;
                }
                result = self.receive_messages(receive_count) => {
                    match result {
                        Ok(messages) => {
                            emit!(SqsMessageReceiveSucceeded {
                                count: messages.len(),
                            });

                            self.backoff.reset();
                            messages
                        }
                        Err(err) => {
                            drop(first_permit);

                            emit!(SqsMessageReceiveError {
                                error: &err,
                            });

                            let delay = self.backoff.next().expect("backoff never ends");

                            tracing::trace!(
                                delay_ms = delay.as_millis(),
                                "`receive_messages` failed, will retry after delay.",
                            );

                            select! {
                                _ = &mut shutdown => break,
                                _ = tokio::time::sleep(delay) => {}
                            }

                            continue;
                        }
                    }
                }
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    drop(first_permit);

                    if let Some(result) = result {
                        self.handle_join_result(result);
                    }

                    continue;
                }
            };

            if messages.is_empty() {
                drop(first_permit);
                continue;
            }

            let mut messages_iter = messages.into_iter();

            if let Some(first_message) = messages_iter.next() {
                self.spawn_worker(first_message, first_permit);
            }

            for message in messages_iter {
                let permit = select! {
                    _ = &mut shutdown => {
                        tracing::info!("Shutdown signal received while dispatching SQS messages.");
                        break;
                    }
                    permit = self.state.sqs_message_semaphore.clone().acquire_owned() => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => break,
                        }
                    }
                    result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                        if let Some(result) = result {
                            self.handle_join_result(result);
                        }

                        continue;
                    }
                };

                self.spawn_worker(message, permit);
            }
        }

        tracing::info!(
            in_flight_tasks = self.tasks.len(),
            "SQS poller stopped receiving. Waiting for in-flight SQS message tasks to finish."
        );

        while let Some(result) = self.tasks.join_next().await {
            self.handle_join_result(result);
        }

        tracing::info!("SQS ingestor poller stopped.");
    }

    fn spawn_worker(&mut self, message: Message, permit: OwnedSemaphorePermit) {
        let mut worker = S3Worker {
            state: Arc::clone(&self.state),
            out: self.out.clone(),
            shutdown: self.shutdown.clone(),
            acknowledgements: self.acknowledgements,
            log_namespace: self.log_namespace,
            bytes_received: self.bytes_received.clone(),
            events_received: self.events_received.clone(),
        };

        self.tasks.spawn(
            async move {
                let _permit = permit;
                worker.process_message(message).await;
            }
            .in_current_span(),
        );
    }

    fn handle_join_result(&self, result: Result<(), tokio::task::JoinError>) {
        if let Err(err) = result {
            if err.is_panic() {
                panic::resume_unwind(err.into_panic());
            }

            tracing::warn!("SQS message worker task was cancelled.");
        }
    }

    async fn receive_messages(
        &mut self,
        max_number_of_messages: i32,
    ) -> Result<Vec<Message>, SdkError<ReceiveMessageError, HttpResponse>> {
        self.state
            .sqs_client
            .receive_message()
            .queue_url(self.state.queue_url.clone())
            .max_number_of_messages(max_number_of_messages)
            .visibility_timeout(self.state.visibility_timeout_secs)
            .wait_time_seconds(self.state.poll_secs)
            .message_system_attribute_names(MessageSystemAttributeName::SentTimestamp)
            .message_attribute_names("VectorRetryCount")
            .send()
            .map_ok(|res| res.messages.unwrap_or_default())
            .await
    }
}

#[derive(Clone)]
struct S3Worker {
    state: Arc<State>,
    out: SourceSender,
    shutdown: ShutdownSignal,
    acknowledgements: bool,
    log_namespace: LogNamespace,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
}

impl S3Worker {
    async fn process_message(&mut self, message: Message) {
        let receipt_handle = match message.receipt_handle.as_ref() {
            None => {
                tracing::warn!(
                    message = "Refusing to process message with no receipt_handle.",
                    ?message.message_id
                );
                return;
            }
            Some(handle) => handle.to_owned(),
        };

        let message_id = message
            .message_id
            .clone()
            .unwrap_or_else(|| "unknown".to_owned());

        let retry_count = message
            .message_attributes
            .as_ref()
            .and_then(|attrs| attrs.get("VectorRetryCount"))
            .and_then(|attr| attr.string_value.as_deref())
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let safe_batch_id = message_id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(80)
            .collect::<String>();

        let safe_batch_id = if safe_batch_id.is_empty() {
            format!("msg-{}", Utc::now().timestamp_millis())
        } else {
            safe_batch_id
        };

        let original_body = message.body.clone().unwrap_or_default();

        let (mut can_delete_original, deferred_body, requeue_body) = match self.handle_sqs_message(message, retry_count).await {
            Ok(result) => {
                emit!(SqsMessageProcessingSucceeded {
                    message_id: &message_id
                });
                (true, result.deferred_body, result.requeue_body)
            }
            Err(err) => {
                emit!(SqsMessageProcessingError {
                    message_id: &message_id,
                    error: &err,
                });
                // Treat the entire message as a failure so it goes through retry/DLQ/deletion logic
                (true, None, Some(original_body))
            }
        };

        if let Some(body) = deferred_body {
            if let Some(deferred) = &self.state.deferred {
                let entry = SendMessageBatchRequestEntry::builder()
                    .id(safe_batch_id.clone())
                    .message_body(body)
                    .build();

                match entry {
                    Ok(entry) => {
                        match self.send_messages(vec![entry], deferred.queue_url.clone()).await {
                            Ok(output) => {
                                let failed_entries = output.failed.as_ref().map_or(0, Vec::len);
                                if failed_entries > 0 {
                                    can_delete_original = false;
                                    tracing::error!(
                                        message_id = %message_id,
                                        failed_entries,
                                        failures = ?output.failed,
                                        "Deferred SQS batch returned failed entries. Original message will not be deleted."
                                    );
                                }
                            }
                            Err(err) => {
                                can_delete_original = false;
                                tracing::error!(
                                    message_id = %message_id,
                                    error = ?err,
                                    "Failed to send deferred SQS message. Original message will not be deleted."
                                );
                            }
                        }
                    }
                    Err(err) => {
                        can_delete_original = false;

                        tracing::error!(
                            message_id = %message_id,
                            error = ?err,
                            "Failed to build deferred SQS message. Original message will not be deleted."
                        );
                    }
                }
            }
        }

        if let Some(body) = requeue_body {
            if self.state.delete_failed_message {
                tracing::warn!(
                    message_id = %message_id,
                    "Failed records were not requeued because delete_failed_message is enabled."
                );
            } else if retry_count >= MAX_REQUEUE_ATTEMPTS {
                can_delete_original = false;
                tracing::error!(
                    message_id = %message_id,
                    retry_count,
                    max_requeue_attempts = MAX_REQUEUE_ATTEMPTS,
                    "Failed records exceeded the local requeue attempt limit. Original message will not be deleted."
                );
            } else {
                let attr_val = match MessageAttributeValue::builder()
                    .data_type("Number")
                    .string_value((retry_count + 1).to_string())
                    .build()
                {
                    Ok(attr_val) => attr_val,
                    Err(err) => {
                        tracing::error!(
                            message_id = %message_id,
                            error = ?err,
                            "Failed to build retry count attribute. Original message will not be deleted."
                        );
                        return;
                    }
                };

                let entry = SendMessageBatchRequestEntry::builder()
                    .id(safe_batch_id.clone())
                    .message_body(body)
                    .delay_seconds(REQUEUE_DELAY_SECONDS)
                    .message_attributes("VectorRetryCount", attr_val)
                    .build();

                match entry {
                    Ok(entry) => {
                        match self.send_messages(vec![entry], self.state.queue_url.clone()).await {
                            Ok(output) => {
                                let failed_entries = output.failed.as_ref().map_or(0, Vec::len);
                                if failed_entries > 0 {
                                    can_delete_original = false;
                                    tracing::error!(
                                        message_id = %message_id,
                                        failed_entries,
                                        failures = ?output.failed,
                                        "Requeue SQS batch returned failed entries. Original message will not be deleted."
                                    );
                                }
                            }
                            Err(err) => {
                                can_delete_original = false;
                                tracing::error!(
                                    message_id = %message_id,
                                    error = ?err,
                                    "Failed to requeue SQS message. Original message will not be deleted."
                                );
                            }
                        }
                    }
                    Err(err) => {
                        can_delete_original = false;
                        tracing::error!(
                            message_id = %message_id,
                            error = ?err,
                            "Failed to build requeue SQS message. Original message will not be deleted."
                        );
                    }
                }
            }
        }

        if self.state.delete_message && can_delete_original {
            let entry = DeleteMessageBatchRequestEntry::builder()
                .id(safe_batch_id)
                .receipt_handle(receipt_handle)
                .build();

            match entry {
                Ok(entry) => {
                    match self.delete_messages(vec![entry]).await {
                        Ok(output) => {
                            let failed_entries = output.failed.as_ref().map_or(0, Vec::len);
                            if failed_entries > 0 {
                                tracing::error!(
                                    message_id = %message_id,
                                    failed_entries,
                                    failures = ?output.failed,
                                    "Delete SQS batch returned failed entries."
                                );
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                message_id = %message_id,
                                error = ?err,
                                "Failed to delete SQS message."
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(
                        message_id = %message_id,
                        error = ?err,
                        "Failed to build SQS delete request."
                    );
                }
            }
        }
    }

    async fn handle_sqs_message(
        &mut self,
        message: Message,
        _retry_count: u32,
    ) -> Result<HandleResult, ProcessingError> {
        let mut sqs_body = message.body.unwrap_or_default();

        let sent_timestamp = message
            .attributes
            .as_ref()
            .and_then(|attrs| attrs.get(&MessageSystemAttributeName::SentTimestamp))
            .and_then(|ts| ts.parse::<i64>().ok())
            .and_then(|ts| Utc.timestamp_millis_opt(ts).single());

        if let Ok(notification) = serde_json::from_str::<SnsNotification>(&sqs_body) {
            sqs_body = notification.message;
        }

        let s3_event: SqsEvent =
            serde_json::from_str(&sqs_body).context(InvalidSqsMessageSnafu {
                message_id: message
                    .message_id
                    .clone()
                    .unwrap_or_else(|| "<empty>".to_owned()),
            })?;

        match s3_event {
            SqsEvent::TestEvent(_s3_test_event) => {
                tracing::debug!(?message.message_id, message = "Found S3 Test Event.");

                Ok(HandleResult {
                    deferred_body: None,
                    requeue_body: None,
                })
            }
            SqsEvent::Event(s3_event) => self.handle_s3_event(s3_event).await,
            SqsEvent::CrowdStrike(fdr_event) => {
                let timestamp_human = fdr_event
                    .timestamp
                    .and_then(|ts| Utc.timestamp_millis_opt(ts).single().map(|t| t.to_rfc3339()))
                    .unwrap_or_else(|| "unknown".to_string());

                tracing::info!(
                    message_id = ?message.message_id,
                    total_size = ?fdr_event.total_size,
                    file_count = ?fdr_event.file_count,
                    timestamp = %timestamp_human,
                    "CrowdStrike FDR event details"
                );

                self.handle_crowdstrike_event(fdr_event, sent_timestamp).await
            }
        }
    }

    async fn handle_s3_event(
        &mut self,
        s3_event: S3Event,
    ) -> Result<HandleResult, ProcessingError> {
        let per_message_parallelism = self
            .state
            .file_concurrency
            .min(s3_event.records.len().max(1));

        let futures = s3_event.records.into_iter().map(|record| {
            let mut process = self.clone();

            async move {
                let event_version: semver::Version = record.event_version.clone().into();

                if !SUPPORTED_S3_EVENT_VERSION.matches(&event_version) {
                    return RecordStatus::Failed(
                        record,
                        ProcessingError::UnsupportedS3EventVersion {
                            version: event_version,
                        },
                    );
                }

                if record.event_name.kind != "ObjectCreated" {
                    emit!(SqsS3EventRecordInvalidEventIgnored {
                        bucket: &record.s3.bucket.name,
                        key: &record.s3.object.key,
                        kind: &record.event_name.kind,
                        name: &record.event_name.name,
                    });

                    return RecordStatus::Ignored;
                }

                match process
                    .process_file(
                        record.s3.bucket.name.clone(),
                        record.s3.object.key.clone(),
                        None,
                        None,
                        Some(record.aws_region.clone()),
                        Some(record.event_time),
                        process.log_namespace,
                    )
                    .await
                {
                    Ok(()) => RecordStatus::Success,
                    Err(ProcessingError::FileTooOld { .. }) => RecordStatus::Deferred(record),
                    Err(err) => RecordStatus::Failed(record, err),
                }
            }
        });

        let mut stream = futures::stream::iter(futures).buffer_unordered(per_message_parallelism);
        let mut deferred_records = Vec::new();
        let mut failed_records = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                RecordStatus::Success | RecordStatus::Ignored => {}
                RecordStatus::Deferred(record) => deferred_records.push(record),
                RecordStatus::Failed(record, err) => {
                    tracing::error!("File processing failed: {:?}", err);
                    failed_records.push(record);
                }
            }
        }

        let deferred_body = if deferred_records.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&S3Event {
                    records: deferred_records,
                })
                .context(SerializePartialMessageSnafu)?,
            )
        };

        let requeue_body = if failed_records.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&S3Event {
                    records: failed_records,
                })
                .context(SerializePartialMessageSnafu)?,
            )
        };

        Ok(HandleResult {
            deferred_body,
            requeue_body,
        })
    }

    async fn handle_crowdstrike_event(
        &mut self,
        fdr_event: CrowdStrikeFdrEvent,
        sent_timestamp: Option<DateTime<Utc>>,
    ) -> Result<HandleResult, ProcessingError> {
        let per_message_parallelism = self
            .state
            .file_concurrency
            .min(fdr_event.files.len().max(1));

        let base_event = fdr_event.clone();

        let futures = fdr_event.files.into_iter().map(|file| {
            let mut process = self.clone();
            let bucket = base_event.bucket.clone();

            async move {
                match process
                    .process_file(
                        bucket,
                        file.path.clone(),
                        Some(file.size),
                        file.checksum.clone(),
                        None,
                        sent_timestamp,
                        process.log_namespace,
                    )
                    .await
                {
                    Ok(()) => RecordStatus::Success,
                    Err(ProcessingError::FileTooOld { .. }) => RecordStatus::Deferred(file),
                    Err(err) => RecordStatus::Failed(file, err),
                }
            }
        });

        let mut stream = futures::stream::iter(futures).buffer_unordered(per_message_parallelism);
        let mut deferred_files = Vec::new();
        let mut failed_files = Vec::new();

        while let Some(result) = stream.next().await {
            match result {
                RecordStatus::Success | RecordStatus::Ignored => {}
                RecordStatus::Deferred(file) => deferred_files.push(file),
                RecordStatus::Failed(file, err) => {
                    tracing::error!("File processing failed: {:?}", err);
                    failed_files.push(file);
                }
            }
        }

        let deferred_body = if deferred_files.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&CrowdStrikeFdrEvent {
                    cid: base_event.cid.clone(),
                    timestamp: base_event.timestamp,
                    file_count: base_event.file_count,
                    total_size: base_event.total_size,
                    bucket: base_event.bucket.clone(),
                    path_prefix: base_event.path_prefix.clone(),
                    files: deferred_files,
                })
                .context(SerializePartialMessageSnafu)?,
            )
        };

        let requeue_body = if failed_files.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&CrowdStrikeFdrEvent {
                    cid: base_event.cid,
                    timestamp: base_event.timestamp,
                    file_count: base_event.file_count,
                    total_size: base_event.total_size,
                    bucket: base_event.bucket,
                    path_prefix: base_event.path_prefix,
                    files: failed_files,
                })
                .context(SerializePartialMessageSnafu)?,
            )
        };

        Ok(HandleResult {
            deferred_body,
            requeue_body,
        })
    }

    async fn process_file(
        &mut self,
        bucket: String,
        key: String,
        size: Option<i64>,
        expected_checksum: Option<String>,
        region: Option<String>,
        event_time: Option<DateTime<Utc>>,
        log_namespace: LogNamespace,
    ) -> Result<(), ProcessingError> {
        if let Some(ref r) = region {
            if self.state.region.as_ref() != r.as_str() {
                return Err(ProcessingError::WrongRegion {
                    bucket,
                    key,
                    region: r.clone(),
                });
            }
        }

        let actual_region = region.unwrap_or_else(|| self.state.region.as_ref().to_string());

        if let Some(deferred) = &self.state.deferred {
            if let Some(time) = event_time {
                let delta = Utc::now() - time;

                if delta.num_seconds() > deferred.max_age_secs as i64 {
                    return Err(ProcessingError::FileTooOld {
                        bucket,
                        key,
                        deferred_queue: deferred.queue_url.clone(),
                    });
                }
            }
        }

        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        let _file_permit = select! {
            _ = &mut shutdown => {
                return Err(ProcessingError::ErrorAcknowledgement {
                    region: actual_region,
                    bucket,
                    key,
                });
            }
            permit = self.state.file_semaphore.clone().acquire_owned() => {
                match permit {
                    Ok(permit) => permit,
                    Err(_) => {
                        return Err(ProcessingError::ErrorAcknowledgement {
                            region: actual_region,
                            bucket,
                            key,
                        });
                    }
                }
            }
        };

        let mut backoff = ExponentialBackoff::default().max_delay(Duration::from_secs(10));
        let max_retries = 3;
        let mut attempts = 0;

        loop {
            attempts += 1;

            match self
                .download_and_process(
                    &bucket,
                    &key,
                    size,
                    expected_checksum.clone(),
                    &actual_region,
                    event_time,
                    log_namespace,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if let ProcessingError::PipelineSend {
                        source: SendError::Closed,
                        ..
                    } = &err
                    {
                        return Err(err);
                    }

                    if attempts > max_retries {
                        return Err(err);
                    }

                    let delay = backoff.next().unwrap_or(Duration::from_secs(10));

                    select! {
                        _ = self.shutdown.clone().fuse() => {
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
        size: Option<i64>,
        expected_checksum: Option<String>,
        region: &str,
        event_time: Option<DateTime<Utc>>,
        log_namespace: LogNamespace,
    ) -> Result<(), ProcessingError> {
        let download_start = Instant::now();

        let object = self
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
            })?;

        let compressed_size = size.or(object.content_length).unwrap_or(0);

        if let Some(expected) = expected_checksum {
            if let Some(etag) = object.e_tag() {
                let etag_clean = etag.trim_matches('"');

                if etag_clean != expected && !etag_clean.contains('-') {
                    return Err(ProcessingError::ChecksumMismatch {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        expected,
                        actual: etag_clean.to_string(),
                    });
                }
            }
        }

        let metadata = object.metadata;

        let timestamp = object
            .last_modified
            .and_then(|ts| Utc.timestamp_opt(ts.secs(), ts.subsec_nanos()).single())
            .or(event_time);

        let mut template = LogEvent::default();

        populate_template_metadata(
            &mut template,
            log_namespace,
            bucket,
            key,
            region,
            &metadata,
            timestamp,
        );
        
        let base_template = template;

        let (batch, receiver) = BatchNotifier::maybe_new_with_receiver(self.acknowledgements);

        let object_reader = super::s3_object_decoder(
            self.state.compression,
            key,
            object.content_encoding.as_deref(),
            object.content_type.as_deref(),
            object.body,
        )
        .await;

        let buffered_reader = BufReader::with_capacity(self.state.read_buffer_bytes, object_reader);

        let read_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let read_error_capture = Arc::clone(&read_error);
        let parse_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let parse_error_for_take = Arc::clone(&parse_error);
        let uncompressed_tracker = Arc::new(AtomicUsize::new(0));

        let lines: Box<dyn Stream<Item = Bytes> + Send + Unpin> = Box::new(
            FramedRead::new(buffered_reader, self.state.decoder.framer.clone())
                .map(move |res| match res {
                    Ok(line) => Some(line),
                    Err(err) => {
                        if let Ok(mut read_error) = read_error_capture.lock() {
                            if read_error.is_none() {
                                *read_error = Some(err.to_string());
                            }
                        }
                        None
                    }
                })
                .take_while(|res| ready(res.is_some()))
                .map(|res| res.expect("stream stopped on first framing error")),
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

        let events_received = self.events_received.clone();
        let decoder = self.state.decoder.clone();
        let parse_chunk_size = self.state.parse_chunk_size;
        let parse_concurrency_per_file = self.state.parse_concurrency_per_file;

        let bytes_received = self.bytes_received.clone();
        let tracker_clone = Arc::clone(&uncompressed_tracker);
        let parse_error_capture = Arc::clone(&parse_error);

        let mut stream = lines
            .chunks(parse_chunk_size)
            .take_while(move |_| {
                let has_error = parse_error_for_take.lock().ok().map_or(false, |g| g.is_some());
                ready(!has_error)
            })
            .map(move |chunk| {
                let decoder = decoder.clone();
                let batch = batch.clone();
                let base_template = base_template.clone();
                let bytes_received = bytes_received.clone();
                let tracker_clone = tracker_clone.clone();
                let parse_error_capture = Arc::clone(&parse_error_capture);

                async move {
                    let (mut chunk_events, chunk_bytes) = match tokio::task::spawn_blocking(move || {
                        if parse_error_capture.lock().ok().map_or(false, |g| g.is_some()) {
                            return (Vec::new(), 0);
                        }

                        let mut chunk_events =
                            Vec::with_capacity(chunk.len().saturating_mul(2));

                        let batch_opt = Some(batch);
                        let mut chunk_bytes = 0;

                        for line in chunk {
                            chunk_bytes += line.len();
                            match decoder.deserializer_parse(line) {
                                Ok((parsed_events, _)) => {
                                    for mut event in parsed_events {
                                        event = event.with_batch_notifier_option(&batch_opt);

                                        if let Some(event_log) = event.maybe_as_log_mut() {
                                            for (k, v) in base_template.all_fields() {
                                                event_log.insert(k.clone(), v.clone());
                                            }
                                        }

                                        chunk_events.push(event);
                                    }
                                }
                                Err(err) => {
                                    if let Ok(mut parse_error) = parse_error_capture.lock() {
                                        if parse_error.is_none() {
                                            *parse_error = Some(err.to_string());
                                        }
                                    }
                                    break;
                                }
                            }
                        }

                        (chunk_events, chunk_bytes)
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            if let Ok(mut parse_error) = parse_error_capture.lock() {
                                if parse_error.is_none() {
                                    *parse_error = Some(err.to_string());
                                }
                            }
                            (Vec::new(), 0)
                        }
                    };

                    if parse_error_capture.lock().ok().map_or(false, |g| g.is_some()) {
                        chunk_events.clear();
                    }

                    tracker_clone.fetch_add(chunk_bytes, Ordering::Relaxed);
                    bytes_received.emit(ByteSize(chunk_bytes));

                    chunk_events
                }
            })
            .buffered(parse_concurrency_per_file)
            .flat_map(move |events| {
                let total_events = events.len();
                
                if total_events > 0 {
                    // Aggregate the estimated sizes
                    let total_bytes: usize = events
                        .iter()
                        .map(|e| e.estimated_json_encoded_size_of())
                        .sum();
                        
                    // Emit once per chunk instead of per event
                    events_received.emit(CountByteSize(total_events, total_bytes));
                }

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

        let read_error = read_error.lock().ok().and_then(|mut error| error.take());
        let parse_error = parse_error.lock().ok().and_then(|mut error| error.take());

        if read_error.is_some() || parse_error.is_some() || send_error.is_some() {
            emit!(S3ObjectProcessingFailed {
                bucket,
                duration: download_start.elapsed()
            });
        } else {
            emit!(S3ObjectProcessingSucceeded {
                bucket,
                duration: download_start.elapsed()
            });
        }

        let uncompressed_total = uncompressed_tracker.load(Ordering::Relaxed) as i64;

        let compression_ratio = if compressed_size > 0 {
            (uncompressed_total as f64) / (compressed_size as f64)
        } else {
            0.0
        };

        if let Some(error) = read_error {
            Err(ProcessingError::ReadObject {
                source: error,
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            })
        } else if let Some(error) = parse_error {
            Err(ProcessingError::ParseTaskJoin {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                error,
            })
        } else if let Some(error) = send_error {
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
                        compressed_size_bytes = compressed_size,
                        uncompressed_size_bytes = uncompressed_total,
                        compression_ratio = format!("{:.2}:1", compression_ratio),
                        "Finished processing of S3 file successfully. No acknowledgements."
                    );

                    Ok(())
                }
                Some(receiver) => match receiver.await {
                    BatchStatus::Delivered => {
                        tracing::info!(
                            %bucket,
                            %key,
                            compressed_size_bytes = compressed_size,
                            uncompressed_size_bytes = uncompressed_total,
                            compression_ratio = format!("{:.2}:1", compression_ratio),
                            "Finished processing of S3 file successfully. Delivered."
                        );

                        Ok(())
                    }
                    BatchStatus::Errored => Err(ProcessingError::ErrorAcknowledgement {
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                        region: region.to_owned(),
                    }),
                    BatchStatus::Rejected => {
                        if self.state.delete_failed_message {
                            Ok(())
                        } else {
                            Err(ProcessingError::ErrorAcknowledgement {
                                bucket: bucket.to_owned(),
                                key: key.to_owned(),
                                region: region.to_owned(),
                            })
                        }
                    }
                },
            }
        }
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
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
    }
}

fn populate_template_metadata(
    template: &mut LogEvent,
    log_namespace: LogNamespace,
    bucket: &str,
    key: &str,
    region: &str,
    metadata: &Option<HashMap<String, String>>,
    timestamp: Option<DateTime<Utc>>,
) {
    let bucket_bytes = Bytes::copy_from_slice(bucket.as_bytes());
    let key_bytes = Bytes::copy_from_slice(key.as_bytes());
    let region_bytes = Bytes::copy_from_slice(region.as_bytes());

    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        template,
        Some(LegacyKey::Overwrite(path!("bucket"))),
        path!("bucket"),
        bucket_bytes,
    );

    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        template,
        Some(LegacyKey::Overwrite(path!("object"))),
        path!("object"),
        key_bytes,
    );

    log_namespace.insert_source_metadata(
        AwsS3Config::NAME,
        template,
        Some(LegacyKey::Overwrite(path!("region"))),
        path!("region"),
        region_bytes,
    );

    if let Some(metadata) = metadata {
        for (k, v) in metadata {
            log_namespace.insert_source_metadata(
                AwsS3Config::NAME,
                template,
                Some(LegacyKey::Overwrite(path!(k))),
                path!("metadata", k.as_str()),
                Bytes::copy_from_slice(v.as_bytes()),
            );
        }
    }

    log_namespace.insert_vector_metadata(
        template,
        log_schema().source_type_key(),
        path!("source_type"),
        Bytes::from_static(AwsS3Config::NAME.as_bytes()),
    );

    match log_namespace {
        LogNamespace::Vector => {
            if let Some(timestamp) = timestamp {
                template.insert(metadata_path!(AwsS3Config::NAME, "timestamp"), timestamp);
            }

            template.insert(metadata_path!("vector", "ingest_timestamp"), Utc::now());
        }
        LogNamespace::Legacy => {
            if let Some(timestamp_key) = log_schema().timestamp_key() {
                template.try_insert(
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
    pub cid: Option<String>,
    pub timestamp: Option<i64>,
    pub file_count: Option<usize>,
    pub total_size: Option<u64>,
    pub bucket: String,
    pub path_prefix: String,
    pub files: Vec<CrowdStrikeFdrFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrowdStrikeFdrFile {
    pub path: String,
    pub size: i64,
    pub checksum: Option<String>,
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

            decoded.map_err(|err| {
                D::Error::custom(format!("error url decoding S3 object key: {err}"))
            })
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
                "Service": "Amazon S3",
                "Event": "s3:TestEvent",
                "Time": "2014-10-13T15:57:02.089Z",
                "Bucket": "bucketname",
                "RequestId": "5582815E1AEA5ADF",
                "HostId": "8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"
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
                "Type": "Notification",
                "MessageId": "63a3f6b6-d533-4a47-aef9-fcf5cf758c76",
                "TopicArn": "arn:aws:sns:us-west-2:123456789012:MyTopic",
                "Subject": "Testing publish to subscribed queues",
                "Message": "{\"Bucket\":\"bucketname\",\"Event\":\"s3:TestEvent\",\"HostId\":\"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE\",\"RequestId\":\"5582815E1AEA5ADF\",\"Service\":\"Amazon S3\",\"Time\":\"2014-10-13T15:57:02.089Z\"}",
                "Timestamp": "2012-03-29T05:12:16.901Z",
                "SignatureVersion": "1",
                "Signature": "EXAMPLEnTrFPa3...",
                "SigningCertURL": "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem",
                "UnsubscribeURL": "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:123456789012:MyTopic:c7fe3a54-ab0e-4ec2-88e0-db410a0f2bee"
            }"#,
        )
        .unwrap();

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