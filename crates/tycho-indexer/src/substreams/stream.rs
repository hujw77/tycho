use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Error};
use async_stream::try_stream;
use futures03::{Stream, StreamExt};
use metrics::{counter, gauge};
use once_cell::sync::Lazy;
use prost::Message as ProstMessage;
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{error, info, trace, warn};

use crate::{
    pb::sf::substreams::{
        rpc::{
            v2::{response::Message, BlockScopedData, BlockUndoSignal, Response},
            v3::Request,
        },
        v1::Package,
    },
    substreams::SubstreamsEndpoint,
};

#[allow(clippy::large_enum_variant)]
pub enum BlockResponse {
    New(BlockScopedData),
    Undo(BlockUndoSignal),
    Ended,
}

pub struct SubstreamsStream {
    stream: Pin<Box<dyn Stream<Item = Result<BlockResponse, Error>> + Send>>,
}

impl SubstreamsStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: Arc<SubstreamsEndpoint>,
        cursor: Option<String>,
        package: Option<Package>,
        output_module_name: String,
        start_block: i64,
        end_block: u64,
        final_blocks_only: bool,
        extractor_id: String,
        partial_blocks: bool,
        params: HashMap<String, String>,
    ) -> Self {
        SubstreamsStream {
            stream: Box::pin(stream_blocks(
                endpoint,
                cursor,
                package,
                output_module_name,
                start_block,
                end_block,
                final_blocks_only,
                extractor_id,
                partial_blocks,
                params,
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_stream(
        stream: Pin<Box<dyn Stream<Item = Result<BlockResponse, Error>> + Send>>,
    ) -> Self {
        Self { stream }
    }
}

static DEFAULT_BACKOFF: Lazy<ExponentialBackoff> =
    Lazy::new(|| ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(45)));

async fn wait_for_next_retry(
    backoff: &mut ExponentialBackoff,
    retry_count: &mut u32,
    extractor_id: &str,
) -> Result<(), Error> {
    if let Some(duration) = backoff.next() {
        info!("Will try to reconnect after {:?}", duration);
        sleep(duration).await;
        *retry_count += 1;
        Ok(())
    } else {
        counter!("substreams_failure", "extractor" => extractor_id.to_string(), "cause" => "max_retries_exceeded").increment(1);
        Err(anyhow!("Backoff requested to stop retrying, quitting"))
    }
}

// Create the Stream implementation that streams blocks with auto-reconnection.
//
// On the first connection, `cursor` is empty (fresh start) and `start_block_num`
// determines where Substreams begins (inclusive). After the first block arrives,
// `latest_cursor` is populated from the response. On any subsequent reconnection
// (hot reconnect within the same process), `latest_cursor` is sent as
// `start_cursor` which takes precedence over `start_block_num`.
#[allow(clippy::too_many_arguments)]
fn stream_blocks(
    endpoint: Arc<SubstreamsEndpoint>,
    cursor: Option<String>,
    package: Option<Package>,
    output_module_name: String,
    start_block_num: i64,
    stop_block_num: u64,
    final_blocks_only: bool,
    extractor_id: String,
    partial_blocks: bool,
    params: HashMap<String, String>,
) -> impl Stream<Item = Result<BlockResponse, Error>> {
    let mut latest_cursor = cursor.unwrap_or_default();
    let mut latest_block = start_block_num as u64;
    let mut retry_count = 0;
    let mut backoff = DEFAULT_BACKOFF.clone();

    try_stream! {
        'retry_loop: loop {
            if retry_count > 0 {
                warn!("Blockstreams disconnected, connecting again");
            }

            let result = endpoint.clone().substreams(Request {
                start_block_num,
                start_cursor: latest_cursor.clone(),
                stop_block_num,
                final_blocks_only,
                package: package.clone(),
                params: params.clone(),
                network: String::new(), // TODO: check if we need to set the network?
                output_module: output_module_name.clone(),
                // There is usually no good reason for you to consume the stream development mode (so switching `true`
                // to `false`). If you do switch it, be aware that more than one output module will be send back to you,
                // and the current code in `process_block_scoped_data` (within your 'main.rs' file) expects a single
                // module.
                production_mode: true,
                debug_initial_store_snapshot_for_modules: vec![],
                dev_output_modules: vec![],
                limit_processed_blocks: u64::MAX,
                progress_messages_interval_ms: 30 * 1000,
                partial_blocks,
                noop_mode: false,
            }).await;

            match result {
                Ok(stream) => {
                    for await response in stream {
                        match process_substreams_response(response).await {
                            BlockProcessedResult::BlockScopedData(block_scoped_data) => {
                                if let Some(block) = block_scoped_data.clock.clone() {
                                    // Only measure lag if the msg is a full block or the last partial
                                    // TODO: substreams is looking to update the partial block service to be faster than the final block confirmation.
                                    // This means .is_last_partial will be unset for the last partial. We'd need to update this logic when that happens
                                    // to monitor for the first partial of the next block as the indicator that the previous block is complete.
                                    if !block_scoped_data.is_partial || block_scoped_data.is_last_partial.is_some_and(|last_partial| last_partial) {
                                        if let Some(block_ts) = block.timestamp {
                                            let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards!?").as_millis();
                                            let lag = now.saturating_sub((block_ts.seconds * 1000) as u128);
                                            gauge!("substreams_lag_millis", "extractor" => extractor_id.clone()).set(lag as f64);
                                        }
                                    }
                                    latest_block = block.number;
                                };

                                gauge!("block_message_size_bytes", "extractor" => extractor_id.clone()).set(block_scoped_data.encoded_len() as f64);

                                // Reset backoff because we got a good value from the stream
                                backoff = DEFAULT_BACKOFF.clone();

                                let cursor = block_scoped_data.cursor.clone();
                                yield BlockResponse::New(block_scoped_data);

                                latest_cursor = cursor;
                            },
                            BlockProcessedResult::BlockUndoSignal(block_undo_signal) => {
                                // Reset backoff because we got a good value from the stream
                                backoff = DEFAULT_BACKOFF.clone();

                                let to_block = block_undo_signal.last_valid_block.clone().unwrap_or_default().number;
                                counter!(
                                    "chain_reorg",
                                    "extractor" => extractor_id.clone(),
                                    "to_block" => to_block.to_string(),
                                    "from_block" => latest_block.to_string()
                                )
                                .increment(1);

                                let cursor = block_undo_signal.last_valid_cursor.clone();
                                yield BlockResponse::Undo(block_undo_signal);

                                latest_cursor = cursor;
                            },
                            BlockProcessedResult::Skip() => {},
                            BlockProcessedResult::TonicError(status) => {
                                // Unauthenticated errors are not retried, we forward the error back to the
                                // stream consumer which handles it
                                if status.code() == tonic::Code::Unauthenticated {
                                    counter!("substreams_failure", "extractor" => extractor_id.clone(), "cause" => "unauthenticated").increment(1);
                                    return Err(anyhow::Error::new(status.clone()))?;
                                }

                                error!("Received tonic error {:#}", status);
                                counter!("substreams_failure", "extractor" => extractor_id.clone(), "cause" => status.code().to_string()).increment(1);

                                // If we reach this point, we must wait a bit before retrying
                                wait_for_next_retry(&mut backoff, &mut retry_count, &extractor_id).await?;
                                continue 'retry_loop;
                            },
                        }
                    }

                    info!("Stream completed, reached end block");
                    yield BlockResponse::Ended;
                    return;
                },
                Err(e) => {
                    counter!("substreams_failure", "module" => output_module_name.clone(), "cause" => "connection_error").increment(1);
                    error!("Unable to connect to endpoint: {:#}", e);

                    // If we reach this point, we must wait a bit before retrying
                    wait_for_next_retry(&mut backoff, &mut retry_count, &extractor_id).await?;
                }
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum BlockProcessedResult {
    Skip(),
    BlockScopedData(BlockScopedData),
    BlockUndoSignal(BlockUndoSignal),
    TonicError(tonic::Status),
}

async fn process_substreams_response(
    result: Result<Response, tonic::Status>,
) -> BlockProcessedResult {
    let response = match result {
        Ok(v) => v,
        Err(e) => return BlockProcessedResult::TonicError(e),
    };

    match response.message {
        Some(Message::Session(session)) => {
            tracing::Span::current().record("sf_trace_id", &session.trace_id);
            info!(
                ?session.resolved_start_block,
                ?session.linear_handoff_block,
                ?session.max_parallel_workers,
                ?session.trace_id,
                "SubstreamSessionInit"
            );
            BlockProcessedResult::Skip()
        }
        Some(Message::BlockScopedData(block_scoped_data)) => {
            BlockProcessedResult::BlockScopedData(block_scoped_data)
        }
        Some(Message::BlockUndoSignal(block_undo_signal)) => {
            BlockProcessedResult::BlockUndoSignal(block_undo_signal)
        }
        Some(Message::Progress(progress)) => {
            trace!("Progress {:?}", progress);

            BlockProcessedResult::Skip()
        }
        None => {
            warn!("Got None on substream message");
            BlockProcessedResult::Skip()
        }
        _ => BlockProcessedResult::Skip(),
    }
}

impl Stream for SubstreamsStream {
    type Item = Result<BlockResponse, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use futures03::StreamExt;

    use super::{stream_blocks, BlockResponse};
    use crate::{
        pb::sf::substreams::rpc::v2::{
            response::Message, BlockScopedData, Response, SessionInit,
        },
        substreams::{
            mock::{start_scripted_mock_substreams, MockSubstreamsScript},
            SubstreamsEndpoint,
        },
    };

    fn session_response() -> Response {
        Response {
            message: Some(Message::Session(SessionInit {
                trace_id: "trace-1".to_string(),
                resolved_start_block: 42,
                linear_handoff_block: 42,
                max_parallel_workers: 1,
                attestation_public_key: String::new(),
                chain_head: 42,
                blocks_to_process_before_start_block: 0,
                effective_blocks_to_process_before_start_block: 0,
                blocks_to_process_after_start_block: 0,
                effective_blocks_to_process_after_start_block: 0,
            })),
        }
    }

    fn block_response(number: u64, cursor: &str) -> Response {
        Response {
            message: Some(Message::BlockScopedData(BlockScopedData {
                output: None,
                clock: Some(crate::pb::sf::substreams::v1::Clock {
                    id: number.to_string(),
                    number,
                    timestamp: None,
                }),
                cursor: cursor.to_string(),
                final_block_height: number,
                debug_map_outputs: vec![],
                debug_store_outputs: vec![],
                attestation: String::new(),
                is_partial: false,
                partial_index: None,
                is_last_partial: None,
            })),
        }
    }

    #[tokio::test]
    async fn reconnects_with_latest_cursor_after_stream_error() {
        let (captured, addr) = start_scripted_mock_substreams(vec![
            MockSubstreamsScript {
                responses: vec![session_response(), block_response(42, "cursor@42")],
                grpc_status: "13",
                grpc_message: Some("forced-reconnect"),
            },
            MockSubstreamsScript {
                responses: vec![session_response(), block_response(43, "cursor@43")],
                grpc_status: "0",
                grpc_message: None,
            },
        ])
        .await;

        let endpoint = Arc::new(
            SubstreamsEndpoint::new(format!("http://{addr}"), None)
                .await
                .expect("endpoint builds"),
        );
        let mut stream = Box::pin(stream_blocks(
            endpoint,
            None,
            None,
            "map_uniswap_family_protocol_changes".to_string(),
            42,
            0,
            false,
            "ethereum:uniswap_family".to_string(),
            false,
            HashMap::new(),
        ));

        let first = stream
            .next()
            .await
            .expect("first block response exists")
            .expect("first block response succeeds");
        let second = stream
            .next()
            .await
            .expect("second block response exists")
            .expect("second block response succeeds");
        let ended = stream
            .next()
            .await
            .expect("ended response exists")
            .expect("ended response succeeds");

        match first {
            BlockResponse::New(block) => assert_eq!(block.cursor, "cursor@42"),
            _ => panic!("expected first response to be a new block"),
        }
        match second {
            BlockResponse::New(block) => assert_eq!(block.cursor, "cursor@43"),
            _ => panic!("expected second response to be a new block"),
        }
        assert!(matches!(ended, BlockResponse::Ended));

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 2, "expected one initial request and one reconnect");
        assert_eq!(requests[0].start_block_num, 42);
        assert!(
            requests[0].start_cursor.is_empty(),
            "fresh shared stream should start without cursor"
        );
        assert_eq!(
            requests[1].start_cursor, "cursor@42",
            "hot reconnect should resume from latest cursor"
        );
    }
}
