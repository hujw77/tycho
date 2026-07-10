//! Substreams Client
//!
//! This module contains a substreams client. Taken from the
//! Rust Sink template repo.
pub mod mock;
pub mod stream;
use std::{fmt::Display, sync::Arc, time::Duration};

use futures03::StreamExt;
use http::{uri::Scheme, Uri};
use tonic::{
    codegen::http,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
};

use crate::pb::sf::substreams::rpc::{
    v2::Response,
    v3::{stream_client::StreamClient, Request},
};
use crate::substreams::mock::MockSubstreamsScript;

#[derive(Clone, Debug)]
pub struct SubstreamsEndpoint {
    pub uri: String,
    pub token: Option<String>,
    channel: Channel,
}

impl Display for SubstreamsEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.uri.as_str(), f)
    }
}

impl SubstreamsEndpoint {
    pub async fn new<S: AsRef<str>>(url: S, token: Option<String>) -> Result<Self, anyhow::Error> {
        let uri = url
            .as_ref()
            .parse::<Uri>()
            .expect("the url should have been validated by now, so it is a valid Uri");

        let endpoint = match uri
            .scheme()
            .unwrap_or(&Scheme::HTTP)
            .as_str()
        {
            "http" => Channel::builder(uri),
            "https" => Channel::builder(uri)
                .tls_config(ClientTlsConfig::new())
                .expect("TLS config on this host is invalid"),
            _ => panic!("invalid uri scheme for firehose endpoint"),
        }
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(false) // Prevent unexpected end of file errors: https://github.com/streamingfast/substreams/issues/277#issuecomment-1690904141
        .tcp_keepalive(Some(Duration::from_secs(30)));

        let uri = endpoint.uri().to_string();
        let channel = endpoint.connect_lazy();

        Ok(SubstreamsEndpoint { uri, channel, token })
    }

    pub async fn substreams(
        self: Arc<Self>,
        request: Request,
    ) -> Result<tonic::Streaming<Response>, anyhow::Error> {
        let token_metadata: Option<MetadataValue<tonic::metadata::Ascii>> = self
            .token
            .clone()
            .map(|token| token.as_str().try_into())
            .transpose()?;

        // This is a large error defined in tonic API, so we can't avoid the lint error. Fortunately
        // we only use it when creating the stream so it's acceptable performance wise.
        #[allow(clippy::result_large_err)]
        let mut client = StreamClient::with_interceptor(
            self.channel.clone(),
            move |mut r: tonic::Request<()>| {
                if let Some(ref t) = token_metadata {
                    r.metadata_mut()
                        .insert("authorization", t.clone());
                }

                Ok(r)
            },
        )
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }

    pub async fn record(
        self: Arc<Self>,
        request: Request,
        max_responses: Option<usize>,
    ) -> Result<MockSubstreamsScript, anyhow::Error> {
        let mut stream = self.substreams(request).await?;
        let mut responses = Vec::new();

        while let Some(item) = stream.next().await {
            match item {
                Ok(response) => {
                    responses.push(response);
                    if max_responses.is_some_and(|limit| responses.len() >= limit) {
                        let recorded = responses.len();
                        return Ok(MockSubstreamsScript::from_owned(
                            responses,
                            "0".to_string(),
                            Some(format!("truncated by recorder after {recorded} responses")),
                        ));
                    }
                }
                Err(status) => {
                    return Ok(MockSubstreamsScript::from_owned(
                        responses,
                        (status.code() as i32).to_string(),
                        Some(status.message().to_string()),
                    ));
                }
            }
        }

        Ok(MockSubstreamsScript::from_owned(responses, "0".to_string(), None))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        pb::sf::substreams::rpc::v2::{response::Message, BlockScopedData, Response, SessionInit},
        substreams::{
            mock::{
                read_mock_substreams_fixture, start_scripted_mock_substreams,
                write_mock_substreams_fixture, MockSubstreamsScript,
            },
            stream::build_substreams_request,
            SubstreamsEndpoint,
        },
    };

    fn session_response(start_block: u64) -> Response {
        Response {
            message: Some(Message::Session(SessionInit {
                trace_id: format!("trace-{start_block}"),
                resolved_start_block: start_block,
                linear_handoff_block: start_block,
                max_parallel_workers: 1,
                attestation_public_key: String::new(),
                chain_head: start_block,
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

    fn temp_fixture_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tycho-indexer-recorded-substreams-{name}-{}-{}.json",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ))
    }

    #[tokio::test]
    async fn records_scripted_substreams_responses_into_fixture_format() {
        let output_module = crate::testing::family_output_module_for_tests("uniswap");
        let expected_responses = vec![
            session_response(42),
            block_response(42, "cursor@42"),
            block_response(43, "cursor@43"),
        ];
        let (captured, addr) = start_scripted_mock_substreams(vec![MockSubstreamsScript {
            responses: expected_responses.clone(),
            grpc_status: "0",
            grpc_message: None,
        }])
        .await;

        let endpoint = Arc::new(
            SubstreamsEndpoint::new(format!("http://{addr}"), None)
                .await
                .expect("endpoint builds"),
        );
        let request = build_substreams_request(
            None,
            None,
            output_module.to_string(),
            42,
            44,
            true,
            false,
            Default::default(),
        );

        let recorded = endpoint
            .record(request, None)
            .await
            .expect("record scripted stream");
        assert_eq!(recorded.grpc_status, "0");
        assert_eq!(recorded.grpc_message, None);
        assert_eq!(recorded.responses, expected_responses);

        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].start_block_num, 42);
        assert_eq!(requests[0].stop_block_num, 44);
        assert_eq!(requests[0].output_module, output_module);
        drop(requests);

        let fixture_path = temp_fixture_path("roundtrip");
        write_mock_substreams_fixture(&fixture_path, std::slice::from_ref(&recorded))
            .expect("write recorded fixture");
        let loaded = read_mock_substreams_fixture(&fixture_path).expect("read recorded fixture");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].grpc_status, "0");
        assert_eq!(loaded[0].grpc_message, None);
        assert_eq!(loaded[0].responses, expected_responses);

        let _ = std::fs::remove_file(fixture_path);
    }
}
