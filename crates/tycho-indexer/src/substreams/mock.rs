//! Generic mock for the Substreams Stream/Blocks gRPC service.
//!
//! Captures every `Request` protobuf sent by the client and returns an empty
//! stream (trailers-only `grpc-status: 0`), which makes `stream_blocks` yield
//! `BlockResponse::Ended` and the runner exit cleanly.
use std::{
    collections::VecDeque,
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    ops::Range,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::{
    body::BoxBody,
    codegen::{http, Body as HttpBody},
    server::NamedService,
};

use crate::pb::sf::substreams::rpc::{v2::Response, v3::Request};

#[derive(Clone, Debug)]
pub struct MockSubstreamsScript {
    pub responses: Vec<Response>,
    pub grpc_status: &'static str,
    pub grpc_message: Option<&'static str>,
}

impl MockSubstreamsScript {
    pub fn from_owned(
        responses: Vec<Response>,
        grpc_status: String,
        grpc_message: Option<String>,
    ) -> Self {
        Self {
            responses,
            grpc_status: Box::leak(grpc_status.into_boxed_str()),
            grpc_message: grpc_message
                .map(|message| Box::leak(message.into_boxed_str()) as &'static str),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockSubstreamsScriptFixture {
    responses_hex: Vec<String>,
    grpc_status: String,
    grpc_message: Option<String>,
}

impl TryFrom<MockSubstreamsScriptFixture> for MockSubstreamsScript {
    type Error = anyhow::Error;

    fn try_from(value: MockSubstreamsScriptFixture) -> Result<Self, Self::Error> {
        let responses = value
            .responses_hex
            .into_iter()
            .map(|hex_payload| {
                let bytes = hex::decode(&hex_payload)?;
                Ok(Response::decode(bytes.as_slice())?)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            responses,
            grpc_status: Box::leak(value.grpc_status.into_boxed_str()),
            grpc_message: value
                .grpc_message
                .map(|message| Box::leak(message.into_boxed_str()) as &'static str),
        })
    }
}

impl From<&MockSubstreamsScript> for MockSubstreamsScriptFixture {
    fn from(value: &MockSubstreamsScript) -> Self {
        Self {
            responses_hex: value
                .responses
                .iter()
                .map(|response| hex::encode(response.encode_to_vec()))
                .collect(),
            grpc_status: value.grpc_status.to_string(),
            grpc_message: value
                .grpc_message
                .map(ToString::to_string),
        }
    }
}

/// Mock gRPC server that captures Substreams `Request` messages.
///
/// Implements `tower::Service` directly — no generated server code needed.
/// Every incoming request is decoded from the gRPC wire format and pushed into
/// [`captured`]. The response is always a trailers-only OK (empty stream).
#[derive(Clone)]
pub struct MockSubstreamsServer {
    captured: Arc<Mutex<Vec<Request>>>,
    scripts: Arc<Mutex<VecDeque<MockSubstreamsScript>>>,
}

impl MockSubstreamsServer {
    fn new(
        scripts: Vec<MockSubstreamsScript>,
    ) -> (Self, Arc<Mutex<Vec<Request>>>, Arc<Mutex<VecDeque<MockSubstreamsScript>>>) {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
        (Self { captured: captured.clone(), scripts: scripts.clone() }, captured, scripts)
    }
}

impl NamedService for MockSubstreamsServer {
    const NAME: &'static str = "sf.substreams.rpc.v3.Stream";
}

impl tonic::codegen::Service<http::Request<tonic::transport::Body>> for MockSubstreamsServer {
    type Response = http::Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::transport::Body>) -> Self::Future {
        let captured = self.captured.clone();
        let scripts = self.scripts.clone();
        Box::pin(async move {
            // Collect the request body using http_body::Body::poll_data
            let mut body = req.into_body();
            let mut buf = Vec::new();
            while let Some(chunk) =
                std::future::poll_fn(|cx| Pin::new(&mut body).poll_data(cx)).await
            {
                if let Ok(data) = chunk {
                    buf.extend_from_slice(&data);
                }
            }

            // gRPC frame: 1 byte compressed flag + 4 bytes length + protobuf message
            if buf.len() > 5 {
                if let Ok(request) = Request::decode(&buf[5..]) {
                    captured.lock().unwrap().push(request);
                }
            }

            let script = scripts.lock().unwrap().pop_front();
            let response_body = ScriptedGrpcBody::new(script);

            Ok(http::Response::builder()
                .header("content-type", "application/grpc")
                .body(BoxBody::new(response_body))
                .unwrap())
        })
    }
}

struct ScriptedGrpcBody {
    chunks: VecDeque<Result<tonic::codegen::Bytes, tonic::Status>>,
    trailers: Option<http::HeaderMap>,
}

impl ScriptedGrpcBody {
    fn new(script: Option<MockSubstreamsScript>) -> Self {
        let mut trailers = http::HeaderMap::new();
        let mut chunks = VecDeque::new();

        match script {
            Some(script) => {
                for response in script.responses {
                    chunks.push_back(Ok(encode_grpc_message(&response)));
                }
                trailers.insert(
                    "grpc-status",
                    http::HeaderValue::from_str(script.grpc_status).expect("grpc status header"),
                );
                if let Some(message) = script.grpc_message {
                    trailers.insert(
                        "grpc-message",
                        http::HeaderValue::from_str(message).expect("grpc message header"),
                    );
                }
            }
            None => {
                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            }
        }

        Self { chunks, trailers: Some(trailers) }
    }
}

impl HttpBody for ScriptedGrpcBody {
    type Data = tonic::codegen::Bytes;
    type Error = tonic::Status;

    fn poll_data(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        Poll::Ready(self.chunks.pop_front())
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(self.trailers.take()))
    }
}

fn encode_grpc_message(message: &Response) -> tonic::codegen::Bytes {
    let payload = message.encode_to_vec();
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    tonic::codegen::Bytes::from(frame)
}

/// Start a mock Substreams gRPC server on an ephemeral port.
///
/// Returns the captured requests and the address the server is listening on.
pub async fn start_mock_substreams() -> (Arc<Mutex<Vec<Request>>>, SocketAddr) {
    let (server, captured, _) = MockSubstreamsServer::new(vec![]);
    let addr = serve_mock_substreams(server).await;

    (captured, addr)
}

pub async fn start_scripted_mock_substreams(
    scripts: Vec<MockSubstreamsScript>,
) -> (Arc<Mutex<Vec<Request>>>, SocketAddr) {
    let (server, captured, _) = MockSubstreamsServer::new(scripts);
    let addr = serve_mock_substreams(server).await;

    (captured, addr)
}

pub fn write_mock_substreams_fixture(
    path: impl AsRef<Path>,
    scripts: &[MockSubstreamsScript],
) -> anyhow::Result<()> {
    let fixture: Vec<_> = scripts
        .iter()
        .map(MockSubstreamsScriptFixture::from)
        .collect();
    std::fs::write(path, serde_json::to_vec_pretty(&fixture)?)?;
    Ok(())
}

pub fn read_mock_substreams_fixture(
    path: impl AsRef<Path>,
) -> anyhow::Result<Vec<MockSubstreamsScript>> {
    let fixture: Vec<MockSubstreamsScriptFixture> = serde_json::from_slice(&std::fs::read(path)?)?;
    fixture
        .into_iter()
        .map(TryInto::try_into)
        .collect::<anyhow::Result<Vec<_>>>()
}

pub fn split_mock_substreams_script(
    script: &MockSubstreamsScript,
    response_ranges: &[Range<usize>],
) -> anyhow::Result<Vec<MockSubstreamsScript>> {
    let response_len = script.responses.len();
    let mut split = Vec::with_capacity(response_ranges.len());

    for range in response_ranges {
        if range.start > range.end || range.end > response_len {
            anyhow::bail!(
                "invalid response range {}..{} for script with {response_len} responses",
                range.start,
                range.end
            );
        }

        split.push(MockSubstreamsScript {
            responses: script.responses[range.start..range.end].to_vec(),
            grpc_status: script.grpc_status,
            grpc_message: script.grpc_message,
        });
    }

    Ok(split)
}

pub fn read_and_split_mock_substreams_fixture(
    path: impl AsRef<Path>,
    script_index: usize,
    response_ranges: &[Range<usize>],
) -> anyhow::Result<Vec<MockSubstreamsScript>> {
    let scripts = read_mock_substreams_fixture(path)?;
    let script = scripts
        .get(script_index)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "fixture script index {script_index} is out of bounds for {} scripts",
                scripts.len()
            )
        })?;
    split_mock_substreams_script(script, response_ranges)
}

pub async fn start_scripted_mock_substreams_from_fixture(
    path: impl AsRef<Path>,
) -> anyhow::Result<(Arc<Mutex<Vec<Request>>>, SocketAddr)> {
    Ok(start_scripted_mock_substreams(read_mock_substreams_fixture(path)?).await)
}

async fn serve_mock_substreams(server: MockSubstreamsServer) -> SocketAddr {
    // Bind to find an available port, then release so tonic can rebind.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve(addr)
            .await
            .unwrap();
    });

    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    addr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::sf::substreams::rpc::v2::{response::Message, Response, SessionInit};

    fn temp_fixture_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "tycho-indexer-substreams-mock-{name}-{}-{}.json",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_default()
        ))
    }

    #[test]
    fn mock_substreams_fixture_roundtrip_preserves_scripts() {
        let path = temp_fixture_path("roundtrip");
        let scripts = vec![
            MockSubstreamsScript {
                responses: vec![Response {
                    message: Some(Message::Session(SessionInit {
                        trace_id: "trace-a".to_string(),
                        resolved_start_block: 123,
                        linear_handoff_block: 123,
                        max_parallel_workers: 1,
                        attestation_public_key: String::new(),
                        chain_head: 123,
                        blocks_to_process_before_start_block: 0,
                        effective_blocks_to_process_before_start_block: 0,
                        blocks_to_process_after_start_block: 0,
                        effective_blocks_to_process_after_start_block: 0,
                    })),
                }],
                grpc_status: "0",
                grpc_message: None,
            },
            MockSubstreamsScript {
                responses: vec![],
                grpc_status: "13",
                grpc_message: Some("internal error"),
            },
        ];

        write_mock_substreams_fixture(&path, &scripts).expect("write fixture");
        let loaded = read_mock_substreams_fixture(&path).expect("read fixture");

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].responses[0], scripts[0].responses[0]);
        assert_eq!(loaded[0].grpc_status, "0");
        assert_eq!(loaded[1].grpc_status, "13");
        assert_eq!(loaded[1].grpc_message, Some("internal error"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn split_mock_substreams_script_slices_response_ranges() {
        let script = MockSubstreamsScript {
            responses: vec![
                Response {
                    message: Some(Message::Session(SessionInit {
                        trace_id: "trace-a".to_string(),
                        resolved_start_block: 123,
                        linear_handoff_block: 123,
                        max_parallel_workers: 1,
                        attestation_public_key: String::new(),
                        chain_head: 123,
                        blocks_to_process_before_start_block: 0,
                        effective_blocks_to_process_before_start_block: 0,
                        blocks_to_process_after_start_block: 0,
                        effective_blocks_to_process_after_start_block: 0,
                    })),
                },
                Response { message: None },
                Response { message: None },
            ],
            grpc_status: "0",
            grpc_message: None,
        };

        let split = split_mock_substreams_script(&script, &[0..2, 2..3]).expect("split script");

        assert_eq!(split.len(), 2);
        assert_eq!(split[0].responses, script.responses[0..2].to_vec());
        assert_eq!(split[1].responses, script.responses[2..3].to_vec());
        assert_eq!(split[0].grpc_status, "0");
        assert_eq!(split[1].grpc_status, "0");
    }

    #[test]
    fn split_mock_substreams_script_rejects_out_of_bounds_ranges() {
        let script = MockSubstreamsScript {
            responses: vec![Response { message: None }],
            grpc_status: "0",
            grpc_message: None,
        };

        let err = split_mock_substreams_script(&script, &[0..2]).expect_err("range should fail");

        assert!(err
            .to_string()
            .contains("invalid response range 0..2 for script with 1 responses"));
    }

    #[test]
    fn read_and_split_mock_substreams_fixture_slices_selected_script() {
        let path = temp_fixture_path("split-fixture");
        let scripts = vec![MockSubstreamsScript {
            responses: vec![
                Response {
                    message: Some(Message::Session(SessionInit {
                        trace_id: "trace-a".to_string(),
                        resolved_start_block: 123,
                        linear_handoff_block: 123,
                        max_parallel_workers: 1,
                        attestation_public_key: String::new(),
                        chain_head: 123,
                        blocks_to_process_before_start_block: 0,
                        effective_blocks_to_process_before_start_block: 0,
                        blocks_to_process_after_start_block: 0,
                        effective_blocks_to_process_after_start_block: 0,
                    })),
                },
                Response { message: None },
                Response { message: None },
            ],
            grpc_status: "0",
            grpc_message: None,
        }];
        write_mock_substreams_fixture(&path, &scripts).expect("write fixture");

        let split = read_and_split_mock_substreams_fixture(&path, 0, &[0..2, 2..3])
            .expect("read and split fixture");

        assert_eq!(split.len(), 2);
        assert_eq!(split[0].responses, scripts[0].responses[0..2].to_vec());
        assert_eq!(split[1].responses, scripts[0].responses[2..3].to_vec());

        let _ = std::fs::remove_file(path);
    }
}
