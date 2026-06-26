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
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use prost::Message;
use tonic::{
    body::BoxBody,
    codegen::{http, Body as HttpBody},
    server::NamedService,
};

use crate::pb::sf::substreams::rpc::{
    v2::Response,
    v3::Request,
};

#[derive(Clone)]
pub struct MockSubstreamsScript {
    pub responses: Vec<Response>,
    pub grpc_status: &'static str,
    pub grpc_message: Option<&'static str>,
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
        (
            Self { captured: captured.clone(), scripts: scripts.clone() },
            captured,
            scripts,
        )
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
                trailers.insert("grpc-status", http::HeaderValue::from_static(script.grpc_status));
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
