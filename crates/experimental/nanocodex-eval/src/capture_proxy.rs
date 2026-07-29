use std::{
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{
        FromRequestParts as _, Request, State,
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
    },
    http::{
        HeaderMap, HeaderName, Method, StatusCode, Uri,
        header::{CONNECTION, CONTENT_LENGTH, HOST, SEC_WEBSOCKET_ACCEPT, UPGRADE},
    },
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use serde_json::{Value, value::RawValue};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt as _,
    net::TcpListener,
    sync::{Mutex, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message as TungsteniteMessage,
        client::IntoClientRequest as _,
        protocol::frame::{CloseFrame as TungsteniteCloseFrame, coding::CloseCode},
    },
};

const CAPTURE_SCHEMA_VERSION: u32 = 1;
const MAX_HTTP_REQUEST_BYTES: usize = 256 * 1024 * 1024;

/// Configuration for the eval-owned host Responses payload capture proxy.
#[doc(hidden)]
pub struct ResponsesCaptureProxyConfig {
    /// Real OpenAI or ChatGPT Codex API base URL.
    pub upstream: String,
    /// Flushed JSONL payload capture written by the proxy.
    pub output: PathBuf,
    /// Optional evaluator-owned selector overrides applied to one `/models`
    /// entry before the catalog is delivered to the client.
    pub model_catalog_override: Option<ResponsesModelCatalogOverride>,
}

/// Explicit model-catalog selector overrides for a controlled evaluation.
///
/// The proxy retains both the exact upstream catalog and the exact catalog
/// delivered to the client whenever this override is applied.
#[doc(hidden)]
#[derive(Clone)]
pub struct ResponsesModelCatalogOverride {
    /// Model slug whose runtime tool-mode selector is pinned.
    pub model: String,
    /// Tool-mode selector delivered to the client.
    pub tool_mode: String,
    /// Multi-agent selector delivered to the client.
    pub multi_agent_version: Option<String>,
}

impl ResponsesModelCatalogOverride {
    /// Pins one model to an explicit Codex tool-mode selector.
    #[must_use]
    pub fn tool_mode(model: impl Into<String>, tool_mode: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            tool_mode: tool_mode.into(),
            multi_agent_version: Some("disabled".to_owned()),
        }
    }
}

/// Failure while running the host Responses payload capture proxy.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum ResponsesCaptureProxyError {
    /// The proxy may only listen on host loopback.
    #[error("Responses capture proxy must listen on loopback, not {0}")]
    NonLoopback(SocketAddr),

    /// The upstream base URL must use HTTP or HTTPS.
    #[error("Responses capture proxy upstream must use http:// or https://: {0}")]
    InvalidUpstream(String),

    /// Listener, capture file, or readiness I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The outgoing HTTP client could not be constructed.
    #[error("failed to build Responses capture HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),

    /// The HTTP server failed.
    #[error("Responses capture HTTP server failed: {0}")]
    Serve(#[source] std::io::Error),

    /// The owned server task failed.
    #[error("Responses capture proxy task failed: {0}")]
    Task(#[source] tokio::task::JoinError),
}

/// One running host-side Responses capture proxy.
#[doc(hidden)]
pub struct ResponsesCaptureProxy {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), ResponsesCaptureProxyError>>>,
}

#[derive(Clone)]
struct ProxyState {
    upstream: Arc<str>,
    http: reqwest::Client,
    recorder: Arc<Mutex<CaptureRecorder>>,
    model_catalog_override: Option<ResponsesModelCatalogOverride>,
}

struct CaptureRecorder {
    output: File,
    started: Instant,
    sequence: u64,
    request_index: u32,
}

#[derive(Clone)]
struct RequestIdentity {
    index: u32,
    phase: String,
}

#[derive(Serialize)]
struct CaptureRecord<'a> {
    schema_version: u32,
    sequence: u64,
    observed_unix_ns: u64,
    elapsed_ns: u64,
    direction: &'static str,
    transport: &'static str,
    request_index: u32,
    phase: &'a str,
    kind: &'static str,
    payload_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    payload: CapturedPayload<'a>,
}

#[derive(Serialize)]
#[serde(tag = "encoding", rename_all = "snake_case")]
enum CapturedPayload<'a> {
    Json { event: &'a RawValue },
    Utf8 { text: &'a str },
    Base64 { data: String },
}

struct CaptureFields<'a> {
    direction: &'static str,
    transport: &'static str,
    request: &'a RequestIdentity,
    kind: &'static str,
    method: Option<&'a str>,
    path: Option<&'a str>,
    status: Option<u16>,
}

impl CaptureRecorder {
    async fn create(path: &Path) -> Result<Self, std::io::Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let output = File::create(path).await?;
        Ok(Self {
            output,
            started: Instant::now(),
            sequence: 0,
            request_index: 0,
        })
    }

    async fn outbound(
        &mut self,
        transport: &'static str,
        kind: &'static str,
        method: Option<&str>,
        path: Option<&str>,
        payload: &[u8],
    ) -> Result<RequestIdentity, std::io::Error> {
        self.request_index = self.request_index.saturating_add(1);
        let request = RequestIdentity {
            index: self.request_index,
            phase: request_phase(payload),
        };
        self.write(
            CaptureFields {
                direction: "outbound",
                transport,
                request: &request,
                kind,
                method,
                path,
                status: None,
            },
            payload,
        )
        .await?;
        Ok(request)
    }

    async fn inbound(
        &mut self,
        transport: &'static str,
        kind: &'static str,
        request: &RequestIdentity,
        status: Option<u16>,
        payload: &[u8],
    ) -> Result<(), std::io::Error> {
        self.write(
            CaptureFields {
                direction: "inbound",
                transport,
                request,
                kind,
                method: None,
                path: None,
                status,
            },
            payload,
        )
        .await
    }

    async fn write(
        &mut self,
        fields: CaptureFields<'_>,
        payload: &[u8],
    ) -> Result<(), std::io::Error> {
        self.sequence = self.sequence.saturating_add(1);
        let elapsed_ns = duration_ns(self.started.elapsed());
        let observed_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, duration_ns);
        let raw = std::str::from_utf8(payload)
            .ok()
            .and_then(|text| RawValue::from_string(text.to_owned()).ok());
        let utf8 = raw
            .is_none()
            .then(|| std::str::from_utf8(payload).ok())
            .flatten();
        let encoded;
        let captured = if let Some(raw) = raw.as_deref() {
            CapturedPayload::Json { event: raw }
        } else if let Some(text) = utf8 {
            CapturedPayload::Utf8 { text }
        } else {
            encoded = STANDARD.encode(payload);
            CapturedPayload::Base64 { data: encoded }
        };
        let record = CaptureRecord {
            schema_version: CAPTURE_SCHEMA_VERSION,
            sequence: self.sequence,
            observed_unix_ns,
            elapsed_ns,
            direction: fields.direction,
            transport: fields.transport,
            request_index: fields.request.index,
            phase: &fields.request.phase,
            kind: fields.kind,
            payload_bytes: payload.len(),
            method: fields.method,
            path: fields.path,
            status: fields.status,
            payload: captured,
        };
        let mut line = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        line.push(b'\n');
        self.output.write_all(&line).await?;
        self.output.flush().await
    }
}

/// Starts a loopback reverse proxy on an already-bound host listener.
///
/// Request and response headers are forwarded but deliberately stay outside
/// the payload artifact, matching Nanocodex's existing `api.event` boundary and
/// avoiding a second retained copy of bearer credentials.
impl ResponsesCaptureProxy {
    /// Starts the proxy without reopening or changing the caller-reserved port.
    pub async fn start(
        listener: StdTcpListener,
        config: ResponsesCaptureProxyConfig,
    ) -> Result<Self, ResponsesCaptureProxyError> {
        let local_addr = listener.local_addr()?;
        if !local_addr.ip().is_loopback() {
            return Err(ResponsesCaptureProxyError::NonLoopback(local_addr));
        }
        if !config.upstream.starts_with("https://") && !config.upstream.starts_with("http://") {
            return Err(ResponsesCaptureProxyError::InvalidUpstream(config.upstream));
        }
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let recorder = CaptureRecorder::create(&config.output).await?;
        let state = ProxyState {
            upstream: Arc::from(config.upstream.trim_end_matches('/')),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(ResponsesCaptureProxyError::HttpClient)?,
            recorder: Arc::new(Mutex::new(recorder)),
            model_catalog_override: config.model_catalog_override,
        };
        let app = Router::new().fallback(proxy_request).with_state(state);
        let (shutdown, shutdown_request) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_request.await;
                })
                .await
                .map_err(ResponsesCaptureProxyError::Serve)
        });
        Ok(Self {
            local_addr,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    /// Returns the host loopback endpoint assigned to this proxy.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Requests graceful shutdown and waits for the server task to finish.
    pub async fn shutdown(mut self) -> Result<(), ResponsesCaptureProxyError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        match task.await {
            Ok(result) => result,
            Err(error) => Err(ResponsesCaptureProxyError::Task(error)),
        }
    }
}

impl Drop for ResponsesCaptureProxy {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn proxy_request(State(state): State<ProxyState>, request: Request) -> Response {
    let (mut parts, body) = request.into_parts();
    let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &state)
        .await
        .ok();
    let method = parts.method;
    let uri = parts.uri;
    let headers = parts.headers;
    let result = if let Some(websocket) = websocket {
        proxy_websocket(state, websocket, uri, headers).await
    } else {
        proxy_http(state, method, uri, headers, body).await
    };
    result.unwrap_or_else(proxy_error_response)
}

async fn proxy_websocket(
    state: ProxyState,
    websocket: WebSocketUpgrade,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, String> {
    let upstream = upstream_url(&state.upstream, &uri, true);
    let mut request = upstream
        .into_client_request()
        .map_err(|error| format!("failed to construct upstream WebSocket request: {error}"))?;
    copy_request_headers(&headers, request.headers_mut(), true);
    let (upstream, upstream_response) = connect_async(request)
        .await
        .map_err(|error| format!("upstream WebSocket connection failed: {error}"))?;
    let response_headers = upstream_response.headers().clone();
    let recorder = Arc::clone(&state.recorder);
    let path = uri.to_string();
    let mut response = websocket.on_upgrade(move |client| async move {
        let _ = bridge_websocket(client, upstream, recorder, path).await;
    });
    copy_websocket_response_headers(&response_headers, response.headers_mut());
    Ok(response)
}

async fn bridge_websocket(
    mut client: WebSocket,
    mut upstream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    recorder: Arc<Mutex<CaptureRecorder>>,
    path: String,
) -> Result<(), String> {
    let mut request = None;
    loop {
        tokio::select! {
            local = client.recv() => {
                let Some(local) = local else {
                    break;
                };
                let local = local.map_err(|error| format!("local WebSocket receive failed: {error}"))?;
                match local {
                    AxumMessage::Text(text) => {
                        let identity = recorder
                            .lock()
                            .await
                            .outbound(
                                "responses_websocket",
                                "message",
                                None,
                                Some(&path),
                                text.as_bytes(),
                            )
                            .await
                            .map_err(|error| format!("failed to record WebSocket request: {error}"))?;
                        request = Some(identity);
                        upstream
                            .send(TungsteniteMessage::Text(text.to_string().into()))
                            .await
                            .map_err(|error| format!("upstream WebSocket send failed: {error}"))?;
                    }
                    AxumMessage::Binary(data) => {
                        let identity = recorder
                            .lock()
                            .await
                            .outbound(
                                "responses_websocket",
                                "binary_message",
                                None,
                                Some(&path),
                                &data,
                            )
                            .await
                            .map_err(|error| format!("failed to record WebSocket request: {error}"))?;
                        request = Some(identity);
                        upstream
                            .send(TungsteniteMessage::Binary(data))
                            .await
                            .map_err(|error| format!("upstream WebSocket send failed: {error}"))?;
                    }
                    AxumMessage::Ping(data) => {
                        upstream
                            .send(TungsteniteMessage::Ping(data))
                            .await
                            .map_err(|error| format!("upstream WebSocket ping failed: {error}"))?;
                    }
                    AxumMessage::Pong(data) => {
                        upstream
                            .send(TungsteniteMessage::Pong(data))
                            .await
                            .map_err(|error| format!("upstream WebSocket pong failed: {error}"))?;
                    }
                    AxumMessage::Close(frame) => {
                        let frame = frame.map(|frame| TungsteniteCloseFrame {
                            code: CloseCode::from(frame.code),
                            reason: frame.reason.to_string().into(),
                        });
                        let _ = upstream.send(TungsteniteMessage::Close(frame)).await;
                        break;
                    }
                }
            }
            remote = upstream.next() => {
                let Some(remote) = remote else {
                    break;
                };
                let remote = remote
                    .map_err(|error| format!("upstream WebSocket receive failed: {error}"))?;
                match remote {
                    TungsteniteMessage::Text(text) => {
                        if let Some(identity) = request.as_ref() {
                            recorder
                                .lock()
                                .await
                                .inbound(
                                    "responses_websocket",
                                    "message",
                                    identity,
                                    None,
                                    text.as_bytes(),
                                )
                                .await
                                .map_err(|error| format!("failed to record WebSocket event: {error}"))?;
                        }
                        client
                            .send(AxumMessage::Text(text.to_string().into()))
                            .await
                            .map_err(|error| format!("local WebSocket send failed: {error}"))?;
                    }
                    TungsteniteMessage::Binary(data) => {
                        if let Some(identity) = request.as_ref() {
                            recorder
                                .lock()
                                .await
                                .inbound(
                                    "responses_websocket",
                                    "binary_message",
                                    identity,
                                    None,
                                    &data,
                                )
                                .await
                                .map_err(|error| format!("failed to record WebSocket event: {error}"))?;
                        }
                        client
                            .send(AxumMessage::Binary(data))
                            .await
                            .map_err(|error| format!("local WebSocket send failed: {error}"))?;
                    }
                    TungsteniteMessage::Ping(data) => {
                        client
                            .send(AxumMessage::Ping(data))
                            .await
                            .map_err(|error| format!("local WebSocket ping failed: {error}"))?;
                    }
                    TungsteniteMessage::Pong(data) => {
                        client
                            .send(AxumMessage::Pong(data))
                            .await
                            .map_err(|error| format!("local WebSocket pong failed: {error}"))?;
                    }
                    TungsteniteMessage::Close(frame) => {
                        let frame = frame.map(|frame| axum::extract::ws::CloseFrame {
                            code: frame.code.into(),
                            reason: frame.reason.to_string().into(),
                        });
                        let _ = client.send(AxumMessage::Close(frame)).await;
                        break;
                    }
                    TungsteniteMessage::Frame(_) => {}
                }
            }
        }
    }
    Ok(())
}

async fn proxy_http(
    state: ProxyState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, String> {
    let model_catalog_override = (uri.path() == "/models")
        .then(|| state.model_catalog_override.clone())
        .flatten();
    let upstream = upstream_url(&state.upstream, &uri, false);
    let body = to_bytes(body, MAX_HTTP_REQUEST_BYTES)
        .await
        .map_err(|error| format!("failed to read local HTTP request: {error}"))?;
    let method_text = method.as_str().to_owned();
    let path = uri.to_string();
    let request = state
        .recorder
        .lock()
        .await
        .outbound(
            "responses_https",
            "body",
            Some(&method_text),
            Some(&path),
            &body,
        )
        .await
        .map_err(|error| format!("failed to record HTTP request: {error}"))?;
    let mut upstream_request = state.http.request(method, upstream);
    for (name, value) in &headers {
        if forward_http_header(name) {
            upstream_request = upstream_request.header(name, value);
        }
    }
    let upstream_response = upstream_request
        .body(body)
        .send()
        .await
        .map_err(|error| format!("upstream HTTP request failed: {error}"))?;
    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    state
        .recorder
        .lock()
        .await
        .inbound(
            "responses_https",
            "response_started",
            &request,
            Some(status.as_u16()),
            &[],
        )
        .await
        .map_err(|error| format!("failed to record HTTP response start: {error}"))?;
    if let Some(model_catalog_override) = model_catalog_override {
        return proxy_overridden_model_catalog(
            &state,
            upstream_response,
            response_headers,
            status,
            &request,
            &model_catalog_override,
        )
        .await;
    }
    let recorder = Arc::clone(&state.recorder);
    let response_request = request.clone();
    let chunks = upstream_response.bytes_stream().then(move |chunk| {
        let recorder = Arc::clone(&recorder);
        let request = request.clone();
        async move {
            match chunk {
                Ok(chunk) => {
                    recorder
                        .lock()
                        .await
                        .inbound("responses_https", "body_chunk", &request, None, &chunk)
                        .await
                        .map_err(std::io::Error::other)?;
                    Ok::<Bytes, std::io::Error>(chunk)
                }
                Err(error) => Err(std::io::Error::other(error)),
            }
        }
    });
    let recorder = Arc::clone(&state.recorder);
    let completed = futures_util::stream::once(async move {
        recorder
            .lock()
            .await
            .inbound(
                "responses_https",
                "response_completed",
                &response_request,
                Some(status.as_u16()),
                &[],
            )
            .await
            .map_err(std::io::Error::other)?;
        Ok::<Bytes, std::io::Error>(Bytes::new())
    });
    let stream = chunks.chain(completed);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::from_u16(status.as_u16())
        .map_err(|error| format!("invalid upstream HTTP status: {error}"))?;
    copy_http_response_headers(&response_headers, response.headers_mut());
    Ok(response)
}

async fn proxy_overridden_model_catalog(
    state: &ProxyState,
    upstream_response: reqwest::Response,
    response_headers: HeaderMap,
    status: reqwest::StatusCode,
    request: &RequestIdentity,
    model_catalog_override: &ResponsesModelCatalogOverride,
) -> Result<Response, String> {
    let upstream = upstream_response
        .bytes()
        .await
        .map_err(|error| format!("failed to read upstream model catalog: {error}"))?;
    if upstream.len() > MAX_HTTP_REQUEST_BYTES {
        return Err(format!(
            "upstream model catalog exceeded {MAX_HTTP_REQUEST_BYTES} bytes"
        ));
    }
    let delivered = override_model_catalog(&upstream, model_catalog_override)?;
    let mut recorder = state.recorder.lock().await;
    recorder
        .inbound(
            "responses_https",
            "model_catalog_upstream_body",
            request,
            Some(status.as_u16()),
            &upstream,
        )
        .await
        .map_err(|error| format!("failed to record upstream model catalog: {error}"))?;
    recorder
        .inbound(
            "responses_https",
            "body_chunk",
            request,
            Some(status.as_u16()),
            &delivered,
        )
        .await
        .map_err(|error| format!("failed to record delivered model catalog: {error}"))?;
    recorder
        .inbound(
            "responses_https",
            "response_completed",
            request,
            Some(status.as_u16()),
            &[],
        )
        .await
        .map_err(|error| format!("failed to record model catalog completion: {error}"))?;
    drop(recorder);

    let mut response = Response::new(Body::from(delivered));
    *response.status_mut() = StatusCode::from_u16(status.as_u16())
        .map_err(|error| format!("invalid upstream HTTP status: {error}"))?;
    copy_http_response_headers(&response_headers, response.headers_mut());
    Ok(response)
}

fn override_model_catalog(
    upstream: &[u8],
    model_catalog_override: &ResponsesModelCatalogOverride,
) -> Result<Vec<u8>, String> {
    let mut catalog: Value = serde_json::from_slice(upstream)
        .map_err(|error| format!("upstream /models response was not valid JSON: {error}"))?;
    let models = catalog
        .get_mut("models")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| "upstream /models response had no models array".to_owned())?;
    let model = models
        .iter_mut()
        .find(|model| {
            model.get("slug").and_then(Value::as_str) == Some(&model_catalog_override.model)
        })
        .ok_or_else(|| {
            format!(
                "upstream /models response had no `{}` entry",
                model_catalog_override.model
            )
        })?;
    let object = model.as_object_mut().ok_or_else(|| {
        format!(
            "upstream /models entry `{}` was not an object",
            model_catalog_override.model
        )
    })?;
    object.insert(
        "tool_mode".to_owned(),
        Value::String(model_catalog_override.tool_mode.clone()),
    );
    if let Some(multi_agent_version) = &model_catalog_override.multi_agent_version {
        object.insert(
            "multi_agent_version".to_owned(),
            Value::String(multi_agent_version.clone()),
        );
    }
    serde_json::to_vec(&catalog)
        .map_err(|error| format!("failed to encode overridden /models response: {error}"))
}

fn upstream_url(base: &str, uri: &Uri, websocket: bool) -> String {
    let mut url = format!("{base}{}", uri.path());
    if let Some(query) = uri.query() {
        url.push('?');
        url.push_str(query);
    }
    if websocket {
        if let Some(rest) = url.strip_prefix("https://") {
            return format!("wss://{rest}");
        }
        if let Some(rest) = url.strip_prefix("http://") {
            return format!("ws://{rest}");
        }
    }
    url
}

fn copy_request_headers(source: &HeaderMap, destination: &mut HeaderMap, websocket: bool) {
    for (name, value) in source {
        if if websocket {
            forward_websocket_header(name)
        } else {
            forward_http_header(name)
        } {
            destination.insert(name.clone(), value.clone());
        }
    }
}

fn copy_websocket_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    for (name, value) in source {
        if !matches!(
            name,
            &CONNECTION | &UPGRADE | &SEC_WEBSOCKET_ACCEPT | &CONTENT_LENGTH
        ) {
            destination.insert(name.clone(), value.clone());
        }
    }
}

fn copy_http_response_headers(source: &HeaderMap, destination: &mut HeaderMap) {
    for (name, value) in source {
        if !matches!(name, &CONNECTION | &UPGRADE | &CONTENT_LENGTH) {
            destination.append(name.clone(), value.clone());
        }
    }
}

const fn forward_websocket_header(name: &HeaderName) -> bool {
    !matches!(
        name,
        &HOST
            | &CONNECTION
            | &UPGRADE
            | &CONTENT_LENGTH
            | &axum::http::header::SEC_WEBSOCKET_KEY
            | &axum::http::header::SEC_WEBSOCKET_VERSION
            | &axum::http::header::SEC_WEBSOCKET_EXTENSIONS
    )
}

const fn forward_http_header(name: &HeaderName) -> bool {
    !matches!(name, &HOST | &CONNECTION | &UPGRADE | &CONTENT_LENGTH)
}

fn request_phase(payload: &[u8]) -> String {
    let Ok(value) = serde_json::from_slice::<Value>(payload) else {
        return "unknown".to_owned();
    };
    if value.get("generate").and_then(Value::as_bool) == Some(false) {
        "warmup".to_owned()
    } else if value.get("type").and_then(Value::as_str) == Some("response.create")
        || value.get("model").is_some()
    {
        "generation".to_owned()
    } else {
        "unknown".to_owned()
    }
}

fn proxy_error_response(error: String) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        format!("Responses capture proxy error: {error}\n"),
    )
        .into_response()
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener as StdTcpListener;

    use futures_util::{SinkExt as _, StreamExt as _};
    use serde_json::Value;
    use tempfile::tempdir;
    use tokio::{
        fs,
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

    use super::{
        ResponsesCaptureProxy, ResponsesCaptureProxyConfig, ResponsesModelCatalogOverride,
        request_phase, upstream_url,
    };

    #[test]
    fn classifies_warmup_and_generation_payloads() {
        assert_eq!(
            request_phase(br#"{"type":"response.create","generate":false}"#),
            "warmup"
        );
        assert_eq!(
            request_phase(br#"{"type":"response.create","model":"gpt-test"}"#),
            "generation"
        );
    }

    #[test]
    fn joins_http_and_websocket_upstream_paths() {
        let uri = "/responses?foo=bar".parse().unwrap();
        assert_eq!(
            upstream_url("https://chatgpt.com/backend-api/codex", &uri, false),
            "https://chatgpt.com/backend-api/codex/responses?foo=bar"
        );
        assert_eq!(
            upstream_url("https://chatgpt.com/backend-api/codex", &uri, true),
            "wss://chatgpt.com/backend-api/codex/responses?foo=bar"
        );
    }

    #[tokio::test]
    async fn forwards_and_flushes_complete_websocket_payloads() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream_listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (stream, _) = upstream_listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            let request = websocket.next().await.unwrap().unwrap();
            assert_eq!(
                request,
                Message::Text(r#"{"type":"response.create","model":"gpt-test"}"#.to_owned().into())
            );
            websocket
                .send(Message::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_test"}}"#
                        .to_owned()
                        .into(),
                ))
                .await
                .unwrap();
            websocket.close(None).await.unwrap();
        });

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_address = listener.local_addr().unwrap();
        let directory = tempdir().unwrap();
        let output = directory.path().join("api-exchanges.jsonl");
        let proxy = ResponsesCaptureProxy::start(
            listener,
            ResponsesCaptureProxyConfig {
                upstream: format!("http://{upstream_address}"),
                output: output.clone(),
                model_catalog_override: None,
            },
        )
        .await
        .unwrap();

        let (mut client, _) = connect_async(format!("ws://{proxy_address}/responses"))
            .await
            .unwrap();
        client
            .send(Message::Text(
                r#"{"type":"response.create","model":"gpt-test"}"#.to_owned().into(),
            ))
            .await
            .unwrap();
        assert_eq!(
            client.next().await.unwrap().unwrap(),
            Message::Text(
                r#"{"type":"response.completed","response":{"id":"resp_test"}}"#
                    .to_owned()
                    .into()
            )
        );
        client.close(None).await.unwrap();
        drop(client);
        upstream.await.unwrap();
        proxy.shutdown().await.unwrap();

        let records = fs::read_to_string(output).await.unwrap();
        let records = records
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["direction"], "outbound");
        assert_eq!(records[0]["payload"]["event"]["model"], "gpt-test");
        assert_eq!(records[1]["direction"], "inbound");
        assert_eq!(records[1]["payload"]["event"]["type"], "response.completed");
    }

    #[tokio::test]
    async fn marks_complete_http_responses_without_buffering_them() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream_listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /models "));
            stream
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Length: 11\r\nConnection: close\r\n\r\nmodel bytes",
                )
                .await
                .unwrap();
        });

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_address = listener.local_addr().unwrap();
        let directory = tempdir().unwrap();
        let output = directory.path().join("api-exchanges.jsonl");
        let proxy = ResponsesCaptureProxy::start(
            listener,
            ResponsesCaptureProxyConfig {
                upstream: format!("http://{upstream_address}"),
                output: output.clone(),
                model_catalog_override: None,
            },
        )
        .await
        .unwrap();

        let response = reqwest::get(format!("http://{proxy_address}/models"))
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        assert_eq!(response.text().await.unwrap(), "model bytes");
        upstream.await.unwrap();
        proxy.shutdown().await.unwrap();

        let records = fs::read_to_string(output).await.unwrap();
        let records = records
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records[0]["direction"], "outbound");
        assert_eq!(records[0]["path"], "/models");
        assert_eq!(records[1]["kind"], "response_started");
        assert_eq!(records[1]["status"], 201);
        assert_eq!(records.last().unwrap()["kind"], "response_completed");
        assert_eq!(records.last().unwrap()["status"], 201);
    }

    #[tokio::test]
    async fn pins_model_tool_mode_and_retains_upstream_and_delivered_catalogs() {
        let upstream_catalog = serde_json::json!({
            "models": [
                {"slug": "gpt-test", "tool_mode": "code_mode"},
                {"slug": "other", "tool_mode": "direct"}
            ]
        })
        .to_string();
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream_listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /models "));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                upstream_catalog.len(),
                upstream_catalog
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_address = listener.local_addr().unwrap();
        let directory = tempdir().unwrap();
        let output = directory.path().join("api-exchanges.jsonl");
        let proxy = ResponsesCaptureProxy::start(
            listener,
            ResponsesCaptureProxyConfig {
                upstream: format!("http://{upstream_address}"),
                output: output.clone(),
                model_catalog_override: Some(ResponsesModelCatalogOverride::tool_mode(
                    "gpt-test",
                    "code_mode_only",
                )),
            },
        )
        .await
        .unwrap();

        let delivered: Value = reqwest::get(format!("http://{proxy_address}/models"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(delivered["models"][0]["tool_mode"], "code_mode_only");
        assert_eq!(delivered["models"][0]["multi_agent_version"], "disabled");
        assert_eq!(delivered["models"][1]["tool_mode"], "direct");
        upstream.await.unwrap();
        proxy.shutdown().await.unwrap();

        let records = fs::read_to_string(output).await.unwrap();
        let records = records
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let upstream = records
            .iter()
            .find(|record| record["kind"] == "model_catalog_upstream_body")
            .unwrap();
        assert_eq!(
            upstream["payload"]["event"]["models"][0]["tool_mode"],
            "code_mode"
        );
        let delivered = records
            .iter()
            .find(|record| record["kind"] == "body_chunk")
            .unwrap();
        assert_eq!(
            delivered["payload"]["event"]["models"][0]["tool_mode"],
            "code_mode_only"
        );
        assert_eq!(
            delivered["payload"]["event"]["models"][0]["multi_agent_version"],
            "disabled"
        );
        assert_eq!(records.last().unwrap()["kind"], "response_completed");
    }
}
