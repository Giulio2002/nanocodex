use std::{collections::VecDeque, pin::Pin};

use futures_util::{Stream, StreamExt};
use reqwest::header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE};
use rmcp::{
    RoleClient,
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::Transport,
};
use sse_stream::{Error as SseError, Sse, SseStream};

type EventStream = Pin<Box<dyn Stream<Item = Result<Sse, SseError>> + Send>>;

pub(crate) struct LegacySseTransport {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    events: Option<EventStream>,
    pending: VecDeque<RxJsonRpcMessage<RoleClient>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LegacySseError {
    #[error("invalid SSE URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("SSE request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("SSE stream ended before advertising a message endpoint")]
    MissingEndpoint,
    #[error("SSE endpoint event did not contain a URL")]
    EmptyEndpoint,
    #[error("failed to encode MCP message: {0}")]
    Encode(#[from] serde_json::Error),
}

impl LegacySseTransport {
    pub(crate) async fn connect(url: &str) -> Result<Self, LegacySseError> {
        let url = reqwest::Url::parse(url)?;
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()?;
        let response = client
            .get(url.clone())
            .header(ACCEPT, "text/event-stream")
            .header(CACHE_CONTROL, "no-cache")
            .send()
            .await?
            .error_for_status()?;
        let mut events: EventStream =
            Box::pin(SseStream::from_bytes_stream(response.bytes_stream()));
        let mut pending = VecDeque::new();

        while let Some(event) = events.next().await {
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    tracing::warn!(
                        "legacy MCP SSE stream failed during endpoint discovery: {error}"
                    );
                    return Err(LegacySseError::MissingEndpoint);
                }
            };
            if event.event.as_deref() == Some("endpoint") {
                let endpoint = event
                    .data
                    .as_deref()
                    .filter(|endpoint| !endpoint.trim().is_empty())
                    .ok_or(LegacySseError::EmptyEndpoint)?;
                let endpoint = url.join(endpoint)?;
                return Ok(Self {
                    client,
                    endpoint,
                    events: Some(events),
                    pending,
                });
            }
            if let Some(message) = decode_message(&event) {
                pending.push_back(message);
            }
        }
        Err(LegacySseError::MissingEndpoint)
    }
}

impl Transport<RoleClient> for LegacySseTransport {
    type Error = LegacySseError;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        async move {
            let message = serde_json::to_vec(&item)?;
            client
                .post(endpoint)
                .header(CONTENT_TYPE, "application/json")
                .body(message)
                .send()
                .await?
                .error_for_status()?;
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        if let Some(message) = self.pending.pop_front() {
            return Some(message);
        }
        let events = self.events.as_mut()?;
        while let Some(event) = events.next().await {
            match event {
                Ok(event) => {
                    if let Some(message) = decode_message(&event) {
                        return Some(message);
                    }
                }
                Err(error) => {
                    tracing::warn!("legacy MCP SSE stream failed: {error}");
                    self.events = None;
                    return None;
                }
            }
        }
        self.events = None;
        None
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.pending.clear();
        self.events = None;
        std::future::ready(Ok(()))
    }
}

fn decode_message(event: &Sse) -> Option<RxJsonRpcMessage<RoleClient>> {
    if !matches!(event.event.as_deref(), None | Some("" | "message")) {
        return None;
    }
    let data = event.data.as_deref()?;
    match serde_json::from_str(data) {
        Ok(message) => Some(message),
        Err(error) => {
            tracing::debug!("ignoring invalid legacy MCP SSE message: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use rmcp::{
        RoleClient,
        service::{RxJsonRpcMessage, TxJsonRpcMessage},
        transport::Transport,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::LegacySseTransport;

    #[tokio::test]
    async fn discovers_relative_endpoint_and_exchanges_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.starts_with("GET /sse HTTP/1.1"));

            let incoming = r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#;
            let body =
                format!("event: endpoint\ndata: /messages?session=test\n\ndata: {incoming}\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.starts_with("POST /messages?session=test HTTP/1.1"));
            stream
                .write_all(b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            request
        });

        let mut transport = LegacySseTransport::connect(&format!("http://{address}/sse"))
            .await
            .unwrap();
        let outgoing = serde_json::from_str::<TxJsonRpcMessage<RoleClient>>(
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .unwrap();
        transport.send(outgoing).await.unwrap();
        let incoming = transport.receive().await.unwrap();
        let incoming = serde_json::to_value(incoming).unwrap();
        assert_eq!(
            incoming["method"],
            serde_json::json!("notifications/tools/list_changed")
        );

        let request = server.await.unwrap();
        assert!(request.contains(r#""method":"notifications/initialized""#));
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
            if let Some(position) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8(request).unwrap()
    }

    #[test]
    fn fixture_messages_match_the_client_role() {
        serde_json::from_str::<TxJsonRpcMessage<RoleClient>>(
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .unwrap();
        serde_json::from_str::<RxJsonRpcMessage<RoleClient>>(
            r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#,
        )
        .unwrap();
    }
}
