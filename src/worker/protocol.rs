//! LiveKit `/agent` control connection.
//!
//! One protobuf message per binary frame; the server owns the read side and the
//! worker writes registration, availability, status and pings.

use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use http::header::AUTHORIZATION;
use livekit_api::access_token::{AccessToken, VideoGrants};
use livekit_protocol::{
    JobType, RegisterWorkerRequest, RegisterWorkerResponse, ServerMessage, WorkerMessage,
    server_message, worker_message,
};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use super::WorkerConfig;
use super::WorkerError;

const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type AgentSink = SplitSink<Socket, Message>;
pub type AgentStream = SplitStream<Socket>;

pub struct AgentLink {
    sink: AgentSink,
    stream: AgentStream,
}

impl AgentLink {
    pub async fn connect(config: &WorkerConfig) -> Result<Self, WorkerError> {
        let token = AccessToken::with_api_key(&config.api_key, &config.api_secret)
            .with_grants(VideoGrants {
                agent: true,
                ..Default::default()
            })
            .to_jwt()
            .map_err(|e| WorkerError::Protocol(format!("agent token: {e}")))?;

        let url = format!("{}/agent", config.livekit_url.trim_end_matches('/'));
        let mut request = url
            .into_client_request()
            .map_err(|e| WorkerError::Protocol(format!("invalid LiveKit URL: {e}")))?;
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| WorkerError::Protocol(format!("agent token header: {e}")))?;
        request.headers_mut().insert(AUTHORIZATION, value);

        let (ws, _) = connect_async(request)
            .await
            .map_err(|e| WorkerError::Transport(e.to_string()))?;
        let (sink, stream) = ws.split();
        Ok(Self { sink, stream })
    }

    /// Register and wait for the server's acknowledgement. Anything the server
    /// sends before that (a stale pong, for example) is ignored.
    pub async fn register(
        &mut self,
        request: RegisterWorkerRequest,
    ) -> Result<RegisterWorkerResponse, WorkerError> {
        write_worker(
            &mut self.sink,
            WorkerMessage {
                message: Some(worker_message::Message::Register(request)),
            },
        )
        .await?;

        let deadline = tokio::time::Instant::now() + REGISTER_TIMEOUT;
        loop {
            let message = tokio::time::timeout_at(deadline, read_server(&mut self.stream))
                .await
                .map_err(|_| {
                    WorkerError::Protocol("worker registration timed out".to_string())
                })??;

            match message.message {
                Some(server_message::Message::Register(response)) => return Ok(response),
                Some(server_message::Message::GoAway(_)) | None => return Err(WorkerError::Closed),
                _ => {}
            }
        }
    }

    pub fn into_parts(self) -> (AgentSink, AgentStream) {
        (self.sink, self.stream)
    }
}

pub fn registration(config: &WorkerConfig) -> RegisterWorkerRequest {
    RegisterWorkerRequest {
        r#type: JobType::JtRoom as i32,
        agent_name: config.agent_name.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        ping_interval: 10,
        instance_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    }
}

pub async fn write_worker(sink: &mut AgentSink, message: WorkerMessage) -> Result<(), WorkerError> {
    let bytes = message.encode_to_vec();
    sink.send(Message::Binary(bytes.into()))
        .await
        .map_err(|e| WorkerError::Transport(e.to_string()))
}

pub async fn read_server(stream: &mut AgentStream) -> Result<ServerMessage, WorkerError> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Binary(bytes))) => {
                return ServerMessage::decode(&bytes[..]).map_err(|e| {
                    WorkerError::Protocol(format!("could not decode an /agent frame: {e}"))
                });
            }
            Some(Ok(Message::Close(_))) | None => return Err(WorkerError::Closed),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(WorkerError::Transport(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_defaults_to_room_jobs() {
        let config = WorkerConfig {
            livekit_url: "ws://127.0.0.1:7880".to_string(),
            api_key: "key".to_string(),
            api_secret: "secret".to_string(),
            agent_name: "sayna-worker".to_string(),
            marvin: super::super::MarvinEndpoint {
                ws_url: "ws://127.0.0.1:4000/voice/ws".to_string(),
                token: None,
            },
            limits: super::super::PoolLimits::new(1, 1, 2).expect("limits"),
        };

        let request = registration(&config);
        assert_eq!(request.r#type, JobType::JtRoom as i32);
        assert_eq!(request.agent_name, "sayna-worker");
        assert_eq!(request.ping_interval, 10);
        assert!(!request.instance_id.is_empty());
    }
}
