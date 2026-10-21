//! End-to-end: register on `/agent`, answer availability, take an assignment,
//! and hand the pre-warmed Marvin socket to the normal session router.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use livekit_protocol::{
    AvailabilityRequest, Job, JobAssignment, JobStatus, JobType, RegisterWorkerResponse, Room,
    ServerMessage, WorkerMessage, server_message, worker_message,
};
use prost::Message as ProstMessage;
use sayna::worker::Worker;
use sayna::{ServerConfig, state::AppState};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{WebSocketStream, accept_async, accept_hdr_async};

struct MockConn {
    ws: WebSocketStream<TcpStream>,
}

async fn spawn_marvin(listener: TcpListener) -> mpsc::UnboundedReceiver<MockConn> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Ok(ws) = accept_async(stream).await {
                    let _ = tx.send(MockConn { ws });
                }
            });
        }
    });
    rx
}

async fn recv_conn(conns: &mut mpsc::UnboundedReceiver<MockConn>) -> MockConn {
    tokio::time::timeout(Duration::from_secs(3), conns.recv())
        .await
        .expect("a Marvin socket should connect")
        .expect("connection channel open")
}

async fn next_text(ws: &mut WebSocketStream<TcpStream>) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("frame should arrive")
            .expect("socket open")
            .expect("frame is valid");
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).expect("valid JSON");
        }
    }
}

async fn send_proto(ws: &mut WebSocketStream<TcpStream>, message: ServerMessage) {
    let bytes = message.encode_to_vec();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("agent frame");
}

async fn next_worker(ws: &mut WebSocketStream<TcpStream>) -> WorkerMessage {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("worker frame should arrive")
            .expect("agent socket open")
            .expect("agent frame is valid");
        if let Message::Binary(bytes) = frame {
            return WorkerMessage::decode(&bytes[..]).expect("worker message decodes");
        }
    }
}

fn test_config(livekit_url: String, marvin_url: String) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        livekit_url,
        livekit_public_url: "http://127.0.0.1:7880".to_string(),
        livekit_api_key: Some("test-key".to_string()),
        livekit_api_secret: Some("test-secret".to_string()),
        livekit_agent_name: None,
        marvin_ws_url: Some(marvin_url),
        marvin_ws_token: None,
        worker_pool_min: 1,
        worker_pool_spare: 0,
        worker_pool_max: 2,
        deepgram_api_key: None,
        elevenlabs_api_key: None,
        google_credentials: None,
        azure_speech_subscription_key: None,
        azure_speech_region: None,
        cartesia_api_key: None,
        recording: None,
        cache_path: None,
        cache_ttl_seconds: Some(3600),
        auth_service_url: None,
        auth_signing_key_path: None,
        auth_api_secrets: Vec::new(),
        auth_timeout_seconds: 5,
        auth_required: false,
        sip: None,
    }
}

#[tokio::test]
async fn worker_registers_assigns_and_runs_a_marvin_session() {
    let agent_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind agent");
    let agent_url = format!("ws://{}", agent_listener.local_addr().expect("addr"));
    let marvin_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind marvin");
    let marvin_url = format!("ws://{}", marvin_listener.local_addr().expect("addr"));
    let mut marvin_conns = spawn_marvin(marvin_listener).await;

    let app = AppState::new(test_config(agent_url, marvin_url)).await;
    let worker = Worker::start(app)
        .await
        .expect("worker starts")
        .expect("MARVIN_WS_URL enables the worker");
    assert_eq!(worker.agent_name(), "sayna-worker");

    let (stream, _) = tokio::time::timeout(Duration::from_secs(3), agent_listener.accept())
        .await
        .expect("agent connection")
        .expect("agent listener");
    let mut auth = None;
    #[allow(clippy::result_large_err)]
    let on_upgrade = |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
        auth = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        Ok(response)
    };
    let mut agent = accept_hdr_async(stream, on_upgrade)
        .await
        .expect("agent upgrade");
    assert!(
        auth.as_deref()
            .is_some_and(|value| value.starts_with("Bearer ")),
        "worker must authenticate the /agent upgrade"
    );

    let register = next_worker(&mut agent).await;
    let Some(worker_message::Message::Register(request)) = register.message else {
        panic!("expected a register frame");
    };
    assert_eq!(request.agent_name, "sayna-worker");
    assert_eq!(request.r#type, JobType::JtRoom as i32);

    send_proto(
        &mut agent,
        ServerMessage {
            message: Some(server_message::Message::Register(RegisterWorkerResponse {
                worker_id: "W_1".to_string(),
                ..Default::default()
            })),
        },
    )
    .await;

    // The socket is already open before the job exists.
    let mut marvin = recv_conn(&mut marvin_conns).await;

    let job = Job {
        id: "AJ_1".to_string(),
        r#type: JobType::JtRoom as i32,
        room: Some(Room {
            name: "call_1".to_string(),
            sid: "RM_1".to_string(),
            ..Default::default()
        }),
        metadata: r#"{"direction":"outbound"}"#.to_string(),
        ..Default::default()
    };

    send_proto(
        &mut agent,
        ServerMessage {
            message: Some(server_message::Message::Availability(AvailabilityRequest {
                job: Some(job.clone()),
                resuming: false,
            })),
        },
    )
    .await;

    let availability = next_worker(&mut agent).await;
    let Some(worker_message::Message::Availability(response)) = availability.message else {
        panic!("expected an availability response");
    };
    assert!(response.available);
    assert_eq!(response.job_id, "AJ_1");
    assert_eq!(response.participant_identity, "marvin-agent");
    assert_eq!(response.participant_name, "Marvin");

    send_proto(
        &mut agent,
        ServerMessage {
            message: Some(server_message::Message::Assignment(JobAssignment {
                job: Some(job),
                url: Some("ws://127.0.0.1:1".to_string()),
                token: "assignment-token".to_string(),
            })),
        },
    )
    .await;

    let assigned = next_text(&mut marvin.ws).await;
    assert_eq!(assigned["type"], "job_assigned");
    assert_eq!(assigned["job_id"], "AJ_1");
    assert_eq!(assigned["room_name"], "call_1");

    // Audio off keeps the test hermetic: no LiveKit room is joined.
    marvin
        .ws
        .send(Message::Text(r#"{"type":"config","audio":false}"#.into()))
        .await
        .expect("send config");
    let ready = next_text(&mut marvin.ws).await;
    assert_eq!(ready["type"], "ready");

    marvin
        .ws
        .send(Message::Text(r#"{"type":"clear"}"#.into()))
        .await
        .expect("send clear");
    marvin
        .ws
        .send(Message::Close(None))
        .await
        .expect("close marvin");

    let finished = loop {
        match next_worker(&mut agent).await.message {
            Some(worker_message::Message::UpdateJob(status))
                if status.status == JobStatus::JsSuccess as i32 =>
            {
                break status;
            }
            Some(_) => continue,
            None => panic!("agent connection ended before the job status"),
        }
    };
    assert_eq!(finished.job_id, "AJ_1");
}
