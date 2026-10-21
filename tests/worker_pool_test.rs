//! End-to-end behaviour of the outbound Marvin socket pool.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use futures_util::StreamExt;
use sayna::worker::{JobBrief, MarvinEndpoint, ParticipantBrief, Pool, PoolLimits};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

struct MockConn {
    ws: WebSocketStream<TcpStream>,
    auth: Option<String>,
}

async fn spawn_marvin() -> (String, mpsc::UnboundedReceiver<MockConn>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("ws://{}", listener.local_addr().expect("addr"));
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut auth = None;
                let accepted = accept_hdr_async(
                    stream,
                    |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
                        auth = request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_string);
                        Ok(response)
                    },
                )
                .await;
                if let Ok(ws) = accepted {
                    let _ = tx.send(MockConn { ws, auth });
                }
            });
        }
    });

    (url, rx)
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

fn job(id: &str) -> JobBrief {
    JobBrief {
        job_id: id.to_string(),
        room_name: "call_1".to_string(),
        room_sid: "RM_1".to_string(),
        job_type: "room".to_string(),
        metadata: json!({ "direction": "outbound" }).to_string(),
        participant: Some(ParticipantBrief {
            identity: "sip_+15105551234".to_string(),
            name: "+15105551234".to_string(),
            sid: "PA_1".to_string(),
            kind: "sip".to_string(),
            attributes: Default::default(),
        }),
    }
}

async fn wait_for_idle(pool: &Pool, count: usize) {
    for _ in 0..100 {
        if pool.stats().idle >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "pool never reached {count} idle sockets: {:?}",
        pool.stats()
    );
}

#[tokio::test]
async fn prewarms_with_bearer_and_writes_job_assigned() {
    let (url, mut conns) = spawn_marvin().await;
    let pool = Pool::start(
        MarvinEndpoint {
            ws_url: url,
            token: Some("secret".to_string()),
        },
        PoolLimits::new(1, 0, 2).expect("limits"),
    )
    .expect("pool");

    let mut warm = recv_conn(&mut conns).await;
    assert_eq!(warm.auth.as_deref(), Some("Bearer secret"));

    let _io = pool
        .assign(job("AJ_1"), Duration::from_secs(2))
        .await
        .expect("lease");

    let frame = next_text(&mut warm.ws).await;
    assert_eq!(frame["type"], "job_assigned");
    assert_eq!(frame["job_id"], "AJ_1");
    assert_eq!(frame["room_name"], "call_1");
    assert_eq!(frame["room_sid"], "RM_1");
    assert_eq!(frame["participant"]["identity"], "sip_+15105551234");
    assert_eq!(frame["metadata"], r#"{"direction":"outbound"}"#);
}

#[tokio::test]
async fn dropping_the_lease_replenishes_the_pool() {
    let (url, mut conns) = spawn_marvin().await;
    let pool = Pool::start(
        MarvinEndpoint {
            ws_url: url,
            token: None,
        },
        PoolLimits::new(1, 0, 2).expect("limits"),
    )
    .expect("pool");

    let mut first = recv_conn(&mut conns).await;
    assert!(first.auth.is_none());

    let io = pool
        .assign(job("AJ_2"), Duration::from_secs(2))
        .await
        .expect("lease");
    assert_eq!(next_text(&mut first.ws).await["type"], "job_assigned");
    drop(io);

    let _replacement = recv_conn(&mut conns).await;
    wait_for_idle(&pool, 1).await;
}

#[tokio::test]
async fn assign_retries_when_the_leased_socket_died() {
    let (url, mut conns) = spawn_marvin().await;
    let pool = Pool::start(
        MarvinEndpoint {
            ws_url: url,
            token: None,
        },
        PoolLimits::new(1, 0, 2).expect("limits"),
    )
    .expect("pool");

    let first = recv_conn(&mut conns).await;
    drop(first);

    let io = pool
        .assign(job("AJ_3"), Duration::from_secs(3))
        .await
        .expect("a replacement socket should serve the job");

    let mut replacement = recv_conn(&mut conns).await;
    let frame = next_text(&mut replacement.ws).await;
    assert_eq!(frame["type"], "job_assigned");
    assert_eq!(frame["job_id"], "AJ_3");
    drop(io);
}
