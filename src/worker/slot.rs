//! One task per outbound Marvin socket.
//!
//! The task owns both halves of the `WebSocketStream`: the write half is only
//! touched here, so a lease never needs a second writer. While no socket is
//! bound it is idle and may only be leased.

use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use http::header::AUTHORIZATION;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, warn};

use crate::handlers::ws::IncomingMessage;
use crate::handlers::ws::messages::MessageRoute;

use super::pool::{JobBrief, MarvinEndpoint, PoolMsg};
use super::roster::{SlotEvent, SlotId};

/// Keeps pre-warmed sockets alive under intermediary idle timeouts.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

pub enum SlotCmd {
    Assign {
        job: Box<JobBrief>,
        input: mpsc::UnboundedSender<SlotInput>,
        ack: oneshot::Sender<Ack>,
    },
    Send(MessageRoute),
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    Accepted,
    Gone,
}

pub enum SlotInput {
    Frame(IncomingMessage),
    Closed { reason: String },
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketSink = SplitSink<Socket, Message>;

pub(super) async fn run(
    id: SlotId,
    endpoint: MarvinEndpoint,
    pool_tx: mpsc::UnboundedSender<PoolMsg>,
) {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    let ws = match connect(&endpoint).await {
        Ok(ws) => ws,
        Err(reason) => {
            warn!(%reason, "could not open the Marvin socket");
            report(&pool_tx, id, SlotEvent::Closed { reason });
            return;
        }
    };
    let (mut sink, mut stream) = ws.split();

    if pool_tx
        .send(PoolMsg::Slot {
            slot_id: id,
            event: SlotEvent::Connected { out: cmd_tx },
        })
        .is_err()
    {
        return;
    }

    let mut bound: Option<mpsc::UnboundedSender<SlotInput>> = None;
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await;

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None | Some(SlotCmd::Close) => {
                    let _ = sink.close().await;
                    report(&pool_tx, id, SlotEvent::Closed { reason: "closed".to_string() });
                    return;
                }
                Some(SlotCmd::Assign { job, input, ack }) => {
                    if bound.is_some() {
                        let _ = ack.send(Ack::Gone);
                        continue;
                    }
                    match send_job_assigned(&mut sink, &job).await {
                        Ok(()) => {
                            let _ = ack.send(Ack::Accepted);
                            bound = Some(input);
                        }
                        Err(reason) => {
                            let _ = ack.send(Ack::Gone);
                            report(&pool_tx, id, SlotEvent::AssignFailed { reason });
                            return;
                        }
                    }
                }
                Some(SlotCmd::Send(route)) => {
                    if !send_route(&mut sink, route).await {
                        notify_closed(&bound, "send failed");
                        report(&pool_tx, id, SlotEvent::Closed { reason: "send failed".to_string() });
                        return;
                    }
                }
            },
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    let Some(input) = &bound else { continue };
                    match serde_json::from_str::<IncomingMessage>(&text) {
                        Ok(msg) => {
                            if input.send(SlotInput::Frame(msg)).is_err() {
                                break;
                            }
                        }
                        Err(e) => warn!(%e, "Marvin sent an unparseable frame"),
                    }
                }
                // Sayna only receives JSON control frames from Marvin; audio flows the other way.
                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    notify_closed(&bound, "peer closed");
                    report(&pool_tx, id, SlotEvent::Closed { reason: "peer closed".to_string() });
                    return;
                }
                Some(Err(e)) => {
                    notify_closed(&bound, &e.to_string());
                    report(&pool_tx, id, SlotEvent::Closed { reason: e.to_string() });
                    return;
                }
            },
            _ = keepalive.tick() => {
                if let Err(e) = sink.send(Message::Ping(Vec::new().into())).await {
                    notify_closed(&bound, &e.to_string());
                    report(&pool_tx, id, SlotEvent::Closed { reason: e.to_string() });
                    return;
                }
            }
        }
    }

    // The input receiver is gone; the lease ended without the socket failing.
    let _ = sink.close().await;
    report(
        &pool_tx,
        id,
        SlotEvent::Closed {
            reason: "session ended".to_string(),
        },
    );
    debug!(?id, "Marvin socket closed");
}

async fn connect(endpoint: &MarvinEndpoint) -> Result<Socket, String> {
    let mut request = endpoint
        .ws_url
        .clone()
        .into_client_request()
        .map_err(|e| format!("invalid MARVIN_WS_URL: {e}"))?;

    if let Some(token) = &endpoint.token {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| format!("invalid MARVIN_WS_TOKEN: {e}"))?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }

    connect_async(request)
        .await
        .map(|(ws, _)| ws)
        .map_err(|e| e.to_string())
}

async fn send_job_assigned(ws: &mut SocketSink, job: &JobBrief) -> Result<(), String> {
    let frame = serde_json::json!({
        "type": "job_assigned",
        "job_id": job.job_id,
        "room_name": job.room_name,
        "room_sid": job.room_sid,
        "job_type": job.job_type,
        "metadata": job.metadata,
        "participant": job.participant,
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .map_err(|e| e.to_string())
}

/// Returns false when the route closed the socket.
async fn send_route(ws: &mut SocketSink, route: MessageRoute) -> bool {
    let sent = match route {
        MessageRoute::Outgoing(message) => match serde_json::to_string(&message) {
            Ok(json) => ws.send(Message::Text(json.into())).await,
            Err(e) => {
                warn!(%e, "could not serialize an outgoing frame");
                return true;
            }
        },
        MessageRoute::Binary(data) => ws.send(Message::Binary(data)).await,
        MessageRoute::Close => {
            let _ = ws.send(Message::Close(None)).await;
            return false;
        }
    };

    match sent {
        Ok(()) => true,
        Err(e) => {
            warn!(%e, "could not write to the Marvin socket");
            false
        }
    }
}

fn notify_closed(bound: &Option<mpsc::UnboundedSender<SlotInput>>, reason: &str) {
    if let Some(input) = bound {
        let _ = input.send(SlotInput::Closed {
            reason: reason.to_string(),
        });
    }
}

fn report(pool_tx: &mpsc::UnboundedSender<PoolMsg>, slot_id: SlotId, event: SlotEvent) {
    let _ = pool_tx.send(PoolMsg::Slot { slot_id, event });
}
