//! One dispatched job: lease a Marvin socket, then run the existing session
//! router over it with the assignment's LiveKit credentials.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::info;

use crate::handlers::ws::SessionOrigin;
use crate::handlers::ws::handler::cleanup_connection_resources;
use crate::handlers::ws::messages::MessageRoute;
use crate::handlers::ws::processor::handle_incoming_message;
use crate::handlers::ws::state::ConnectionState;
use crate::state::AppState;

use super::pool::{JobBrief, ParticipantBrief, Pool};
use super::slot::{SlotCmd, SlotInput};

const ACQUIRE_DEADLINE: Duration = Duration::from_secs(10);
const PARTICIPANT_LOOKUP_ATTEMPTS: usize = 5;
const PARTICIPANT_LOOKUP_DELAY: Duration = Duration::from_millis(100);

pub struct JobOutcome {
    pub status: i32,
    pub error: String,
}

impl JobOutcome {
    pub fn success() -> Self {
        Self {
            status: livekit_protocol::JobStatus::JsSuccess as i32,
            error: String::new(),
        }
    }

    pub fn failed(error: String) -> Self {
        Self {
            status: livekit_protocol::JobStatus::JsFailed as i32,
            error,
        }
    }
}

pub async fn run(
    app: Arc<AppState>,
    pool: Pool,
    mut job: JobBrief,
    url: String,
    token: String,
    mut cancel: oneshot::Receiver<()>,
) -> JobOutcome {
    if job.participant.is_none() && job.metadata.trim().is_empty() {
        job.participant = lookup_room_participant(&app, &job.room_name).await;
    }

    let job_id = job.job_id.clone();
    let mut slot = match pool.assign(job, ACQUIRE_DEADLINE).await {
        Ok(slot) => slot,
        Err(e) => return JobOutcome::failed(format!("no Marvin socket for {job_id}: {e}")),
    };
    info!(%job_id, "Marvin socket leased for the assigned job");

    let state = Arc::new(RwLock::new(ConnectionState::with_origin(
        SessionOrigin::Agent { url, token },
    )));

    let (message_tx, mut message_rx) = mpsc::channel::<MessageRoute>(1024);
    let out = slot.out.clone();
    let forwarder = tokio::spawn(async move {
        while let Some(route) = message_rx.recv().await {
            if out.send(SlotCmd::Send(route)).is_err() {
                break;
            }
        }
    });

    let mut terminated = false;
    loop {
        tokio::select! {
            _ = &mut cancel => {
                info!(%job_id, "job terminated by LiveKit");
                terminated = true;
                break;
            }
            input = slot.input.recv() => match input {
                Some(SlotInput::Frame(frame)) => {
                    if !handle_incoming_message(*frame, &state, &message_tx, &app).await {
                        break;
                    }
                }
                Some(SlotInput::Closed { reason }) => {
                    info!(%job_id, %reason, "Marvin socket closed");
                    break;
                }
                None => break,
            },
        }
    }

    forwarder.abort();
    cleanup_connection_resources(&app, &state, false).await;

    if terminated {
        JobOutcome::failed("job terminated".to_string())
    } else {
        JobOutcome::success()
    }
}

async fn lookup_room_participant(app: &Arc<AppState>, room_name: &str) -> Option<ParticipantBrief> {
    let handler = app.livekit_room_handler.as_ref()?;
    for attempt in 0..PARTICIPANT_LOOKUP_ATTEMPTS {
        if let Ok(participants) = handler.list_participants(room_name).await
            && let Some(found) = participants.iter().find(|p| {
                livekit_protocol::participant_info::Kind::try_from(p.kind)
                    != Ok(livekit_protocol::participant_info::Kind::Agent)
            })
        {
            return Some(ParticipantBrief::from_proto(found));
        }
        if attempt + 1 < PARTICIPANT_LOOKUP_ATTEMPTS {
            tokio::time::sleep(PARTICIPANT_LOOKUP_DELAY).await;
        }
    }
    None
}
