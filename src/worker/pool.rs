//! Outbound Marvin socket pool.
//!
//! One actor owns the [`Roster`]; callers only see [`Pool::assign`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as StdInstant};

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};
use tracing::debug;

use crate::config::ServerConfig;

use super::roster::{Roster, SlotEvent, SlotId};
use super::slot::{Ack, SlotCmd, SlotInput};

const ASSIGN_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const WARM_TIMEOUT: Duration = Duration::from_secs(10);
const REAP_INTERVAL: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolLimits {
    pub min: usize,
    pub spare: usize,
    pub max: usize,
}

impl PoolLimits {
    pub fn new(min: usize, spare: usize, max: usize) -> Result<Self, PoolError> {
        if max == 0 {
            return Err(PoolError::InvalidLimits(
                "WORKER_POOL_MAX must be at least 1".to_string(),
            ));
        }
        if min > max {
            return Err(PoolError::InvalidLimits(format!(
                "WORKER_POOL_MIN ({min}) must not exceed WORKER_POOL_MAX ({max})"
            )));
        }
        if spare > max {
            return Err(PoolError::InvalidLimits(format!(
                "WORKER_POOL_SPARE ({spare}) must not exceed WORKER_POOL_MAX ({max})"
            )));
        }
        Ok(Self { min, spare, max })
    }

    pub fn from_config(config: &ServerConfig) -> Result<Self, PoolError> {
        Self::new(
            config.worker_pool_min,
            config.worker_pool_spare,
            config.worker_pool_max,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarvinEndpoint {
    pub ws_url: String,
    pub token: Option<String>,
}

impl MarvinEndpoint {
    pub fn from_config(config: &ServerConfig) -> Result<Self, PoolError> {
        let ws_url = config.marvin_ws_url.clone().ok_or(PoolError::MissingUrl)?;
        Ok(Self {
            ws_url,
            token: config.marvin_ws_token.clone(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("MARVIN_WS_URL is not configured")]
    MissingUrl,
    #[error("invalid worker pool limits: {0}")]
    InvalidLimits(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AcquireError {
    #[error("timed out waiting for a Marvin socket")]
    Timeout,
    #[error("worker pool is closed")]
    PoolClosed,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PoolStats {
    pub idle: usize,
    pub warming: usize,
    pub leased: usize,
    pub limit: usize,
}

impl PoolStats {
    pub fn total(&self) -> usize {
        self.idle + self.warming + self.leased
    }

    pub fn has_capacity(&self) -> bool {
        self.idle + self.warming > 0 || self.total() < self.limit
    }
}

/// The projection of a LiveKit job that Marvin needs. Built once at the
/// protocol boundary; `metadata` stays opaque to Sayna.
#[derive(Debug, Clone, Serialize)]
pub struct JobBrief {
    pub job_id: String,
    pub room_name: String,
    pub room_sid: String,
    pub job_type: String,
    pub metadata: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participant: Option<ParticipantBrief>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParticipantBrief {
    pub identity: String,
    pub name: String,
    pub sid: String,
    pub kind: String,
    pub attributes: HashMap<String, String>,
}

impl ParticipantBrief {
    pub fn from_proto(p: &livekit_protocol::ParticipantInfo) -> Self {
        Self {
            identity: p.identity.clone(),
            name: p.name.clone(),
            sid: p.sid.clone(),
            kind: participant_kind(p.kind),
            attributes: p.attributes.clone(),
        }
    }
}

impl JobBrief {
    pub fn from_job(job: &livekit_protocol::Job) -> Option<JobBrief> {
        let room = job.room.as_ref()?;
        Some(JobBrief {
            job_id: job.id.clone(),
            room_name: room.name.clone(),
            room_sid: room.sid.clone(),
            job_type: match livekit_protocol::JobType::try_from(job.r#type) {
                Ok(livekit_protocol::JobType::JtPublisher) => "publisher",
                Ok(livekit_protocol::JobType::JtParticipant) => "participant",
                _ => "room",
            }
            .to_string(),
            metadata: job.metadata.clone(),
            participant: job.participant.as_ref().map(ParticipantBrief::from_proto),
        })
    }
}

fn participant_kind(kind: i32) -> String {
    use livekit_protocol::participant_info::Kind;
    match Kind::try_from(kind) {
        Ok(Kind::Sip) => "sip",
        Ok(Kind::Agent) => "agent",
        Ok(Kind::Standard) => "standard",
        Ok(Kind::Ingress) => "ingress",
        Ok(Kind::Egress) => "egress",
        _ => "unknown",
    }
    .to_string()
}

#[derive(Clone)]
pub struct Pool {
    tx: mpsc::UnboundedSender<PoolMsg>,
    shared: Arc<PoolShared>,
}

struct PoolShared {
    endpoint: MarvinEndpoint,
    limits: PoolLimits,
    snapshot: Mutex<PoolStats>,
    tx: mpsc::UnboundedSender<PoolMsg>,
}

pub(super) enum PoolMsg {
    Assign {
        job: Box<JobBrief>,
        input: mpsc::UnboundedSender<SlotInput>,
        reply: oneshot::Sender<Result<Lease, LeaseError>>,
    },
    Slot {
        slot_id: SlotId,
        event: SlotEvent,
    },
    Shutdown,
}

pub(super) struct Lease {
    slot_id: SlotId,
    out: mpsc::UnboundedSender<SlotCmd>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum LeaseError {
    /// No idle socket yet; one is warming or capacity may free up.
    Warming,
    /// The socket died between lease and `job_assigned`; take the next one.
    Stale,
}

/// A leased socket plus the one receiver allowed to read from it. Dropping this
/// closes the socket and tells the pool, so a session can never leak a lease.
pub struct SlotIo {
    pub out: mpsc::UnboundedSender<SlotCmd>,
    pub input: mpsc::UnboundedReceiver<SlotInput>,
    _guard: DropGuard,
}

struct DropGuard {
    slot_id: SlotId,
    out: mpsc::UnboundedSender<SlotCmd>,
    pool_tx: mpsc::UnboundedSender<PoolMsg>,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        let _ = self.out.send(SlotCmd::Close);
        let _ = self.pool_tx.send(PoolMsg::Slot {
            slot_id: self.slot_id,
            event: SlotEvent::Closed {
                reason: "session ended".to_string(),
            },
        });
    }
}

impl Pool {
    pub fn start(endpoint: MarvinEndpoint, limits: PoolLimits) -> Result<Pool, PoolError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(PoolShared {
            endpoint,
            limits: limits.clone(),
            snapshot: Mutex::new(PoolStats {
                limit: limits.max,
                ..Default::default()
            }),
            tx: tx.clone(),
        });

        tokio::spawn(run_actor(shared.clone(), rx));

        Ok(Pool { tx, shared })
    }

    /// Lease one idle socket with `job_assigned` already written, or wait up to
    /// `deadline` for a warming one. Sockets that die mid-lease are skipped.
    pub async fn assign(&self, job: JobBrief, deadline: Duration) -> Result<SlotIo, AcquireError> {
        let at = Instant::now() + deadline;
        loop {
            if Instant::now() >= at {
                return Err(AcquireError::Timeout);
            }

            let (input_tx, input) = mpsc::unbounded_channel();
            let (reply_tx, reply_rx) = oneshot::channel();
            self.tx
                .send(PoolMsg::Assign {
                    job: Box::new(job.clone()),
                    input: input_tx,
                    reply: reply_tx,
                })
                .map_err(|_| AcquireError::PoolClosed)?;

            match tokio::time::timeout_at(at, reply_rx).await {
                Err(_) => return Err(AcquireError::Timeout),
                Ok(Err(_)) => return Err(AcquireError::PoolClosed),
                Ok(Ok(Ok(lease))) => {
                    return Ok(SlotIo {
                        out: lease.out.clone(),
                        input,
                        _guard: DropGuard {
                            slot_id: lease.slot_id,
                            out: lease.out,
                            pool_tx: self.tx.clone(),
                        },
                    });
                }
                Ok(Ok(Err(LeaseError::Stale))) => continue,
                Ok(Ok(Err(LeaseError::Warming))) => {
                    let wake = (Instant::now() + RETRY_DELAY).min(at);
                    sleep_until(wake).await;
                }
            }
        }
    }

    pub fn stats(&self) -> PoolStats {
        self.shared
            .snapshot
            .lock()
            .map(|stats| stats.clone())
            .unwrap_or_default()
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(PoolMsg::Shutdown);
    }
}

async fn run_actor(shared: Arc<PoolShared>, mut rx: mpsc::UnboundedReceiver<PoolMsg>) {
    let mut roster = Roster::default();
    let mut next_id = 0u64;
    let mut reap = tokio::time::interval(REAP_INTERVAL);
    reap.tick().await;

    reconcile(&mut roster, &shared, &mut next_id);

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                None | Some(PoolMsg::Shutdown) => return,
                Some(PoolMsg::Slot { slot_id, event }) => roster.apply(slot_id, event),
                Some(PoolMsg::Assign { job, input, reply }) => {
                    assign(&mut roster, &shared.tx, *job, input, reply);
                }
            },
            _ = reap.tick() => {
                for expired in roster.expire(StdInstant::now(), WARM_TIMEOUT) {
                    debug!(?expired, "dropping a Marvin socket that never connected");
                }
            }
        }

        reconcile(&mut roster, &shared, &mut next_id);
    }
}

fn assign(
    roster: &mut Roster,
    pool_tx: &mpsc::UnboundedSender<PoolMsg>,
    job: JobBrief,
    input: mpsc::UnboundedSender<SlotInput>,
    reply: oneshot::Sender<Result<Lease, LeaseError>>,
) {
    let Some((slot_id, out)) = roster.lease(&job.job_id) else {
        let _ = reply.send(Err(LeaseError::Warming));
        return;
    };

    let (ack_tx, ack_rx) = oneshot::channel();
    let cmd = SlotCmd::Assign {
        job: Box::new(job),
        input,
        ack: ack_tx,
    };

    if out.send(cmd).is_err() {
        roster.apply(
            slot_id,
            SlotEvent::Closed {
                reason: "slot task gone".to_string(),
            },
        );
        let _ = reply.send(Err(LeaseError::Stale));
        return;
    }

    let pool_tx = pool_tx.clone();
    tokio::spawn(async move {
        let result = match tokio::time::timeout(ASSIGN_ACK_TIMEOUT, ack_rx).await {
            Ok(Ok(Ack::Accepted)) => Ok(Lease { slot_id, out }),
            Ok(Ok(Ack::Gone)) | Ok(Err(_)) | Err(_) => {
                let _ = out.send(SlotCmd::Close);
                let _ = pool_tx.send(PoolMsg::Slot {
                    slot_id,
                    event: SlotEvent::Closed {
                        reason: "assign ack failed".to_string(),
                    },
                });
                Err(LeaseError::Stale)
            }
        };
        let _ = reply.send(result);
    });
}

fn reconcile(roster: &mut Roster, shared: &PoolShared, next_id: &mut u64) {
    let plan = roster.plan(&shared.limits);

    for id in &plan.trim {
        if let Some(out) = roster.idle_sender(*id) {
            debug!(?id, "closing an idle Marvin socket above the pool target");
            let _ = out.send(SlotCmd::Close);
        }
        roster.apply(
            *id,
            SlotEvent::Closed {
                reason: "trimmed".to_string(),
            },
        );
    }

    for _ in 0..plan.spawn {
        let id = SlotId(*next_id);
        *next_id += 1;
        roster.insert_warming(id);
        tokio::spawn(super::slot::run(
            id,
            shared.endpoint.clone(),
            shared.tx.clone(),
        ));
    }

    if let Ok(mut snapshot) = shared.snapshot.lock() {
        *snapshot = roster.stats(shared.limits.max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_reject_zero_max() {
        assert!(PoolLimits::new(0, 0, 0).is_err());
    }

    #[test]
    fn limits_reject_min_above_max() {
        assert!(PoolLimits::new(3, 1, 2).is_err());
    }

    #[test]
    fn limits_reject_spare_above_max() {
        assert!(PoolLimits::new(1, 3, 2).is_err());
    }

    #[test]
    fn limits_accept_the_documented_defaults() {
        assert_eq!(
            PoolLimits::new(2, 2, 50).expect("defaults are valid"),
            PoolLimits {
                min: 2,
                spare: 2,
                max: 50
            }
        );
    }
}
