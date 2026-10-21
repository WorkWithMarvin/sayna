//! LiveKit `/agent` worker and the pre-warmed Marvin socket pool.

mod dispatch;
pub mod pool;
mod protocol;
pub mod roster;
pub mod slot;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use livekit_protocol::{
    AvailabilityRequest, AvailabilityResponse, JobStatus, UpdateJobStatus, UpdateWorkerStatus,
    WorkerMessage, WorkerPong, WorkerStatus, server_message, worker_message,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::config::ServerConfig;
use crate::state::AppState;

use dispatch::JobOutcome;
use protocol::{AgentLink, read_server, registration, write_worker};

pub use pool::{
    AcquireError, JobBrief, MarvinEndpoint, ParticipantBrief, Pool, PoolError, PoolLimits,
    PoolStats, SlotIo,
};

/// Live worker state, surfaced by `GET /`. The container has no log stream, so
/// this is the only way to see why `/agent` registration failed.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct WorkerHealth {
    pub agent_name: String,
    pub registered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub pool: PoolStats,
}

static HEALTH: Mutex<WorkerHealth> = Mutex::new(WorkerHealth {
    agent_name: String::new(),
    registered: false,
    worker_id: None,
    last_error: None,
    pool: PoolStats {
        idle: 0,
        warming: 0,
        leased: 0,
        limit: 0,
    },
});

fn update_health(f: impl FnOnce(&mut WorkerHealth)) {
    if let Ok(mut health) = HEALTH.lock() {
        f(&mut health);
    }
}

pub(crate) fn set_pool_stats(stats: PoolStats) {
    update_health(|health| health.pool = stats);
}

/// `None` when the worker is disabled (`MARVIN_WS_URL` unset).
pub fn health() -> Option<WorkerHealth> {
    HEALTH
        .lock()
        .ok()
        .map(|health| health.clone())
        .filter(|health| !health.agent_name.is_empty())
}

const PING_INTERVAL: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("LiveKit API credentials are required for the /agent worker")]
    MissingCredentials,
    #[error("worker transport: {0}")]
    Transport(String),
    #[error("worker protocol: {0}")]
    Protocol(String),
    #[error("worker pool: {0}")]
    Pool(#[from] PoolError),
    #[error("worker connection closed")]
    Closed,
}

#[derive(Clone)]
pub struct WorkerConfig {
    pub livekit_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub agent_name: String,
    pub marvin: MarvinEndpoint,
    pub limits: PoolLimits,
}

impl WorkerConfig {
    /// `None` when `MARVIN_WS_URL` is unset: the worker path is opt-in.
    pub fn from_server_config(config: &ServerConfig) -> Result<Option<Self>, WorkerError> {
        let marvin = match MarvinEndpoint::from_config(config) {
            Ok(marvin) => marvin,
            Err(PoolError::MissingUrl) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let (Some(api_key), Some(api_secret)) = (
            config.livekit_api_key.clone(),
            config.livekit_api_secret.clone(),
        ) else {
            return Err(WorkerError::MissingCredentials);
        };

        Ok(Some(Self {
            livekit_url: config.livekit_url.clone(),
            api_key,
            api_secret,
            agent_name: config
                .livekit_agent_name
                .clone()
                .unwrap_or_else(|| "sayna-worker".to_string()),
            marvin,
            limits: PoolLimits::from_config(config)?,
        }))
    }
}

#[derive(Clone)]
pub struct Worker {
    config: WorkerConfig,
    pool: Pool,
    app: Arc<AppState>,
}

impl Worker {
    pub async fn start(app: Arc<AppState>) -> Result<Option<Worker>, WorkerError> {
        let Some(config) = WorkerConfig::from_server_config(&app.config)? else {
            return Ok(None);
        };
        let pool = Pool::start(config.marvin.clone(), config.limits.clone())?;
        let worker = Worker { config, pool, app };
        update_health(|health| {
            health.agent_name = worker.config.agent_name.clone();
            health.pool = worker.pool.stats();
        });

        let runner = worker.clone();
        tokio::spawn(async move { runner.run().await });

        Ok(Some(worker))
    }

    pub fn agent_name(&self) -> &str {
        &self.config.agent_name
    }

    async fn run(self) {
        loop {
            if let Err(e) = self.run_once().await {
                warn!(%e, "LiveKit worker connection lost; reconnecting");
                update_health(|health| {
                    health.registered = false;
                    health.last_error = Some(e.to_string());
                });
                tokio::time::sleep(RECONNECT_DELAY).await;
            } else {
                update_health(|health| health.registered = false);
            }
        }
    }

    async fn run_once(&self) -> Result<(), WorkerError> {
        let mut link = AgentLink::connect(&self.config).await?;
        let registered = link.register(registration(&self.config)).await?;
        info!(
            worker_id = %registered.worker_id,
            agent = %self.config.agent_name,
            "LiveKit worker registered"
        );
        update_health(|health| {
            health.registered = true;
            health.worker_id = Some(registered.worker_id.clone());
            health.last_error = None;
        });

        let (mut sink, mut stream) = link.into_parts();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let mut active: HashMap<String, oneshot::Sender<()>> = HashMap::new();
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await;
        let mut seq = 0u64;

        // A registered worker that never reports load is not dispatched to.
        write_worker(&mut sink, worker_status(0, self.pool.stats().limit, seq)).await?;

        let outcome = loop {
            tokio::select! {
                incoming = read_server(&mut stream) => {
                    let message = match incoming {
                        Ok(message) => message,
                        Err(e) => break Err(e),
                    };
                    debug!(frame = frame_kind(&message), "LiveKit /agent frame");

                    match message.message {
                        Some(server_message::Message::Availability(request)) => {
                            let response = self.availability(&request);
                            if let Err(e) = write_worker(
                                &mut sink,
                                WorkerMessage {
                                    message: Some(worker_message::Message::Availability(response)),
                                },
                            )
                            .await
                            {
                                break Err(e);
                            }
                        }
                        Some(server_message::Message::Assignment(assignment)) => {
                            let Some(job) = assignment.job.clone() else {
                                continue;
                            };
                            let Some(brief) = JobBrief::from_job(&job) else {
                                warn!(job_id = %job.id, "assignment has no room; declining to serve it");
                                continue;
                            };

                            let job_id = brief.job_id.clone();
                            let task_job_id = job_id.clone();
                            let app = self.app.clone();
                            let pool = self.pool.clone();
                            let events = events_tx.clone();
                            let url = assignment.url.unwrap_or_default();
                            let token = assignment.token;

                            let (cancel_tx, cancel_rx) = oneshot::channel();
                            tokio::spawn(async move {
                                let outcome =
                                    dispatch::run(app, pool, brief, url, token, cancel_rx).await;
                                let _ = events.send(WorkerEvent::JobDone {
                                    job_id: task_job_id,
                                    outcome,
                                });
                            });
                            active.insert(job_id.clone(), cancel_tx);
                            seq += 1;

                            if let Err(e) = write_worker(&mut sink, job_status(job_id, JobStatus::JsRunning, String::new())).await {
                                break Err(e);
                            }
                            if let Err(e) = write_worker(&mut sink, worker_status(active.len(), self.pool.stats().limit, seq)).await {
                                break Err(e);
                            }
                        }
                        Some(server_message::Message::Termination(termination)) => {
                            if let Some(cancel) = active.remove(&termination.job_id) {
                                let _ = cancel.send(());
                            }
                        }
                        Some(server_message::Message::Pong(WorkerPong { .. }))
                        | Some(server_message::Message::Register(_)) => {}
                        Some(server_message::Message::GoAway(_)) | None => break Ok(()),
                    }
                }
                _ = ping.tick() => {
                    let ping = WorkerMessage {
                        message: Some(worker_message::Message::Ping(livekit_protocol::WorkerPing {
                            timestamp: now_millis(),
                        })),
                    };
                    if let Err(e) = write_worker(&mut sink, ping).await {
                        break Err(e);
                    }
                    seq += 1;
                    let status = worker_status(active.len(), self.pool.stats().limit, seq);
                    if let Err(e) = write_worker(&mut sink, status).await {
                        break Err(e);
                    }
                }
                Some(WorkerEvent::JobDone { job_id, outcome }) = events_rx.recv() => {
                    active.remove(&job_id);
                    seq += 1;
                    if let Err(e) = write_worker(&mut sink, job_status(job_id, JobStatus::try_from(outcome.status).unwrap_or(JobStatus::JsFailed), outcome.error)).await {
                        break Err(e);
                    }
                    if let Err(e) = write_worker(&mut sink, worker_status(active.len(), self.pool.stats().limit, seq)).await {
                        break Err(e);
                    }
                }
            }
        };

        for (_, cancel) in active {
            let _ = cancel.send(());
        }
        outcome
    }

    fn availability(&self, request: &AvailabilityRequest) -> AvailabilityResponse {
        let stats = self.pool.stats();
        update_health(|health| health.pool = stats.clone());
        AvailabilityResponse {
            job_id: request
                .job
                .as_ref()
                .map(|job| job.id.clone())
                .unwrap_or_default(),
            available: stats.has_capacity(),
            supports_resume: false,
            terminate: false,
            participant_identity: "marvin-agent".to_string(),
            participant_name: "Marvin".to_string(),
            participant_metadata: String::new(),
            participant_attributes: Default::default(),
        }
    }
}

enum WorkerEvent {
    JobDone { job_id: String, outcome: JobOutcome },
}

fn job_status(job_id: String, status: JobStatus, error: String) -> WorkerMessage {
    WorkerMessage {
        message: Some(worker_message::Message::UpdateJob(UpdateJobStatus {
            job_id,
            status: status as i32,
            error,
        })),
    }
}

fn worker_status(job_count: usize, limit: usize, seq: u64) -> WorkerMessage {
    let load = if limit == 0 {
        0.0
    } else {
        job_count as f32 / limit as f32
    };
    let status = if limit > 0 && job_count >= limit {
        WorkerStatus::WsFull
    } else {
        WorkerStatus::WsAvailable
    };
    WorkerMessage {
        message: Some(worker_message::Message::UpdateWorker(UpdateWorkerStatus {
            status: Some(status as i32),
            load,
            job_count: job_count as u32,
            draining: false,
            seq,
        })),
    }
}

fn frame_kind(message: &livekit_protocol::ServerMessage) -> &'static str {
    match message.message {
        Some(server_message::Message::Register(_)) => "register",
        Some(server_message::Message::Availability(_)) => "availability",
        Some(server_message::Message::Assignment(_)) => "assignment",
        Some(server_message::Message::Termination(_)) => "termination",
        Some(server_message::Message::Pong(_)) => "pong",
        Some(server_message::Message::GoAway(_)) => "go_away",
        None => "empty",
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
