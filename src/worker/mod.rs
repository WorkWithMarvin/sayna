//! LiveKit `/agent` worker and the pre-warmed Marvin socket pool.

pub mod pool;
pub mod roster;
pub mod slot;

pub use pool::{
    AcquireError, JobBrief, MarvinEndpoint, ParticipantBrief, Pool, PoolError, PoolLimits,
    PoolStats, SlotIo,
};
