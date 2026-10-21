//! Lease accounting for the outbound Marvin socket pool.
//!
//! The pool actor is the only writer: socket tasks report [`SlotEvent`]s and
//! [`Roster::plan`] turns the resulting state into spawns and trims.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::pool::{PoolLimits, PoolStats};
use super::slot::SlotCmd;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(pub u64);

pub enum SlotPhase {
    Warming {
        started: Instant,
    },
    Idle {
        since: Instant,
        out: mpsc::UnboundedSender<SlotCmd>,
    },
    Leased {
        job_id: String,
        since: Instant,
    },
    Dead {
        reason: String,
    },
}

pub enum SlotEvent {
    Connected { out: mpsc::UnboundedSender<SlotCmd> },
    AssignFailed { reason: String },
    Closed { reason: String },
}

/// What the actor must do after a state change. Spawning is expressed as a
/// count because the actor owns id allocation.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub spawn: usize,
    pub trim: Vec<SlotId>,
}

#[derive(Default)]
pub struct Roster {
    slots: BTreeMap<SlotId, SlotPhase>,
}

impl Roster {
    pub fn insert_warming(&mut self, id: SlotId) {
        self.slots.insert(
            id,
            SlotPhase::Warming {
                started: Instant::now(),
            },
        );
    }

    pub fn apply(&mut self, id: SlotId, event: SlotEvent) {
        match event {
            SlotEvent::Connected { out } => {
                if let Some(phase) = self.slots.get_mut(&id) {
                    if !matches!(phase, SlotPhase::Leased { .. }) {
                        *phase = SlotPhase::Idle {
                            since: Instant::now(),
                            out,
                        };
                    }
                }
            }
            SlotEvent::AssignFailed { reason } | SlotEvent::Closed { reason } => {
                self.slots.insert(id, SlotPhase::Dead { reason });
            }
        }
    }

    /// Target idle = `clamp(max(min - leased, spare), 0, max - leased)`.
    ///
    /// `spare` is headroom above live jobs, `min` is the floor when nothing is
    /// live, and `max - leased` is all the room left once jobs own sockets.
    pub fn plan(&mut self, limits: &PoolLimits) -> Plan {
        self.slots
            .retain(|_, phase| !matches!(phase, SlotPhase::Dead { .. }));

        let leased = self.count(|p| matches!(p, SlotPhase::Leased { .. }));
        let idle = self.count(|p| matches!(p, SlotPhase::Idle { .. }));
        let warming = self.count(|p| matches!(p, SlotPhase::Warming { .. }));

        let floor = limits.min.saturating_sub(leased).max(limits.spare);
        let target = floor.min(limits.max.saturating_sub(leased));

        let spawn = target.saturating_sub(idle + warming);

        let trim = if idle > floor {
            self.slots
                .iter()
                .filter(|(_, phase)| matches!(phase, SlotPhase::Idle { .. }))
                .map(|(id, _)| *id)
                .take(idle - floor)
                .collect()
        } else {
            Vec::new()
        };

        Plan { spawn, trim }
    }

    pub fn lease(&mut self, job_id: &str) -> Option<(SlotId, mpsc::UnboundedSender<SlotCmd>)> {
        let id = self
            .slots
            .iter()
            .find_map(|(id, phase)| matches!(phase, SlotPhase::Idle { .. }).then_some(*id))?;
        let phase = self.slots.remove(&id)?;
        let SlotPhase::Idle { out, .. } = phase else {
            return None;
        };
        self.slots.insert(
            id,
            SlotPhase::Leased {
                job_id: job_id.to_string(),
                since: Instant::now(),
            },
        );
        Some((id, out))
    }

    pub fn idle_sender(&self, id: SlotId) -> Option<&mpsc::UnboundedSender<SlotCmd>> {
        match self.slots.get(&id) {
            Some(SlotPhase::Idle { out, .. }) => Some(out),
            _ => None,
        }
    }

    /// Warming sockets that never completed their handshake are dead weight:
    /// they are replaced on the next plan.
    pub fn expire(&mut self, now: Instant, warm_timeout: Duration) -> Vec<SlotId> {
        let expired: Vec<SlotId> = self
            .slots
            .iter()
            .filter_map(|(id, phase)| match phase {
                SlotPhase::Warming { started } if now.duration_since(*started) >= warm_timeout => {
                    Some(*id)
                }
                _ => None,
            })
            .collect();

        for id in &expired {
            self.slots.insert(
                *id,
                SlotPhase::Dead {
                    reason: "warm timeout".to_string(),
                },
            );
        }
        expired
    }

    pub fn stats(&self, max: usize) -> PoolStats {
        PoolStats {
            idle: self.count(|p| matches!(p, SlotPhase::Idle { .. })),
            warming: self.count(|p| matches!(p, SlotPhase::Warming { .. })),
            leased: self.count(|p| matches!(p, SlotPhase::Leased { .. })),
            limit: max,
        }
    }

    fn count(&self, predicate: impl Fn(&SlotPhase) -> bool) -> usize {
        self.slots.values().filter(|phase| predicate(phase)).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(min: usize, spare: usize, max: usize) -> PoolLimits {
        PoolLimits::new(min, spare, max).expect("valid limits")
    }

    fn idle_roster(count: u64) -> Roster {
        let mut roster = Roster::default();
        for id in 0..count {
            let (out, _rx) = mpsc::unbounded_channel();
            roster.insert_warming(SlotId(id));
            roster.apply(SlotId(id), SlotEvent::Connected { out });
        }
        roster
    }

    #[test]
    fn plan_spawns_min_when_empty() {
        let mut roster = Roster::default();
        let plan = roster.plan(&limits(2, 2, 50));
        assert_eq!(plan.spawn, 2);
        assert!(plan.trim.is_empty());
    }

    #[test]
    fn plan_counts_warming_toward_the_target() {
        let mut roster = Roster::default();
        roster.insert_warming(SlotId(1));
        roster.insert_warming(SlotId(2));
        assert_eq!(roster.plan(&limits(2, 2, 50)).spawn, 0);
    }

    #[test]
    fn plan_keeps_spare_above_leased_jobs() {
        let mut roster = idle_roster(4);
        for _ in 0..3 {
            roster.lease("job").expect("idle slot");
        }
        let plan = roster.plan(&limits(2, 2, 50));
        // floor = max(2 - 3, 2) = 2, with one idle left -> one replacement.
        assert_eq!(plan.spawn, 1);
        assert!(plan.trim.is_empty());
    }

    #[test]
    fn plan_clamps_target_to_max() {
        let mut roster = idle_roster(2);
        roster.lease("job").expect("idle slot");
        roster.lease("job").expect("idle slot");
        let plan = roster.plan(&limits(2, 2, 2));
        assert_eq!(plan.spawn, 0);
    }

    #[test]
    fn plan_never_exceeds_max_with_spare() {
        let mut roster = idle_roster(1);
        roster.lease("job").expect("idle slot");
        let plan = roster.plan(&limits(2, 2, 2));
        // max - leased = 1, idle = 0, so only one spawn even though spare is 2.
        assert_eq!(plan.spawn, 1);
    }

    #[test]
    fn plan_trims_excess_idle() {
        let mut roster = idle_roster(5);
        let plan = roster.plan(&limits(2, 2, 50));
        assert_eq!(plan.spawn, 0);
        assert_eq!(plan.trim, vec![SlotId(0), SlotId(1), SlotId(2)]);
    }

    #[test]
    fn plan_reaps_dead_slots() {
        let mut roster = idle_roster(1);
        let id = SlotId(0);
        roster.apply(
            id,
            SlotEvent::Closed {
                reason: "peer closed".to_string(),
            },
        );
        let plan = roster.plan(&limits(1, 0, 4));
        assert_eq!(plan.spawn, 1);
        assert_eq!(roster.stats(4).total(), 0);
    }

    #[test]
    fn lease_moves_the_oldest_idle_slot_to_leased() {
        let mut roster = idle_roster(2);
        let (id, _out) = roster.lease("job-1").expect("idle slot");
        assert_eq!(id, SlotId(0));
        let stats = roster.stats(10);
        assert_eq!(stats.idle, 1);
        assert_eq!(stats.leased, 1);
        assert!(roster.idle_sender(SlotId(0)).is_none());
    }

    #[test]
    fn expire_drops_warming_slots_past_the_timeout() {
        let mut roster = Roster::default();
        roster.insert_warming(SlotId(1));
        let future = Instant::now() + Duration::from_secs(30);
        let expired = roster.expire(future, Duration::from_secs(10));
        assert_eq!(expired, vec![SlotId(1)]);
        assert_eq!(roster.stats(4).warming, 0);
    }
}
