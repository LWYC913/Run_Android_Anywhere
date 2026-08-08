//! Bounded weighted fairness across lanes and projects.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use run_anywhere_contracts::{JobId, JobMode, ProjectId};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriorityLane {
    Interactive,
    Batch,
}

impl From<JobMode> for PriorityLane {
    fn from(mode: JobMode) -> Self {
        match mode {
            JobMode::BrowserDebug => Self::Interactive,
            JobMode::HeadlessCi => Self::Batch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FairQueueConfig {
    pub capacity: usize,
    pub interactive_weight: usize,
    pub batch_weight: usize,
    pub batch_aging: std::time::Duration,
}

impl Default for FairQueueConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            interactive_weight: 3,
            batch_weight: 1,
            batch_aging: std::time::Duration::from_secs(5 * 60),
        }
    }
}

impl From<&crate::config::FairnessSettings> for FairQueueConfig {
    fn from(settings: &crate::config::FairnessSettings) -> Self {
        Self {
            capacity: settings.buffer_size,
            interactive_weight: settings.interactive_weight,
            batch_weight: settings.batch_weight,
            batch_aging: settings.batch_aging,
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum FairQueueConfigError {
    #[error("fairness buffer capacity must be greater than zero")]
    EmptyCapacity,
    #[error("both fairness lane weights must be greater than zero")]
    EmptyWeight,
    #[error("the combined lane weight must not exceed 1024")]
    ExcessiveWeight,
    #[error("batch aging must be greater than zero")]
    EmptyBatchAging,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FairEntry<T> {
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub lane: PriorityLane,
    /// Canonical database creation time used for FIFO ordering.
    pub created_at: DateTime<Utc>,
    /// Time inserted into this in-memory buffer, used for aging.
    pub enqueued_at: DateTime<Utc>,
    pub payload: T,
}

impl<T> FairEntry<T> {
    pub fn new(
        job_id: JobId,
        project_id: ProjectId,
        lane: PriorityLane,
        created_at: DateTime<Utc>,
        enqueued_at: DateTime<Utc>,
        payload: T,
    ) -> Self {
        Self {
            job_id,
            project_id,
            lane,
            created_at,
            enqueued_at,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FairQueueError<T> {
    Full(FairEntry<T>),
    Duplicate(FairEntry<T>),
}

#[derive(Debug)]
pub struct FairQueue<T> {
    capacity: usize,
    batch_aging: ChronoDuration,
    schedule: Vec<PriorityLane>,
    cursor: usize,
    interactive: LaneQueue<T>,
    batch: LaneQueue<T>,
    job_ids: BTreeSet<JobId>,
    len: usize,
    last_pop_forced_aged_batch: bool,
}

impl<T> FairQueue<T> {
    pub fn new(config: FairQueueConfig) -> Result<Self, FairQueueConfigError> {
        if config.capacity == 0 {
            return Err(FairQueueConfigError::EmptyCapacity);
        }
        if config.interactive_weight == 0 || config.batch_weight == 0 {
            return Err(FairQueueConfigError::EmptyWeight);
        }
        let combined_weight = config
            .interactive_weight
            .checked_add(config.batch_weight)
            .ok_or(FairQueueConfigError::ExcessiveWeight)?;
        if combined_weight > 1024 {
            return Err(FairQueueConfigError::ExcessiveWeight);
        }
        if config.batch_aging.is_zero() {
            return Err(FairQueueConfigError::EmptyBatchAging);
        }
        let batch_aging = ChronoDuration::from_std(config.batch_aging)
            .map_err(|_| FairQueueConfigError::EmptyBatchAging)?;
        let mut schedule = Vec::with_capacity(combined_weight);
        schedule.extend(std::iter::repeat_n(
            PriorityLane::Interactive,
            config.interactive_weight,
        ));
        schedule.extend(std::iter::repeat_n(
            PriorityLane::Batch,
            config.batch_weight,
        ));

        Ok(Self {
            capacity: config.capacity,
            batch_aging,
            schedule,
            cursor: 0,
            interactive: LaneQueue::default(),
            batch: LaneQueue::default(),
            job_ids: BTreeSet::new(),
            len: 0,
            last_pop_forced_aged_batch: false,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn lane_len(&self, lane: PriorityLane) -> usize {
        self.lane(lane).len
    }

    pub fn contains(&self, job_id: &JobId) -> bool {
        self.job_ids.contains(job_id)
    }

    pub fn get_mut(&mut self, job_id: &JobId) -> Option<&mut FairEntry<T>> {
        self.interactive
            .get_mut(job_id)
            .or_else(|| self.batch.get_mut(job_id))
    }

    /// Visit buffered entries without changing lane, project-ring, or FIFO
    /// ordering. The dispatcher uses this to renew every held JetStream
    /// delivery independently of how quickly workers become available.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut FairEntry<T>> {
        self.interactive.iter_mut().chain(self.batch.iter_mut())
    }

    pub fn push(&mut self, entry: FairEntry<T>) -> Result<(), FairQueueError<T>> {
        if self.job_ids.contains(&entry.job_id) {
            return Err(FairQueueError::Duplicate(entry));
        }
        if self.len == self.capacity {
            return Err(FairQueueError::Full(entry));
        }
        self.job_ids.insert(entry.job_id.clone());
        self.lane_mut(entry.lane).push(entry);
        self.len += 1;
        Ok(())
    }

    /// Select the next item. Lane service follows weighted round-robin. Once a
    /// batch entry ages out, one may jump the cursor, but two aging overrides
    /// never run consecutively so newly-arrived interactive work still moves.
    pub fn pop(&mut self, now: DateTime<Utc>) -> Option<FairEntry<T>> {
        let aged_batch = self
            .batch
            .oldest_enqueued_at()
            .is_some_and(|enqueued_at| now.signed_duration_since(enqueued_at) >= self.batch_aging);
        if aged_batch && !self.last_pop_forced_aged_batch {
            self.last_pop_forced_aged_batch = true;
            return self.pop_lane(PriorityLane::Batch);
        }
        self.last_pop_forced_aged_batch = false;

        for _ in 0..self.schedule.len() {
            let lane = self.schedule[self.cursor];
            self.cursor = (self.cursor + 1) % self.schedule.len();
            if let Some(entry) = self.pop_lane(lane) {
                return Some(entry);
            }
        }
        None
    }

    pub fn remove(&mut self, job_id: &JobId) -> Option<FairEntry<T>> {
        let removed = self
            .interactive
            .remove(job_id)
            .or_else(|| self.batch.remove(job_id));
        if removed.is_some() {
            self.job_ids.remove(job_id);
            self.len -= 1;
        }
        removed
    }

    fn pop_lane(&mut self, lane: PriorityLane) -> Option<FairEntry<T>> {
        let entry = self.lane_mut(lane).pop()?;
        self.job_ids.remove(&entry.job_id);
        self.len -= 1;
        Some(entry)
    }

    fn lane(&self, lane: PriorityLane) -> &LaneQueue<T> {
        match lane {
            PriorityLane::Interactive => &self.interactive,
            PriorityLane::Batch => &self.batch,
        }
    }

    fn lane_mut(&mut self, lane: PriorityLane) -> &mut LaneQueue<T> {
        match lane {
            PriorityLane::Interactive => &mut self.interactive,
            PriorityLane::Batch => &mut self.batch,
        }
    }
}

#[derive(Debug)]
struct LaneQueue<T> {
    projects: BTreeMap<ProjectId, Vec<FairEntry<T>>>,
    project_ring: VecDeque<ProjectId>,
    len: usize,
}

impl<T> Default for LaneQueue<T> {
    fn default() -> Self {
        Self {
            projects: BTreeMap::new(),
            project_ring: VecDeque::new(),
            len: 0,
        }
    }
}

impl<T> LaneQueue<T> {
    fn push(&mut self, entry: FairEntry<T>) {
        let project_id = entry.project_id.clone();
        let bucket = self.projects.entry(project_id.clone()).or_insert_with(|| {
            self.project_ring.push_back(project_id);
            Vec::new()
        });
        let position = bucket
            .binary_search_by(|candidate| {
                (&candidate.created_at, &candidate.job_id).cmp(&(&entry.created_at, &entry.job_id))
            })
            .unwrap_or_else(|position| position);
        bucket.insert(position, entry);
        self.len += 1;
    }

    fn pop(&mut self) -> Option<FairEntry<T>> {
        let project_id = self.project_ring.pop_front()?;
        let (entry, has_more) = {
            let bucket = self
                .projects
                .get_mut(&project_id)
                .expect("project ring and buckets stay synchronized");
            let entry = bucket.remove(0);
            (entry, !bucket.is_empty())
        };
        if has_more {
            self.project_ring.push_back(project_id);
        } else {
            self.projects.remove(&project_id);
        }
        self.len -= 1;
        Some(entry)
    }

    fn remove(&mut self, job_id: &JobId) -> Option<FairEntry<T>> {
        let project_id = self.projects.iter().find_map(|(project_id, entries)| {
            entries
                .iter()
                .any(|entry| &entry.job_id == job_id)
                .then(|| project_id.clone())
        })?;
        let bucket = self.projects.get_mut(&project_id)?;
        let position = bucket.iter().position(|entry| &entry.job_id == job_id)?;
        let entry = bucket.remove(position);
        if bucket.is_empty() {
            self.projects.remove(&project_id);
            self.project_ring
                .retain(|candidate| candidate != &project_id);
        }
        self.len -= 1;
        Some(entry)
    }

    fn get_mut(&mut self, job_id: &JobId) -> Option<&mut FairEntry<T>> {
        self.projects
            .values_mut()
            .flat_map(|entries| entries.iter_mut())
            .find(|entry| &entry.job_id == job_id)
    }

    fn oldest_enqueued_at(&self) -> Option<DateTime<Utc>> {
        self.projects
            .values()
            .flat_map(|entries| entries.iter().map(|entry| entry.enqueued_at))
            .min()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut FairEntry<T>> {
        self.projects
            .values_mut()
            .flat_map(|entries| entries.iter_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    fn entry(
        job: &str,
        project: &str,
        lane: PriorityLane,
        created: i64,
        enqueued: i64,
    ) -> FairEntry<()> {
        FairEntry::new(
            JobId::new(job).unwrap(),
            ProjectId::new(project).unwrap(),
            lane,
            at(created),
            at(enqueued),
            (),
        )
    }

    #[test]
    fn applies_three_to_one_lane_weight() {
        let mut queue = FairQueue::new(FairQueueConfig::default()).unwrap();
        for index in 0..8 {
            queue
                .push(entry(
                    &format!("job_i{index}"),
                    "proj_i",
                    PriorityLane::Interactive,
                    index,
                    100,
                ))
                .unwrap();
            queue
                .push(entry(
                    &format!("job_b{index}"),
                    "proj_b",
                    PriorityLane::Batch,
                    index,
                    100,
                ))
                .unwrap();
        }
        let lanes = (0..8)
            .map(|_| queue.pop(at(100)).unwrap().lane)
            .collect::<Vec<_>>();
        assert_eq!(
            lanes,
            vec![
                PriorityLane::Interactive,
                PriorityLane::Interactive,
                PriorityLane::Interactive,
                PriorityLane::Batch,
                PriorityLane::Interactive,
                PriorityLane::Interactive,
                PriorityLane::Interactive,
                PriorityLane::Batch,
            ]
        );
    }

    #[test]
    fn round_robins_projects_and_preserves_project_fifo() {
        let mut queue = FairQueue::new(FairQueueConfig::default()).unwrap();
        queue
            .push(entry("job_a2", "proj_a", PriorityLane::Interactive, 2, 10))
            .unwrap();
        queue
            .push(entry("job_b1", "proj_b", PriorityLane::Interactive, 1, 10))
            .unwrap();
        queue
            .push(entry("job_a1", "proj_a", PriorityLane::Interactive, 1, 10))
            .unwrap();
        assert_eq!(queue.pop(at(10)).unwrap().job_id.as_str(), "job_a1");
        assert_eq!(queue.pop(at(10)).unwrap().job_id.as_str(), "job_b1");
        assert_eq!(queue.pop(at(10)).unwrap().job_id.as_str(), "job_a2");
    }

    #[test]
    fn aged_batch_can_jump_an_interactive_slot_but_not_twice() {
        let mut queue = FairQueue::new(FairQueueConfig::default()).unwrap();
        queue
            .push(entry("job_b1", "proj_b", PriorityLane::Batch, 1, 0))
            .unwrap();
        queue
            .push(entry("job_b2", "proj_b", PriorityLane::Batch, 2, 0))
            .unwrap();
        queue
            .push(entry("job_i1", "proj_i", PriorityLane::Interactive, 3, 300))
            .unwrap();
        assert_eq!(queue.pop(at(300)).unwrap().job_id.as_str(), "job_b1");
        assert_eq!(queue.pop(at(300)).unwrap().job_id.as_str(), "job_i1");
        assert_eq!(queue.pop(at(300)).unwrap().job_id.as_str(), "job_b2");
    }

    #[test]
    fn rejects_duplicate_and_over_capacity_entries_without_losing_them() {
        let mut queue = FairQueue::new(FairQueueConfig {
            capacity: 1,
            ..FairQueueConfig::default()
        })
        .unwrap();
        queue
            .push(entry("job_1", "proj_a", PriorityLane::Batch, 1, 1))
            .unwrap();
        let duplicate = entry("job_1", "proj_a", PriorityLane::Batch, 1, 1);
        assert!(matches!(
            queue.push(duplicate),
            Err(FairQueueError::Duplicate(_))
        ));
        let full = entry("job_2", "proj_a", PriorityLane::Batch, 2, 2);
        assert!(matches!(queue.push(full), Err(FairQueueError::Full(_))));
    }
}
