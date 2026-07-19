use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use run_anywhere_contracts::{
    HostArch, IsolationTier, RuntimeProfile, WorkerHeartbeat, WorkerId, WorkerRegistration,
    WorkerState, WorkerStatus,
};

use crate::{
    HeartbeatReceipt, Repository, RepositoryError, RepositoryResult, codec::encode_enum,
    rows::WorkerRow,
};

impl Repository {
    pub async fn upsert_worker(
        &self,
        registration: WorkerRegistration,
    ) -> RepositoryResult<WorkerStatus> {
        let runtimes = validate_runtimes(registration.runtimes)?;
        let arch = encode_enum(registration.arch)?;
        let capacity = i64::from(registration.capacity);
        let row = sqlx::query_as::<_, WorkerRow>(
            "INSERT INTO workers (id, runtimes, kvm, gpu, arch, capacity) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET runtimes = EXCLUDED.runtimes, kvm = EXCLUDED.kvm, \
             gpu = EXCLUDED.gpu, arch = EXCLUDED.arch, capacity = EXCLUDED.capacity, \
             last_heartbeat_at = now(), updated_at = now() \
             WHERE workers.active_jobs <= EXCLUDED.capacity \
             RETURNING id, runtimes, kvm, gpu, arch, capacity, active_jobs, state, \
             last_heartbeat_at, reported_last_seen_at, registered_at, updated_at",
        )
        .bind(registration.worker_id.as_str())
        .bind(runtimes)
        .bind(registration.kvm)
        .bind(registration.gpu)
        .bind(arch)
        .bind(capacity)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            RepositoryError::Conflict(
                "worker capacity cannot be lowered below its active reservations".to_owned(),
            )
        })?;
        row.into_status()
    }

    pub async fn record_heartbeat(
        &self,
        heartbeat: WorkerHeartbeat,
        lease_ttl: Duration,
    ) -> RepositoryResult<HeartbeatReceipt> {
        let lease_seconds = lease_ttl.num_seconds();
        if lease_seconds <= 0 {
            return Err(RepositoryError::Validation(
                "lease TTL must be at least one second".to_owned(),
            ));
        }
        if heartbeat.active_jobs > heartbeat.capacity {
            return Err(RepositoryError::Validation(
                "heartbeat capacity must be at least active_jobs".to_owned(),
            ));
        }
        let runtimes = validate_runtimes(heartbeat.runtimes.clone())?;
        let arch = encode_enum(heartbeat.arch)?;
        let capacity = i64::from(heartbeat.capacity);
        let mut tx = self.pool.begin().await?;

        // Every path that needs both rows locks jobs before workers. Sorting the
        // lock set also keeps concurrent multi-lease heartbeats deterministic.
        let mut job_ids = heartbeat
            .lease_extends
            .iter()
            .map(|extension| extension.job_id.to_string())
            .collect::<Vec<_>>();
        job_ids.sort_unstable();
        job_ids.dedup();
        if !job_ids.is_empty() {
            sqlx::query("SELECT id FROM jobs WHERE id = ANY($1) ORDER BY id FOR UPDATE")
                .bind(&job_ids)
                .fetch_all(&mut *tx)
                .await?;
        }

        let active_jobs: Option<i64> =
            sqlx::query_scalar("SELECT active_jobs FROM workers WHERE id = $1 FOR UPDATE")
                .bind(heartbeat.worker_id.as_str())
                .fetch_optional(&mut *tx)
                .await?;
        let Some(active_jobs) = active_jobs else {
            return Err(RepositoryError::not_found(
                "worker",
                heartbeat.worker_id.as_str(),
            ));
        };
        if active_jobs > capacity {
            return Err(RepositoryError::Conflict(
                "reported capacity is below active reservations".to_owned(),
            ));
        }
        let recorded_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE workers SET runtimes = $2, kvm = $3, gpu = $4, arch = $5, capacity = $6, \
             last_heartbeat_at = $7, reported_last_seen_at = $8, updated_at = $7 WHERE id = $1",
        )
        .bind(heartbeat.worker_id.as_str())
        .bind(runtimes)
        .bind(heartbeat.kvm)
        .bind(heartbeat.gpu)
        .bind(arch)
        .bind(capacity)
        .bind(recorded_at)
        .bind(heartbeat.last_seen)
        .execute(&mut *tx)
        .await?;

        let mut extended = Vec::new();
        let mut rejected = Vec::new();
        for extension in heartbeat.lease_extends {
            let updated = sqlx::query(
                "UPDATE jobs SET lease_expires_at = $4 + make_interval(secs => $5), \
                 last_lease_extended_at = $4 \
                 WHERE id = $1 AND worker_id = $2 AND lease_id = $3 \
                 AND lease_expires_at > $4 \
                 AND state NOT IN ('queued','passed','failed','cancelled','timed_out','infra_failed')",
            )
            .bind(extension.job_id.as_str())
            .bind(heartbeat.worker_id.as_str())
            .bind(extension.lease_id.as_str())
            .bind(recorded_at)
            .bind(lease_seconds as f64)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() == 1 {
                extended.push(extension);
            } else {
                rejected.push(extension);
            }
        }
        tx.commit().await?;
        Ok(HeartbeatReceipt {
            recorded_at,
            extended,
            rejected,
        })
    }

    pub async fn set_worker_state(
        &self,
        worker_id: &WorkerId,
        state: WorkerState,
    ) -> RepositoryResult<WorkerStatus> {
        let state = encode_enum(state)?;
        let row = sqlx::query_as::<_, WorkerRow>(
            "UPDATE workers SET state = $2, updated_at = now() WHERE id = $1 \
             RETURNING id, runtimes, kvm, gpu, arch, capacity, active_jobs, state, \
             last_heartbeat_at, reported_last_seen_at, registered_at, updated_at",
        )
        .bind(worker_id.as_str())
        .bind(state)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| RepositoryError::not_found("worker", worker_id.as_str()))?;
        row.into_status()
    }

    pub async fn list_workers(&self) -> RepositoryResult<Vec<WorkerStatus>> {
        sqlx::query_as::<_, WorkerRow>(
            "SELECT id, runtimes, kvm, gpu, arch, capacity, active_jobs, state, \
             last_heartbeat_at, reported_last_seen_at, registered_at, updated_at \
             FROM workers ORDER BY registered_at, id",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(WorkerRow::into_status)
        .collect()
    }

    pub async fn find_workers_matching(
        &self,
        profile: &RuntimeProfile,
        minimum: IsolationTier,
        heartbeat_cutoff: DateTime<Utc>,
        limit: u32,
    ) -> RepositoryResult<Vec<WorkerStatus>> {
        profile
            .validate()
            .map_err(|error| RepositoryError::Validation(error.to_string()))?;
        if !profile.isolation_tier.satisfies(minimum) {
            return Ok(Vec::new());
        }
        if !abi_matches_host(profile.abi, profile.host_arch) {
            return Ok(Vec::new());
        }
        let runtime = encode_enum(profile.runtime_kind)?;
        let arch = encode_enum(profile.host_arch)?;
        let needs_kvm = profile.isolation_tier == IsolationTier::VmIsolated
            || matches!(
                profile.runtime_kind,
                run_anywhere_contracts::RuntimeKind::AndroidEmulatorContainer
                    | run_anywhere_contracts::RuntimeKind::Cuttlefish
            );
        let limit = crate::models::checked_limit(limit)?;
        sqlx::query_as::<_, WorkerRow>(
            "SELECT id, runtimes, kvm, gpu, arch, capacity, active_jobs, state, \
             last_heartbeat_at, reported_last_seen_at, registered_at, updated_at \
             FROM workers WHERE state = 'online' AND active_jobs < capacity \
             AND last_heartbeat_at >= $1 AND $2 = ANY(runtimes) AND arch = $3 \
             AND (NOT $4 OR kvm) \
             ORDER BY active_jobs ASC, last_heartbeat_at DESC, id LIMIT $5",
        )
        .bind(heartbeat_cutoff)
        .bind(runtime)
        .bind(arch)
        .bind(needs_kvm)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(WorkerRow::into_status)
        .collect()
    }
}

fn validate_runtimes(
    runtimes: Vec<run_anywhere_contracts::RuntimeKind>,
) -> RepositoryResult<Vec<String>> {
    if runtimes.is_empty() {
        return Err(RepositoryError::Validation(
            "worker must advertise at least one runtime".to_owned(),
        ));
    }
    let encoded = runtimes
        .into_iter()
        .map(encode_enum)
        .collect::<RepositoryResult<Vec<_>>>()?;
    let unique = encoded.iter().collect::<HashSet<_>>();
    if unique.len() != encoded.len() {
        return Err(RepositoryError::Validation(
            "worker runtimes must be unique".to_owned(),
        ));
    }
    Ok(encoded)
}

const fn abi_matches_host(abi: run_anywhere_contracts::AndroidAbi, host: HostArch) -> bool {
    use run_anywhere_contracts::AndroidAbi;
    matches!(
        (abi, host),
        (AndroidAbi::X86 | AndroidAbi::X86_64, HostArch::X86_64)
            | (
                AndroidAbi::ArmeabiV7a | AndroidAbi::Arm64V8a,
                HostArch::Aarch64
            )
    )
}
