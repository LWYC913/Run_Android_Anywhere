ALTER TABLE projects
    ADD COLUMN max_concurrent_jobs BIGINT NOT NULL DEFAULT 2,
    ADD COLUMN max_outstanding_jobs BIGINT NOT NULL DEFAULT 100,
    ADD CONSTRAINT projects_max_concurrent_jobs_range CHECK (
        max_concurrent_jobs BETWEEN 1 AND 4294967295
    ),
    ADD CONSTRAINT projects_max_outstanding_jobs_range CHECK (
        max_outstanding_jobs BETWEEN 1 AND 4294967295
    ),
    ADD CONSTRAINT projects_job_quotas_ordered CHECK (
        max_concurrent_jobs <= max_outstanding_jobs
    );

CREATE INDEX jobs_project_outstanding_idx
    ON jobs (project_id, created_at, id)
    WHERE state NOT IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed');

CREATE INDEX jobs_project_active_lease_idx
    ON jobs (project_id, lease_expires_at, id)
    WHERE lease_id IS NOT NULL
      AND state NOT IN ('passed', 'failed', 'cancelled', 'timed_out', 'infra_failed');
