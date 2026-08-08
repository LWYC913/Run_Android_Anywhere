DROP INDEX IF EXISTS jobs_project_active_lease_idx;
DROP INDEX IF EXISTS jobs_project_outstanding_idx;

ALTER TABLE projects
    DROP CONSTRAINT IF EXISTS projects_job_quotas_ordered,
    DROP CONSTRAINT IF EXISTS projects_max_outstanding_jobs_range,
    DROP CONSTRAINT IF EXISTS projects_max_concurrent_jobs_range,
    DROP COLUMN IF EXISTS max_outstanding_jobs,
    DROP COLUMN IF EXISTS max_concurrent_jobs;
