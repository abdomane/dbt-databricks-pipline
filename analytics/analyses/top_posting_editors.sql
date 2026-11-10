-- Which employers edit their job postings the most after initial publication?
-- Counts distinct jobs with >1 SCD2 version in job_postings_snapshot.

WITH edited_jobs AS (
    SELECT
        company_name,
        job_id,
        COUNT(*) AS version_count
    FROM {{ ref('job_postings_snapshot') }}
    GROUP BY company_name, job_id
    HAVING COUNT(*) > 1
)

SELECT
    company_name,
    COUNT(DISTINCT job_id) AS edited_postings,
    SUM(version_count - 1) AS total_edits,
     MAX(version_count) AS max_versions_for_single_job
FROM edited_jobs
GROUP BY company_name
ORDER BY edited_postings DESC
LIMIT 20
