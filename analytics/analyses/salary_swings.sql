-- Track postings whose advertised salary changed between scrapes in the SCD2 snapshot.
-- Surfaces large jumps or mid-edit typos (e.g. $60K -> $1,000,000).

WITH salary_versions AS (
    SELECT
        job_id,
        company_name,
        job_title,
        salary_avg,
        salary_min,
        salary_max,
        salary_period,
        salary_currency,
        dbt_valid_from,
        dbt_valid_to,
        LAG(salary_avg) OVER (
            PARTITION BY job_id
            ORDER BY dbt_valid_from
        ) AS prev_salary_avg
    FROM {{ ref('job_postings_snapshot') }}
    WHERE salary_avg IS NOT NULL
)

SELECT
    job_id,
    company_name,
    job_title,
    prev_salary_avg,
    salary_avg AS new_salary_avg,
    salary_avg - prev_salary_avg AS salary_diff,
    ROUND((salary_avg - prev_salary_avg) / NULLIF(prev_salary_avg, 0) * 100.0, 1) AS pct_change,
    salary_period,
    salary_currency,
    dbt_valid_from AS changed_at
FROM salary_versions
WHERE prev_salary_avg IS NOT NULL
  AND salary_avg != prev_salary_avg
ORDER BY ABS(salary_avg - prev_salary_avg) DESC
