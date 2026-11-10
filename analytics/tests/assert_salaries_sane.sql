-- Singular test: catch implausible annual USD salaries (e.g. typed "...–1M" ceilings).
-- Known scraper/employer typos (27 postings at $1M) are surfaced with severity='warn'
-- so the pipeline alerts without blocking nightly production builds.
{{ config(severity='warn') }}

SELECT
    job_id,
    company_name,
    job_title,
    salary_min,
    salary_max,
    salary_avg,
    salary_period,
    salary_currency
FROM {{ ref('fct_job_postings') }}
WHERE salary_currency = 'USD'
  AND salary_period = 'year'
  AND (
      salary_max >= 1000000
      OR salary_min < 15000
      OR salary_min > salary_max
  )
