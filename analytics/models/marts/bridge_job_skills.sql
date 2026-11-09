WITH job_skills AS (
    SELECT * FROM {{ ref('stg_job_skills') }}
),

job_postings AS (
    SELECT job_id FROM {{ ref('fct_job_postings') }}
)

-- inner join: only jobs present in the fact table, so referential
-- integrity to fct_job_postings holds by construction
SELECT
    job_skills.job_id,
    job_skills.skill_id,
    job_skills.skill_keyword
FROM job_skills
INNER JOIN job_postings
    ON job_skills.job_id = job_postings.job_id
