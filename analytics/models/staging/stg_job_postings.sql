{% set extension_keyword = [
        "No degree mentioned", 
        "Paid time off", 
        "Health insurance", 
        "Dental insurance"] 
%}
with source AS
    (
        SELECT *
        FROM {{ source('jobs', 'raw_job_postings') }}   
        WHERE  ERROR IS NOT TRUE
    ),
parsed AS
    (
        SELECT 
            *,
            FROM_JSON(job_extensions_raw, 'ARRAY<STRING>') AS job_extensions
        FROM source
    ),
cleaned AS
    (
        SELECT 
            job_id,
            job_title,
            TRIM(company_name) AS company_name,
            job_location,
            REGEXP_REPLACE(job_via, '^via', '') AS job_via,
            job_posted_at,
            job_salary,
            job_schedule_type,
            job_work_from_home,
            search_time AS searched_at,
            search_date,
            search_term,
            search_location,
            {%for keyword in extension_keyword -%}
            array_contains(
                job_extensions,
                '{{ keyword }}'
            ) AS has_{{ keyword | replace(" ", "_") | lower }}
            {{- ',' if not loop.last }}
            {% endfor -%}
        FROM parsed
    )
SELECT *
FROM cleaned