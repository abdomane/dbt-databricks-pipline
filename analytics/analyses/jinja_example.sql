    {% set roles = dbt_utils.get_column_values(table=ref('stg_job_postings'), column='search_term') %}
    {% for role in roles -%}
    SELECT 
        '{{ role }}' AS role,
        count(*) AS postings
    FROM {{ref('stg_job_postings')}}
    WHERE search_term = '{{ role }}'
    {{"UNION ALL" if not loop.last else ""}} 
    {% endfor %}