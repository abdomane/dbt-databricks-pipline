    {% set roles = ['data analyst', 'data engineer', 'data scientist'] %}
    {% for role in roles -%}
    SELECT 
        search_term AS role,
        count(*) AS postings
    FROM {{ref('stg_job_postings')}}
    WHERE search_term = '{{ role | title }}'  
    GROUP BY search_term  
    {{"UNION ALL" if not loop.last else ""}} 
    {% endfor %}