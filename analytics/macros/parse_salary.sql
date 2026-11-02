{% macro parse_salary(salary_column, part='min') -%}
    {%- if part == 'min' -%}
        {%- set segment = "split(" ~ salary_column ~ ", '–')[0]" -%}
    {%- else -%}
        {%- set segment = "element_at(split(" ~ salary_column ~ ", '–'), -1)" -%}
    {%- endif -%}
    TRY_CAST(REPLACE(REGEXP_EXTRACT({{ segment }}, '([0-9][0-9,.]*)', 1), ',', '') AS DECIMAL(15, 2))
        * CASE UPPER(REGEXP_EXTRACT({{ segment }}, '[0-9][0-9,.]*([KkMm])', 1))
            WHEN 'K' THEN 1000
            WHEN 'M' THEN 1000000
            ELSE 1
          END
{%- endmacro %}