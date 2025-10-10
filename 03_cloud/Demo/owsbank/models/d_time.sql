{{ config(
    materialized='table',
    tags=['dim']
)}}

{% set date_parts = ['year', 'month', 'week', 'day', 'DOW'] %}

with cleaned_d_time as(
	select
		time_id,
        {% if target.type == 'postgres' -%}
            action_timestamp::timestamp AS action_timestamp
        {% elif target.type == 'bigquery' -%}
		    timestamp(action_timestamp)  AS action_timestamp
        {%- else -%}
            action_timestamp AS action_timestamp
        {%- endif -%}
	from {{ source('mysql_raw', 'd_time') }} 
)

select
  time_id,
  action_timestamp,
  {% for part in date_parts -%}
    EXTRACT({{ part | upper }} FROM action_timestamp) as action_{{part}},
  {% endfor -%}
  {% if target.type == 'postgres' %}
    TRIM(TO_CHAR(action_timestamp, 'Day')) as action_weekday
  {%- else -%}
    to_char(action_timestamp, 'Day') as action_weekday
  {% endif %}
FROM cleaned_d_time
{{ limit_lines_dev(environment='prod') }}