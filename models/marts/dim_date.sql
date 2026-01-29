{{
    config(
        materialized='external',
        format='csv',
        location="data/output/dim_date.csv",
        options={
            "overwrite": True
        }
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
)

select
    date_day,
    date_part('year', date_day) as year,
    date_part('month', date_day) as month,
    date_part('dow', date_day) in (0, 6) as is_weekend

from date_spine
