{{
    config(
        materialized='table',
        partition_by={
            'field': 'period_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['country_code', 'accommodation_type_code']
    )
}}

-- Dashboard tile 1 (categorical): tourists by accommodation type and origin country
-- Dashboard tile 2 (temporal): total tourists over time

with base as (
    select
        period_date,
        year,
        quarter,
        territory_code,
        territory_name,
        country_code,
        country_name,
        accommodation_type_code,
        accommodation_type,
        accommodation_category,
        sum(tourists) as total_tourists
    from {{ ref('stg_tourist_accommodations') }}
    -- Exclude totals to avoid double counting
    where accommodation_type_code != '_T'
      and country_code != '_T'
      and period_date is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select * from base
