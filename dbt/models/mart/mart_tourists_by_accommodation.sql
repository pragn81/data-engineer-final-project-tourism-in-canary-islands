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
        sum(tourists) as total_tourists
    from {{ ref('stg_tourist_accommodations') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from base
