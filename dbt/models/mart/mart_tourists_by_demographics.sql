{{
    config(
        materialized='table',
        partition_by={
            'field': 'period_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['country_code', 'sex_code', 'age_group_code']
    )
}}

-- Dashboard tile 1 (categorical): tourist distribution by sex and age group
-- Dashboard tile 2 (temporal): tourist trend over time by demographic segment

with base as (
    select
        period_date,
        year,
        quarter,
        territory_code,
        territory_name,
        country_code,
        country_name,
        sex_code,
        sex,
        age_group_code,
        age_group,
        sum(tourists) as total_tourists
    from {{ ref('stg_tourist_age_sex') }}
    -- Exclude totals to avoid double counting
    where sex_code != '_T'
      and age_group_code != '_T'
      and country_code != '_T'
      and period_date is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

select * from base
