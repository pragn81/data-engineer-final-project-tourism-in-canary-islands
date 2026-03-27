with source as (
    select * from {{ source('raw', 'tourist_revenue') }}
),

renamed as (
    select
        territorio_code                         as territory_code,
        territorio                              as territory_name,
        pais_residencia_code                    as country_code,
        pais_residencia                         as country_name,
        nivel_ingresos_code                     as income_level_code,
        nivel_ingresos                          as income_level,
        time_period_code,
        cast(period_date as date)               as period_date,
        extract(year  from period_date)         as year,
        cast(quarter as int64)                  as quarter,
        cast(obs_value as int64)                as tourists
    from source
    where obs_value is not null
)

select * from renamed
