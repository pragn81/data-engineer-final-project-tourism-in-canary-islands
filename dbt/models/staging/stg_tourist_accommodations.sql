with source as (
    select * from {{ source('raw', 'tourist_accommodations') }}
),

renamed as (
    select
        territorio_code                         as territory_code,
        territorio                              as territory_name,
        pais_residencia_code                    as country_code,
        pais_residencia                         as country_name,
        tipo_alojamiento_code                   as accommodation_type_code,
        tipo_alojamiento                        as accommodation_type,
        time_period_code,
        cast(period_date as date)               as period_date,
        extract(year  from period_date)         as year,
        cast(quarter as int64)                  as quarter,
        cast(obs_value as int64)                as tourists
    from source
    where obs_value is not null
)

select * from renamed
