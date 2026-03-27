with source as (
    select * from {{ source('raw', 'tourist_age_sex') }}
),

renamed as (
    select
        territorio_code                         as territory_code,
        territorio                              as territory_name,
        pais_residencia_code                    as country_code,
        pais_residencia                         as country_name,
        sexo_code                               as sex_code,
        sexo                                    as sex,
        edad_code                               as age_group_code,
        edad                                    as age_group,
        time_period_code,
        cast(period_date as date)               as period_date,
        extract(year  from period_date)         as year,
        cast(quarter as int64)                  as quarter,
        cast(obs_value as int64)                as tourists
    from source
    where obs_value is not null
)

select * from renamed
