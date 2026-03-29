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
        coalesce(
            edad,
            case edad_code
                when 'Y_GE44' then 'From 44 years and over'
                when 'Y16T44' then 'From 16 to 44 years'
                when '_T'     then 'Total'
                else edad_code
            end
        )                                       as age_group,
        time_period_code,
        cast(period_date as date)               as period_date,
        extract(year  from period_date)         as year,
        cast(quarter as int64)                  as quarter,
        cast(obs_value as int64)                as tourists
    from source
    where obs_value is not null
      and sexo_code  != '_T'
      and edad_code  != '_T'
)

select * from renamed
