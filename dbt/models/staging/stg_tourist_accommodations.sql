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
        coalesce(
            tipo_alojamiento,
            case tipo_alojamiento_code
                when 'HOTEL_ESTRELLAS_5'                      then '5-star hotel'
                when 'HOTEL_ESTRELLAS_4'                      then '4-star hotel'
                when 'HOTEL_ESTRELLAS_S1'                     then '1-3 star hotel'
                when 'APARTAMENTO_VILLA'                      then 'Apartment / villa'
                when 'VIVIENDA_HABITACION_ALQUILADA_PARTICULAR' then 'Private rental room'
                when 'VIVIENDA_GRATUITA'                      then 'Free accommodation'
                when '_T'                                     then 'Total'
                when '_O'                                     then 'Other'
                else tipo_alojamiento_code
            end
        )                                       as accommodation_type,
        case tipo_alojamiento_code
            when 'HOTEL_ESTRELLAS_5'  then 'Hotel'
            when 'HOTEL_ESTRELLAS_4'  then 'Hotel'
            when 'HOTEL_ESTRELLAS_S1' then 'Hotel'
            when 'APARTAMENTO_VILLA'  then 'Apartment / villa'
            when 'VIVIENDA_HABITACION_ALQUILADA_PARTICULAR' then 'Private rental'
            when 'VIVIENDA_GRATUITA'  then 'Free accommodation'
            when '_O'                 then 'Other'
            else null
        end                                     as accommodation_category,
        time_period_code,
        cast(period_date as date)               as period_date,
        extract(year  from period_date)         as year,
        cast(quarter as int64)                  as quarter,
        cast(obs_value as int64)                as tourists
    from source
    where obs_value is not null
)

select * from renamed
