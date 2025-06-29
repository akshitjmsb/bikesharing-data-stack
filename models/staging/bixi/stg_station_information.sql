-- models/staging/bixi/stg_station_information.sql

with source as (
    select * 
    from {{ source('raw', 'station_information') }}
),

renamed as (
    select
        station_id::string as station_id,
        name::string as station_name,
        short_name::string as station_short_name,
        lat::float as latitude,
        lon::float as longitude,
        capacity::int as capacity,
        has_kiosk::boolean as has_kiosk,
        is_charging::boolean as is_charging,
        eightd_has_key_dispenser::boolean as has_key_dispenser,
        electric_bike_surcharge_waiver::boolean as has_electric_bike_waiver,
        _airbyte_extracted_at::timestamp_ntz as extracted_at
    from source
)

select * from renamed
