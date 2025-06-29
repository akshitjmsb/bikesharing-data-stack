-- models/staging/bixi/stg_station_status.sql

with source as (

    select * 
    from {{ source('raw', 'station_status') }}

),

renamed as (

    select
        -- IDs
        station_id::string                                               as station_id,

        -- Availability counts
        num_bikes_available::int                                         as bikes_available,
        num_docks_available::int                                         as docks_available,
        num_bikes_disabled::int                                          as bikes_disabled,
        num_docks_disabled::int                                          as docks_disabled,
        num_ebikes_available::int                                        as ebikes_available,

        -- Boolean flags (0 / 1 in raw → BOOLEAN here)
        case when is_installed  = 1 then true else false end             as is_installed,
        case when is_renting    = 1 then true else false end             as is_renting,
        case when is_returning  = 1 then true else false end             as is_returning,
        is_charging                                                  as is_charging,   -- already BOOLEAN
        eightd_has_available_keys                                   as has_available_keys,

        -- Timestamps
        last_reported::int                                             as last_reported_epoch,
        to_timestamp_ntz(last_reported::int)                           as last_reported_at,
        _airbyte_extracted_at::timestamp_ntz                           as extracted_at

    from source
)

select * from renamed;
