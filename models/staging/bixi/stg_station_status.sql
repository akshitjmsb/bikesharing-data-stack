-- models/staging/bixi/stg_station_status.sql

with

source as (

    -- select all columns from the source table
    select
        station_id,
        num_bikes_available,
        num_ebikes_available,
        num_bikes_disabled,
        num_docks_available,
        num_docks_disabled,
        is_installed,
        is_renting,
        is_returning,
        last_reported,
        _airbyte_extracted_at
    from {{ source('raw', 'station_status') }}

),

renamed as (

    select
        -- primary key
        station_id::string as station_id,

        -- station status metrics
        num_bikes_available::integer as bikes_available,
        num_ebikes_available::integer as ebikes_available,
        num_bikes_disabled::integer as bikes_disabled,
        num_docks_available::integer as docks_available,
        num_docks_disabled::integer as docks_disabled,

        -- station status booleans
        iff(is_installed = 1, true, false) as is_installed,
        iff(is_renting = 1, true, false) as is_renting,
        iff(is_returning = 1, true, false) as is_returning,

        -- timestamps converted to Montreal's timezone
        convert_timezone('UTC', 'America/Montreal', to_timestamp(last_reported::integer)) as last_reported_at,
        convert_timezone('UTC', 'America/Montreal', _airbyte_extracted_at::timestamp_ntz) as extracted_at

    from source

)

select * from renamed
