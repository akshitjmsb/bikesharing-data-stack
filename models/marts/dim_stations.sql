-- models/marts/dim_stations.sql

-- This config block tells dbt to build this model as a physical 'table'.
-- Tables are much faster for dashboards and analysts to query.
{{
  config(
    materialized = 'table'
  )
}}

with

station_info as (

    -- The ref() function is how we use our staging models as building blocks.
    select * from {{ ref('stg_station_information') }}

),

station_status as (

    select * from {{ ref('stg_station_status') }}

),

final as (

    select
        -- All columns from the station_information staging model
        info.station_id,
        info.station_name,
        info.station_short_name,
        info.latitude,
        info.longitude,
        info.capacity,
        info.has_kiosk,
        info.is_charging,
        info.has_key_dispenser,
        info.has_electric_bike_waiver,

        -- All columns from the station_status staging model
        status.bikes_available,
        status.ebikes_available,
        status.bikes_disabled,
        status.docks_available,
        status.docks_disabled,
        status.is_installed,
        status.is_renting,
        status.is_returning,
        status.last_reported_at,
        
        -- We'll take the timestamp from the status table as it's the most frequently updated.
        status.extracted_at

    from station_info as info
    
    -- We join the two tables together using their common column, station_id.
    -- A left join ensures we keep all stations, even if they don't have a status report.
    left join station_status as status
        on info.station_id = status.station_id

)

select * from final
