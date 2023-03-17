{{ config(materialized='view') }}

SELECT 
    dispatching_base_num,

    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropOff_datetime AS timestamp) AS dropoff_datetime,

    CAST(PUlocationID as integer) as  pickup_locationid,
    CAST(DOlocationID as integer) as dropoff_locationid,

    SR_Flag
FROM {{ source('staging','fhv_tripdata') }}
    