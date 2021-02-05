CREATE OR REPLACE VIEW public.performance_for_bi AS
WITH events AS (
SELECT vehicle_id
      ,'ride'          AS event_type
      ,ride_id         AS event_id
      ,NULL            AS last_deployment_id
      ,time_ride_start AS ts
      ,start_lat
      ,start_lng
      ,end_lat
      ,end_lng
      ,gross_amount
      ,EXTRACT(epoch FROM (time_ride_end - time_ride_start)) / 3600 AS duration_hours
      ,distance_meters
  FROM ride

UNION ALL

SELECT vehicle_id
      ,'deployment'       AS event_type
      ,task_id            AS event_id
      ,task_id            AS last_deployment_id
      ,time_task_resolved AS ts
      ,NULL               AS start_lat
      ,NULL               AS start_lng
      ,NULL               AS end_lat
      ,NULL               AS end_lng
      ,NULL               AS gross_amount
      ,NULL               AS duration_hours
      ,NULL               AS distance_meters
  FROM deployment
)
  SELECT vehicle_id
        ,event_type
        ,event_id
        ,FIRST_VALUE(last_deployment_id) OVER (PARTITION BY vehicle_id, value_partition ORDER BY ts ) AS last_deployment_id
        ,ts
        ,start_lat
        ,start_lng
        ,end_lat
        ,end_lng
        ,gross_amount
        ,duration_hours
        ,ROUND(distance_meters) AS distance_meters
    FROM (
            SELECT SUM(CASE WHEN last_deployment_id IS NULL THEN 0 ELSE 1 END)
                    OVER
                    (PARTITION BY vehicle_id ORDER BY ts) AS value_partition
                  ,*
              FROM events
          ORDER BY vehicle_id, ts
         ) AS x
ORDER BY vehicle_id, ts DESC
