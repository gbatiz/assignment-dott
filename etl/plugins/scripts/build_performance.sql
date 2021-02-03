BEGIN;

DROP TABLE IF EXISTS public.performance;
CREATE TABLE public.performance AS


WITH last_cycle_rides AS (
-- This table contains all rides per vehicle since their last deployment,
-- excluding any rides outside a cycle (incomplete data), and adding a revenue rank
-- for filtering the most profitable 5 rides later, with recency as tiebreaker.
        SELECT ride.*
              ,ROW_NUMBER() OVER (PARTITION BY ride.vehicle_id ORDER BY ride.gross_amount DESC, time_ride_start DESC) AS revenue_rank
          FROM public.ride AS ride
    -- Get rid of rides not in the last cycle
    INNER JOIN (
                  SELECT vehicle_id
                        ,MAX(time_task_resolved) AS ts
                    FROM public.deployment
                GROUP BY vehicle_id
               ) AS last_deploy
               ON ride.vehicle_id = last_deploy.vehicle_id
               AND ride.time_ride_start > last_deploy.ts
    -- Get rid of rides outside a cycle, due to incomplete data
    INNER JOIN (
                  SELECT vehicle_id
                        ,MAX(time_task_created) AS ts
                    FROM public.pickup AS last_pickup
                   GROUP BY vehicle_id
               ) AS last_pickup
               ON ride.vehicle_id = last_pickup.vehicle_id
               AND (
                       ride.time_ride_start < last_pickup.ts
                       OR
                       last_pickup.ts < last_deploy.ts
                   )


), last_deployments AS (
-- This table contains all deployments per vehicle
-- with a recency column for filtering the last 5 later.
    SELECT vehicle_id
          ,task_id
          ,time_task_created AS ts
          ,ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY time_task_created DESC) AS recency
      FROM public.deployment


), last_rides_and_deployments AS (
-- This table is a union of the last 5 rides
-- and the last 5 deployments per vehicle,
-- sorted by vehicle and time.
    (SELECT ts
          ,vehicle_id
          ,'deployment' AS "event"
          ,task_id
          ,NULL AS ride_id
          ,NULL AS revenue
          ,NULL AS distance_meters
          ,NULL AS start_lat
          ,NULL AS start_lng
          ,NULL AS end_lat
          ,NULL AS end_lng
      FROM last_deployments
     WHERE recency <= 5)

    UNION ALL

    (SELECT time_ride_start AS ts
          ,vehicle_id
          ,'ride'  AS "event"
          ,NULL AS task_id
          ,ride_id
          ,gross_amount AS revenue
          ,distance_meters
          ,start_lat
          ,start_lng
          ,end_lat
          ,end_lng
      FROM last_cycle_rides
     WHERE revenue_rank <= 5)

     ORDER BY vehicle_id, ts DESC

), last_rides_and_deployments_with_recency AS (
-- This table adds a recency column to filter for only 5 rows per vehicle later
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY ts DESC) AS recency
      FROM last_rides_and_deployments


), reshaped AS (
-- Drop unneeded timestamp column, reshape table to have a vehicle_id to for lookups,
-- and a json string containing the API-response
      SELECT vehicle_id AS lookup
            ,CAST(ARRAY_TO_JSON(ARRAY_AGG(JSON_STRIP_NULLS(ROW_TO_JSON(performance)))) AS text) AS response
        FROM (
              -- Just need to drop the timestamp column
              SELECT vehicle_id
                    ,"event"
                    ,task_id
                    ,ride_id
                    ,revenue
                    ,distance_meters
                    ,start_lat
                    ,start_lng
                    ,end_lat
                    ,end_lng
                FROM last_rides_and_deployments_with_recency
               WHERE recency <= 5
             ) AS performance
    GROUP BY vehicle_id
)


    SELECT *
      FROM reshaped

UNION ALL

        SELECT p.qr_code AS lookup
              ,r.response
          FROM reshaped AS r
    INNER JOIN (
                SELECT DISTINCT vehicle_id
                               ,qr_code
                           FROM public.pickup
               ) AS p
               ON r.lookup = p.vehicle_id;


CREATE INDEX idx_lookup ON public.performance ( lookup );

COMMIT;
