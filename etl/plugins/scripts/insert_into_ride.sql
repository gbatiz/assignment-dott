CREATE EXTENSION IF NOT EXISTS earthdistance CASCADE;

BEGIN;

-- Log duplicates
INSERT INTO public.duplicate
    SELECT s.ride_id      AS id
          ,'ride'         AS table_name
          ,'{{ ti }}'     AS airflow_task_run_id
      FROM staging.ride AS s
INNER JOIN public.ride AS p
        ON s.ride_id = p.ride_id
     WHERE s.ride_id != 'ride_id';

-- Release batch
WITH noheader_typecasted AS (
SELECT ride_id
      ,vehicle_id
      ,CAST(time_ride_start AS timestamptz)  AS time_ride_start
      ,CAST(time_ride_end   AS timestamptz)  AS time_ride_end
      ,NULLIF(CAST(start_lat AS decimal), 0) AS start_lat
      ,NULLIF(CAST(start_lng AS decimal), 0) AS start_lng
      ,NULLIF(CAST(end_lat   AS decimal), 0) AS end_lat
      ,NULLIF(CAST(end_lng   AS decimal), 0) AS end_lng
      ,CAST(gross_amount    AS decimal)      AS gross_amount
  FROM staging.ride
 WHERE ride_id != 'ride_id'
)
INSERT INTO public.ride
SELECT *
      ,EARTH_DISTANCE(
          LL_TO_EARTH(start_lat, start_lng)
         ,LL_TO_EARTH(end_lat, end_lng)
       )                                     AS distance_meters
FROM noheader_typecasted
ON CONFLICT ON CONSTRAINT ride_pkey DO NOTHING;

COMMIT;
