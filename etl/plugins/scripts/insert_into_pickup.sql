BEGIN;

-- Log duplicates
INSERT INTO public.duplicate
    SELECT s.task_id      AS id
          ,'pickup'       AS table_name
          ,'{{ ti }}'     AS airflow_task_run_id
      FROM staging.pickup AS s
INNER JOIN public.pickup AS p
        ON s.task_id = p.task_id
     WHERE s.task_id != 'task_id';

-- Release batch
INSERT INTO public.pickup
SELECT task_id
      ,vehicle_id
      ,qr_code
      ,CAST(time_task_created AS timestamptz)  AS time_task_created
      ,CAST(time_task_resolved AS timestamptz) AS time_task_resolved
  FROM staging.pickup
 WHERE task_id != 'task_id'
ON CONFLICT ON CONSTRAINT pickup_pkey DO NOTHING;

COMMIT;
