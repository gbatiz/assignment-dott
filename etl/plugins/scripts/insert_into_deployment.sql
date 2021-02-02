BEGIN;

-- Log duplicates
INSERT INTO public.duplicate
    SELECT s.task_id      AS id
          ,'deployment'   AS table_name
          ,'{{ ti }}'     AS airflow_task_run_id
      FROM staging.deployment AS s
INNER JOIN public.deployment AS p
        ON s.task_id = p.task_id
     WHERE s.task_id != 'task_id';

-- Release batch
INSERT INTO public.deployment
SELECT task_id
      ,vehicle_id
      ,CAST(time_task_created AS timestamptz)  AS time_task_created
      ,CAST(time_task_resolved AS timestamptz) AS time_task_resolved
  FROM staging.deployment
 WHERE task_id != 'task_id'
ON CONFLICT ON CONSTRAINT deployment_pkey DO NOTHING;

COMMIT;
