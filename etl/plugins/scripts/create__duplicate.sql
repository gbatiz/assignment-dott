DROP TABLE IF EXISTS public.duplicate CASCADE;
CREATE TABLE IF NOT EXISTS public.duplicate (
	 id                  text NOT NULL
	,table_name          text NOT NULL
	,airflow_task_run_id text NOT NULL
);
