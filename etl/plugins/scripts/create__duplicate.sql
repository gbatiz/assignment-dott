-- DROP TABLE IF EXISTS duplicate;
CREATE TABLE IF NOT EXISTS duplicate (
	 id                  text NOT NULL
	,table_name          text NOT NULL
	,airflow_task_run_id text NOT NULL
);
