DROP TABLE IF EXISTS staging.deployment CASCADE;
CREATE TABLE staging.deployment (
	 task_id            TEXT
	,vehicle_id         TEXT
	,time_task_created  TEXT
	,time_task_resolved TEXT
);
