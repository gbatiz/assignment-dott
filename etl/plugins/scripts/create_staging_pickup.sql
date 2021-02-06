CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.pickup CASCADE;
CREATE TABLE staging.pickup (
	 task_id            TEXT
	,vehicle_id         TEXT
	,qr_code            TEXT
	,time_task_created  TEXT
	,time_task_resolved TEXT
);
