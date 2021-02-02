-- DROP TABLE IF EXISTS deployment;
CREATE TABLE IF NOT EXISTS deployment (
	 task_id            TEXT        PRIMARY KEY CONSTRAINT len_task_id    CHECK (char_length(task_id)    = 20)
	,vehicle_id         TEXT        NOT NULL    CONSTRAINT len_vehicle_id CHECK (char_length(vehicle_id) = 20)
	,time_task_created  TIMESTAMPTZ NOT NULL
	,time_task_resolved TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_deployment_time ON deployment ( time_task_resolved );
