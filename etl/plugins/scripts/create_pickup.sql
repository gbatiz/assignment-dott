-- DROP TABLE IF EXISTS pickup;
CREATE TABLE IF NOT EXISTS pickup (
	 task_id            TEXT        PRIMARY KEY CONSTRAINT len_task_id    CHECK (char_length(task_id)    = 20)
	,vehicle_id         TEXT        NOT NULL    CONSTRAINT len_vehicle_id CHECK (char_length(vehicle_id) = 20)
	,qr_code            TEXT        NOT NULL    CONSTRAINT len_qr_code    CHECK (char_length(qr_code)    =  6)
	,time_task_created  TIMESTAMPTZ NOT NULL
	,time_task_resolved TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_pickup_time ON pickup ( time_task_created );
