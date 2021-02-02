CREATE TABLE IF NOT EXISTS pickup (
	 task_id            TEXT        NOT NULL CONSTRAINT task_id    CHECK (char_length(task_id)    = 20)
	,vehicle_id         TEXT        NOT NULL CONSTRAINT vehicle_id CHECK (char_length(vehicle_id) = 20)
	,qr_code            TEXT        NOT NULL CONSTRAINT qr_code    CHECK (char_length(qr_code)    =  6)
	,time_task_created  TIMESTAMPTZ NOT NULL
	,time_task_resolved TIMESTAMPTZ NOT NULL
);
