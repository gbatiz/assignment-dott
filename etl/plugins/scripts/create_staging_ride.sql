CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.ride CASCADE;
CREATE TABLE staging.ride (
	 ride_id         TEXT
	,vehicle_id      TEXT
	,time_ride_start TEXT
	,time_ride_end   TEXT
	,start_lat       TEXT
	,start_lng       TEXT
	,end_lat         TEXT
	,end_lng         TEXT
	,gross_amount    TEXT
);
