DROP TABLE IF EXISTS public.ride;
CREATE TABLE IF NOT EXISTS public.ride (
	 ride_id         TEXT        PRIMARY KEY CONSTRAINT ride_id    CHECK (char_length(ride_id)    = 20)
	,vehicle_id      TEXT        NOT NULL    CONSTRAINT vehicle_id CHECK (char_length(vehicle_id) = 20)
	,time_ride_start TIMESTAMPTZ NOT NULL
	,time_ride_end   TIMESTAMPTZ NOT NULL
	,start_lat       DECIMAL     NOT NULL
	,start_lng       DECIMAL     NOT NULL
	,end_lat         DECIMAL     NOT NULL
	,end_lng         DECIMAL     NOT NULL
	,gross_amount    DECIMAL     NULL
	,distance_meters DECIMAL     NULL
);
CREATE INDEX idx_ridestart_time ON public.ride ( time_ride_start );
