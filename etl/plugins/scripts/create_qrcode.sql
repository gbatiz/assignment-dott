DROP TABLE IF EXISTS public.qr_code CASCADE;
CREATE TABLE IF NOT EXISTS public.qr_code (
	 qr_code    TEXT PRIMARY KEY CONSTRAINT len_qr_code    CHECK (char_length(qr_code)    =  6)
	,vehicle_id TEXT NOT NULL    CONSTRAINT len_vehicle_id CHECK (char_length(vehicle_id) = 20)

);
CREATE INDEX idx_vehicle_id ON public.qr_code ( vehicle_id );
