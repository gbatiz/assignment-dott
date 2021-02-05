BEGIN;

INSERT INTO public.qr_code
SELECT DISTINCT qr_code
               ,vehicle_id
  FROM staging.pickup
 WHERE task_id != 'task_id'
ON CONFLICT ON CONSTRAINT qr_code_pkey DO NOTHING;

COMMIT;
