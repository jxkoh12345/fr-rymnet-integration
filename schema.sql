-- hik_record_status needs a `status` column to hold SUCCESS | PENDING | FAILED.
-- The tables already exist; this only adds the missing column db.py writes to.
ALTER TABLE hik_record_status
    ADD COLUMN IF NOT EXISTS status varchar(20);

-- Optional: set_status() filters by hik_record_id on every update.
CREATE INDEX IF NOT EXISTS idx_hik_record_status_record_id
    ON hik_record_status (hik_record_id);
