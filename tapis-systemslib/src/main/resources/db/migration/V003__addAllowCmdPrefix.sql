ALTER TABLE systems ADD COLUMN IF NOT EXISTS enable_cmd_prefix BOOLEAN NOT NULL DEFAULT FALSE;