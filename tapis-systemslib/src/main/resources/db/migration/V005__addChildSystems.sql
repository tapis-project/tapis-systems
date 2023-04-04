ALTER TABLE systems ADD COLUMN IF NOT EXISTS parent_id TEXT;
ALTER TABLE systems ADD COLUMN IF NOT EXISTS allow_children BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS sys_system_parent_id_idx ON systems (parent_id);