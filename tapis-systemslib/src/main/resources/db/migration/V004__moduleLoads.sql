-- ----------------------------------------------------------------------------------------
--  Create new table to support multiple module load entries associated with a scheduler profile.
--  Create new table, copy data to new table, drop data from old table.
-- ----------------------------------------------------------------------------------------

-- Add a serial primary key to main table so we can create the foreign key in the aux table
ALTER TABLE scheduler_profiles ADD COLUMN IF NOT EXISTS seq_id SERIAL PRIMARY KEY;

-- Add aux table for module load spec entries
CREATE TABLE IF NOT EXISTS sched_profile_mod_load
(
    seq_id SERIAL PRIMARY KEY,
    sched_profile_seq_id INTEGER REFERENCES scheduler_profiles(seq_id) ON DELETE CASCADE,
    sched_profile_name TEXT,
    tenant TEXT NOT NULL,
    module_load_command TEXT NOT NULL,
    modules_to_load TEXT[],
    UNIQUE (tenant, sched_profile_name)
);
ALTER TABLE sched_profile_mod_load OWNER TO tapis_sys;
--
-- Copy data from scheduler_profiles table to new table
--
INSERT INTO sched_profile_mod_load (sched_profile_seq_id, sched_profile_name, tenant, module_load_command, modules_to_load)
SELECT s.seq_id, s.name, s.tenant, s.module_load_command, s.modules_to_load
    FROM scheduler_profiles as s;
--
-- Drop columns from scheduler_profiles table
--
ALTER TABLE scheduler_profiles DROP COLUMN IF EXISTS module_load_command;
ALTER TABLE scheduler_profiles DROP COLUMN IF EXISTS modules_to_load;

