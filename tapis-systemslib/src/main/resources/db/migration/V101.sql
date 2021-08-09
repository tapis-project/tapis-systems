-- ##############################################################
-- Migration: V101
-- This migration:
--   - Adds table for Scheduler Profiles
--
-- ##############################################################
--
ALTER ROLE tapis_sys SET search_path = 'tapis_sys';
SET search_path TO tapis_sys;
-- ----------------------------------------------------------------------------------------
--                                     SCHEDULER PROFILES
-- ----------------------------------------------------------------------------------------
-- Scheduler Profiles table
CREATE TABLE IF NOT EXISTS scheduler_profiles
(
  tenant      TEXT NOT NULL,
  name        TEXT NOT NULL,
  description TEXT,
  owner       TEXT NOT NULL,
  module_load_command TEXT NOT NULL,
  modules_to_load TEXT[],
  hidden_options TEXT[],
  uuid uuid NOT NULL,
  created    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  UNIQUE (tenant, name)
);
ALTER TABLE scheduler_profiles OWNER TO tapis_sys;
CREATE INDEX IF NOT EXISTS schedprof_tenant_name_idx ON scheduler_profiles (tenant, name);
COMMENT ON COLUMN scheduler_profiles.tenant IS 'Tenant name associated with the profile';
COMMENT ON COLUMN scheduler_profiles.name IS 'Unique name for the profile';
COMMENT ON COLUMN scheduler_profiles.description IS 'Profile description';
COMMENT ON COLUMN scheduler_profiles.owner IS 'User name of system owner';
COMMENT ON COLUMN scheduler_profiles.module_load_command IS 'Command to load software library modules.';
COMMENT ON COLUMN scheduler_profiles.modules_to_load IS 'Software library modules that should be loaded for each job.';
COMMENT ON COLUMN scheduler_profiles.hidden_options IS 'Scheduler options that are subsumed by TAPIS.';
COMMENT ON COLUMN scheduler_profiles.created IS 'UTC time for when record was created';
COMMENT ON COLUMN scheduler_profiles.updated IS 'UTC time for when record was last updated';
