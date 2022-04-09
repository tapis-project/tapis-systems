-- Initial DB schema creation for Tapis Systems Service
-- postgres commands to create all tables, indices and other database artifacts.
-- Prerequisites:
-- Create DB named tapissysdb and user named tapis_sys
--   CREATE DATABASE tapissysdb ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8';
--   CREATE USER tapis_sys WITH ENCRYPTED PASSWORD '<password>'
--   GRANT ALL PRIVILEGES ON DATABASE tapissysdb TO tapis_sys;
-- Fast way to check for table:
--   SELECT to_regclass('tapis_sys.systems');
--
--
-- TIMEZONE Convention
----------------------
-- All tables in this application conform to the same timezone usage rule:
--
--   All dates, times and timestamps are stored as UTC WITHOUT TIMEZONE information.
--
-- All temporal values written to the database are required to be UTC, all temporal
-- values read from the database can be assumed to be UTC.

-- NOTES for jOOQ
--   When a POJO has a default constructor (which is needed for jersey's SelectableEntityFilteringFeature)
--     then column names must match POJO attributes (with convention an_attr -> anAttr)
--     in order for jOOQ to set the attribute during Record.into()
--     Possibly another option would be to create a custom mapper to be used by Record.into()
--
-- Create the schema and set the search path
CREATE SCHEMA IF NOT EXISTS tapis_sys AUTHORIZATION tapis_sys;
ALTER ROLE tapis_sys SET search_path = 'tapis_sys';
SET search_path TO tapis_sys;

-- Set permissions
-- GRANT CONNECT ON DATABASE tapissysdb TO tapis_sys;
-- GRANT USAGE ON SCHEMA tapis_sys TO tapis_sys;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tapis_sys TO tapis_sys;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tapis_sys TO tapis_sys;

-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Systems table
-- Basic system attributes
CREATE TABLE systems
(
  seq_id      SERIAL PRIMARY KEY,
  tenant      TEXT NOT NULL,
  id          TEXT NOT NULL,
  description TEXT,
  system_type TEXT NOT NULL,
  owner       TEXT NOT NULL,
  host        TEXT NOT NULL,
  enabled     BOOLEAN NOT NULL DEFAULT true,
  effective_user_id TEXT NOT NULL,
  default_authn_method  TEXT NOT NULL,
  bucket_name    TEXT,
  root_dir       TEXT,
  port       INTEGER NOT NULL DEFAULT -1,
  use_proxy  BOOLEAN NOT NULL DEFAULT false,
  proxy_host TEXT,
  proxy_port INTEGER NOT NULL DEFAULT -1,
  dtn_system_id TEXT,
  dtn_mount_point TEXT,
  dtn_mount_source_path TEXT,
  is_dtn   BOOLEAN NOT NULL DEFAULT false,
  can_exec   BOOLEAN NOT NULL DEFAULT false,
  can_run_batch BOOLEAN NOT NULL DEFAULT false,
  mpi_cmd TEXT,
  job_runtimes JSONB,
  job_working_dir TEXT,
  job_env_variables JSONB NOT NULL,
  job_max_jobs INTEGER NOT NULL DEFAULT -1,
  job_max_jobs_per_user INTEGER NOT NULL DEFAULT -1,
  batch_scheduler TEXT,
  batch_logical_queues JSONB NOT NULL,
  batch_default_logical_queue TEXT,
  batch_scheduler_profile TEXT,
  job_capabilities JSONB NOT NULL,
  tags       TEXT[] NOT NULL,
  notes      JSONB NOT NULL,
  import_ref_id TEXT,
  uuid uuid NOT NULL,
  deleted    BOOLEAN NOT NULL DEFAULT false,
  created    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  UNIQUE (tenant, id)
);
ALTER TABLE systems OWNER TO tapis_sys;
CREATE INDEX sys_tenant_name_idx ON systems (tenant, id);
CREATE INDEX sys_host_idx ON systems (host);
CREATE INDEX sys_owner_idx ON systems (owner);
CREATE INDEX sys_tags_idx ON systems using GIN(tags);
COMMENT ON COLUMN systems.seq_id IS 'System sequence id';
COMMENT ON COLUMN systems.tenant IS 'Tenant name associated with system';
COMMENT ON COLUMN systems.id IS 'Unique name for the system';
COMMENT ON COLUMN systems.description IS 'System description';
COMMENT ON COLUMN systems.system_type IS 'Type of system';
COMMENT ON COLUMN systems.owner IS 'User name of system owner';
COMMENT ON COLUMN systems.host IS 'System host name or ip address';
COMMENT ON COLUMN systems.enabled IS 'Indicates if system is currently active and available for use';
COMMENT ON COLUMN systems.effective_user_id IS 'User name to use when accessing the system';
COMMENT ON COLUMN systems.default_authn_method IS 'How authorization is handled by default';
COMMENT ON COLUMN systems.bucket_name IS 'Name of the bucket for an S3 system';
COMMENT ON COLUMN systems.root_dir IS 'Effective root directory path for a Unix system';
COMMENT ON COLUMN systems.port IS 'Port number used to access a system';
COMMENT ON COLUMN systems.use_proxy IS 'Indicates if system should accessed through a proxy';
COMMENT ON COLUMN systems.proxy_host IS 'Proxy host name or ip address';
COMMENT ON COLUMN systems.proxy_port IS 'Proxy port number';
COMMENT ON COLUMN systems.dtn_system_id IS 'Alternate system to use as a Data Transfer Node (DTN)';
COMMENT ON COLUMN systems.dtn_mount_point IS 'Mount point on local system for the DTN';
COMMENT ON COLUMN systems.is_dtn IS 'Indicates if system is to serve as a data transfer node';
COMMENT ON COLUMN systems.can_exec IS 'Indicates if system can be used to execute jobs';
COMMENT ON COLUMN systems.job_runtimes IS 'Runtimes associated with system';
COMMENT ON COLUMN systems.job_working_dir IS 'Parent directory from which a job is run. Relative to effective root directory.';
COMMENT ON COLUMN systems.job_env_variables IS 'Environment variables added to shell environment';
COMMENT ON COLUMN systems.job_max_jobs IS 'Maximum total number of jobs that can be queued or running on the system at a given time.';
COMMENT ON COLUMN systems.job_max_jobs_per_user IS 'Maximum total number of jobs associated with a specific user that can be queued or running on the system at a given time.';
COMMENT ON COLUMN systems.can_run_batch IS 'Flag indicating if system supports running jobs using a batch scheduler.';
COMMENT ON COLUMN systems.batch_scheduler IS 'Type of scheduler used when running batch jobs';
COMMENT ON COLUMN systems.batch_logical_queues IS 'Logical queues associated with system';
COMMENT ON COLUMN systems.batch_default_logical_queue IS 'Default logical batch queue for the system';
COMMENT ON COLUMN systems.batch_scheduler_profile IS 'Scheduler profile for the system';
COMMENT ON COLUMN systems.job_capabilities IS 'Capabilities associated with system';
COMMENT ON COLUMN systems.tags IS 'Tags for user supplied key:value pairs';
COMMENT ON COLUMN systems.notes IS 'Notes for general information stored as JSON';
COMMENT ON COLUMN systems.import_ref_id IS 'Reference for systems created via import';
COMMENT ON COLUMN systems.deleted IS 'Indicates if system has been soft deleted';
COMMENT ON COLUMN systems.created IS 'UTC time for when record was created';
COMMENT ON COLUMN systems.updated IS 'UTC time for when record was last updated';

-- System updates table
-- Track changes for systems
CREATE TABLE system_updates
(
    seq_id SERIAL PRIMARY KEY,
    system_seq_id INTEGER REFERENCES systems(seq_id) ON DELETE CASCADE,
    obo_tenant TEXT NOT NULL,
    obo_user TEXT NOT NULL,
    jwt_tenant TEXT NOT NULL,
    jwt_user TEXT NOT NULL,
    system_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    description JSONB NOT NULL,
    raw_data TEXT,
    uuid uuid NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
ALTER TABLE system_updates OWNER TO tapis_sys;
COMMENT ON COLUMN system_updates.seq_id IS 'System update request sequence id';
COMMENT ON COLUMN system_updates.system_seq_id IS 'Sequence id of system being updated';
COMMENT ON COLUMN system_updates.obo_tenant IS 'OBO Tenant associated with the change request';
COMMENT ON COLUMN system_updates.obo_user IS 'OBO User associated with the change request';
COMMENT ON COLUMN system_updates.jwt_tenant IS 'Tenant of user who requested the update';
COMMENT ON COLUMN system_updates.jwt_user IS 'Name of user who requested the update';
COMMENT ON COLUMN system_updates.system_id IS 'Id of system being updated';
COMMENT ON COLUMN system_updates.operation IS 'Type of update operation';
COMMENT ON COLUMN system_updates.description IS 'JSON describing the change. Secrets scrubbed as needed.';
COMMENT ON COLUMN system_updates.raw_data IS 'Raw data associated with the request, if available. Secrets scrubbed as needed.';
COMMENT ON COLUMN system_updates.uuid IS 'UUID of system being updated';
COMMENT ON COLUMN system_updates.created IS 'UTC time for when record was created';

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
