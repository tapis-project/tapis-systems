---- Original table for mapping of tapis users to login users
--CREATE TABLE systems_login_user
--(
--    system_seq_id INTEGER REFERENCES systems(seq_id) ON DELETE CASCADE,
--    tenant TEXT NOT NULL,
--    system_id TEXT NOT NULL,
--    tapis_user TEXT NOT NULL,
--    login_user TEXT NOT NULL,
--    created    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
--    updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
--    PRIMARY KEY (tenant, system_id, tapis_user)
--);
--
-- Re-purpose systems_login_user table for recording information on credential registrations for systems.
-- Rename table from systems_login_user to systems_cred_info
-- Add new columns:
--   is_static - indicates if record is for the static or dynamic effectiveUserId case
--   has_credentials - indicates if system has credentials registered for the current defaultAuthnMethod
--   has_password - indicates if credentials for PASSWORD have been registered.
--   has_pki_keys - indicates if credentials for PKI_KEYS have been registered.
--   has_access_key - indicates if credentials for ACCESS_KEY have been registered.
--   has_token - indicates if credentials for TOKEN have been registered.
--   sync_status - indicates current status of synchronization between SK and Systems service.
--      PENDING - Record requires synchronization
--      IN_PROGRESS - Systems service is in the process of synchronizing the record
--      FAILED - Synchronization failed.
--      COMPLETE - Synchronization completed successfully.
--   sync_failed - Timestamp for last sync attempt failure. Null if no failures.
--   sync_fail_count - Number of sync attempts that have failed
--   sync_fail_message - Message indicating why last sync attempt failed
--
-- Default for is_static is false since if there is an existing record then it is for a login user mapping and that
--    is only used for dynamic.
-- Remove the NOT NULL constraint on the login_user column since now we will have records even if there
--    is no mapping.
-- Change primary key from (tenant, system_id, tapis_user) to (tenant, system_id, tapis_user, is_static)
--    since that is what makes a record unique
--
-- Rename table
ALTER TABLE IF EXISTS systems_login_user RENAME TO systems_cred_info;
-- Allow login_user to be NULL
ALTER TABLE systems_cred_info ALTER login_user DROP NOT NULL;
-- Add columns
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_credentials BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS is_static BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_password BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_pki_keys BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_access_key BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_token BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_status TEXT NOT NULL DEFAULT 'PENDING';
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_failed TIMESTAMP WITHOUT TIME ZONE;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_fail_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_fail_message TEXT;

-- We need to add is_static to the primary key because static and dynamic users are independent even if
--   the username is identical. Credentials are registered and stored in SK separately.
-- Drop existing primary key
ALTER TABLE systems_cred_info DROP CONSTRAINT systems_login_user_pkey;
-- Create new primary key
ALTER TABLE systems_cred_info ADD primary key (tenant, system_id, tapis_user, is_static);

--
-- NOTES
--  - Are entries in the login_user table are deleted when a system is deleted or a credential is deleted?
--  - When system is deleted:
--    - Not deleted when system is deleted. But that is probably okay.
--      Credentials only deleted for static user case, currently login_user table only for dynamic use case,
--      so thinking was probably that okay to leave credentials around for dynamic user case.
--      Not okay for static user case because it is a more dangerous service account.
--      Since dynamic user credentials left in place it makes sense to leave login_user entries in place.
--  - When credential deleted?
--    - Yes, login mapping is removed in this case
--      (only for dynamic user, since should only be a mapping for dynamic users.)
--      NOTE: On delete cascade does not help because systems can only be soft deleted by users.
--            On delete cascade will only apply if we ever support a hard delete or a db admin is
--            manually cleaning up system records