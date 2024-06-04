---- Mapping of tapis users to login users
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
--   is_dynamic - indicates if record is for the static or dynamic effectiveUserId case
--   has_credentials - indicates if system has credentials registered for the current defaultAuthnMethod
--   has_password - indicates if credentials for PASSWORD have been registered.
--   has_pki_keys - indicates if credentials for PKI_KEYS have been registered.
--   has_access_key - indicates if credentials for ACCESS_KEY have been registered.
--   has_token - indicates if credentials for TOKEN have been registered.
--   has_cert - indicates if credentials for CERT have been registered.
--   sync_status - indicates current status of synchronization between SK and Systems service.
--      PENDING - Record requires synchronization
--      IN_PROGRESS - We are in the process of synchronizing record
--      FAILED - Synchronization failed.
--      COMPLETE - Synchronization completed successfully.
--   sync_fail_message - Message indicating why last synch attempt failed
--   sync_fail_count - Number of sync attempts that have failed
-- Default for is_dynamic is true since if there is an existing record then it is for a login user mapping and that
--    is only used for dynamic.
-- Remove the NOT NULL constraint on the login_user column since now we will have records even if there
--    is no mapping.
-- Change primary key from (tenant, system_id, tapis_user) to (tenant, system_id, tapis_user, is_dynamic)
--

ALTER TABLE IF EXISTS systems_login_user RENAME TO systems_cred_info;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_credentials BOOLEAN;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS is_dynamic BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_password BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_pki_keys BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_access_key BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_token BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS has_cert BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_status TEXT NOT NULL DEFAULT 'PENDING';
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_fail_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS sync_fail_message TEXT;

-- We need to add is_dynamic to the primary key because static and dynamic users are independent even if
--   the username is identical. Credentials are registered and stored in SK separately.
-- Drop existing primary key
ALTER TABLE systems_cred_info DROP CONSTRAINT systems_login_user_pkey;
-- Create new primary key
ALTER TABLE systems_cred_info ADD primary key (tenant, system_id, tapis_user, is_dynamic);

--
-- TODO  - NOTES
-- TODO - check code to make sure entries in the login_user table are deleted when a system is deleted
-- TODO - or a credential is deleted.
-- TODO     No, not deleted when system is deleted. But that is probably okay.
-- TODO     Credentials only deleted for static user case, currently login_user table only for dynamic use case,
-- TODO     so thinking was probably that okay to leave credentials around for dynamic user case.
-- TODO     Not okay for static user case because it is a more dangerous service account.
-- TODO     Since dynamic user credentials left in place it makes sense to leave login_user entries in place.
-- TODO BUT what about when cred itself is deleted? Yes, login mapping is removed in this case
-- TODO         (only for dynamic user, since should only be a mapping for dynamic users.)
-- TODO   NOTE: on delete cascade does not help because systems can only be soft deleted by users.
-- TODO         on delete cascade will only apply if we ever support a hard delete or a db admin is
-- TODO         manually cleaning up system records
