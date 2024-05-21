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

-- TODO - check code to make sure entries in the login_user table are deleted when a system is deleted
-- TODO - or a credential is deleted.
--      No, not deleted when system is deleted. But that is probably okay.
--      Creds only deleted for static user case, currently login_user table only for dynamic use case,
--      so thinking was probably that okay to leave creds around for dynamic user case.
--      Not okay for static user case because it is a more dangerous service account.
--      Since dynamic user creds left in place it makes sense to leave login_user entries in place.
-- TODO BUT what about when cred itself is deleted? Yes, login mapping is removed in this case
-- TODO         (only for dynamic user, since should only be a mapping for dynamic users.)
-- TODO   NOTE: on delete cascade does not help because systems can only be soft deleted by users.
-- TODO         on delete cascace will only apply if we ever support a hard delete or a db admin is
-- TODO         manually cleaning up system records
-- Have all columns not null except has_creds.
--   TODO/TBD - use has_creds as a flag indicating values need to be filled in from SK queries
--           default for is_dynamic is true since if there is an existing record then it is for a
--           login user mapping and that is only used for dynamic.
-- BUT, how to tell if there is a host login user for a dynamic user?
--      TBD: allow login_user to be empty string to indicate there is no mapping?
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_credentials boolean BOOLEAN;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS is_dynamic boolean BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_password boolean BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_pki boolean BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_key boolean BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_token boolean BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE systems_login_user ADD COLUMN IF NOT EXISTS has_cert boolean BOOLEAN NOT NULL DEFAULT false;

-- We need to add is_dynamic to the primary key because static and dynamic users are independent even if
--   the username is identical. Credentials are registered and stored in SK separately.
-- TODO drop existing primary key
ALTER TABLE systems_login_user DROP CONSTRAINT systems_login_user_pkey;
-- TODO create new primary key
ALTER TABLE systems_login_user ADD CONSTRAINT primary key (tenant, system_id, tapis_user, is_dynamic)

