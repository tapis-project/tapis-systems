--TABLE systems_cred_info
-- We need to add host_login_user to the primary key to properly reflect how credentials are stored in SK.
-- It is possible for multiple credentials to be registered for a system when effUser is static.
-- In this case the current PK(tenant,system_id,tapis_user,is_static) is not unique. We need to allow for multiple
--   entries differing by the host_login_user.
-- This would happen, for example if the owner (or tenant admin) changes the value of the static effUser and
--    registers new credentials. The credentials for the previous static effUser would still be
--    in place in SK and would be used again if the static effUser were switched back.
--
-- Drop existing primary key
ALTER TABLE systems_cred_info DROP CONSTRAINT systems_cred_info_pkey;
-- Create new primary key
ALTER TABLE systems_cred_info ADD primary key (tenant, system_id, tapis_user, is_static, host_login_user);