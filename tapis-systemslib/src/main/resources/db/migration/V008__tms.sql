---- Updates to support new features in Trust Manager Systems (TMS)

-- Add columns tms_login_user, tms_resource_provider and tms_resource_provider_account to systems_cred_info
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS tms_login_user TEXT;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS tms_resource_provider TEXT;
ALTER TABLE systems_cred_info ADD COLUMN IF NOT EXISTS tms_resource_provider_account TEXT;
