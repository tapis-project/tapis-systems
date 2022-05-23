-- ----------------------------------------------------------------------------------------
--                                     SYSTEMS
-- ----------------------------------------------------------------------------------------
-- Mapping of tapis users to login users
CREATE TABLE systems_login_user
(
    system_seq_id INTEGER REFERENCES systems(seq_id) ON DELETE CASCADE,
    tenant TEXT NOT NULL,
    system_id TEXT NOT NULL,
    tapis_user TEXT NOT NULL,
    login_user TEXT NOT NULL,
    created    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated    TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (tenant, system_id, tapis_user)
);
ALTER TABLE systems_login_user OWNER TO tapis_sys;
