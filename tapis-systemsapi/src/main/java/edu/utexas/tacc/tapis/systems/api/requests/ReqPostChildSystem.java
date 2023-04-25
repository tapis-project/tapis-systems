package edu.utexas.tacc.tapis.systems.api.requests;

import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_CHILD_ENABLED;

public class ReqPostChildSystem {
    public String id;
    public String effectiveUserId;
    public String rootDir;
    public String owner;
    public boolean enabled = DEFAULT_CHILD_ENABLED;
}
