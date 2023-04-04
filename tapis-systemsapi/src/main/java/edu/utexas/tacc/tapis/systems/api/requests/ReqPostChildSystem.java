package edu.utexas.tacc.tapis.systems.api.requests;

public class ReqPostChildSystem {
    private final String id;

    private final String effectiveUserId;

    private final String rootDir;
    private final String owner;

    public ReqPostChildSystem(String id, String effectiveUserId, String rootDir, String owner) {
        this.id = id;
        this.effectiveUserId = effectiveUserId;
        this.rootDir = rootDir;
        this.owner = owner;
    }

    public String getId() {
        return id;
    }

    public String getEffectiveUserId() {
        return effectiveUserId;
    }

    public String getRootDir() {
        return rootDir;
    }

    public String getOwner() {
        return owner;
    }
}
