/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables.records;


import com.google.gson.JsonElement;

import edu.utexas.tacc.tapis.systems.gen.jooq.tables.Systems;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

import java.time.LocalDateTime;
import java.util.UUID;

import org.jooq.Record1;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class SystemsRecord extends UpdatableRecordImpl<SystemsRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>tapis_sys.systems.seq_id</code>. System sequence id
     */
    public void setSeqId(Integer value) {
        set(0, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.seq_id</code>. System sequence id
     */
    public Integer getSeqId() {
        return (Integer) get(0);
    }

    /**
     * Setter for <code>tapis_sys.systems.tenant</code>. Tenant name associated
     * with system
     */
    public void setTenant(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.tenant</code>. Tenant name associated
     * with system
     */
    public String getTenant() {
        return (String) get(1);
    }

    /**
     * Setter for <code>tapis_sys.systems.id</code>. Unique name for the system
     */
    public void setId(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.id</code>. Unique name for the system
     */
    public String getId() {
        return (String) get(2);
    }

    /**
     * Setter for <code>tapis_sys.systems.description</code>. System description
     */
    public void setDescription(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.description</code>. System description
     */
    public String getDescription() {
        return (String) get(3);
    }

    /**
     * Setter for <code>tapis_sys.systems.system_type</code>. Type of system
     */
    public void setSystemType(SystemType value) {
        set(4, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.system_type</code>. Type of system
     */
    public SystemType getSystemType() {
        return (SystemType) get(4);
    }

    /**
     * Setter for <code>tapis_sys.systems.owner</code>. User name of system
     * owner
     */
    public void setOwner(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.owner</code>. User name of system
     * owner
     */
    public String getOwner() {
        return (String) get(5);
    }

    /**
     * Setter for <code>tapis_sys.systems.host</code>. System host name or ip
     * address
     */
    public void setHost(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.host</code>. System host name or ip
     * address
     */
    public String getHost() {
        return (String) get(6);
    }

    /**
     * Setter for <code>tapis_sys.systems.enabled</code>. Indicates if system is
     * currently active and available for use
     */
    public void setEnabled(Boolean value) {
        set(7, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.enabled</code>. Indicates if system is
     * currently active and available for use
     */
    public Boolean getEnabled() {
        return (Boolean) get(7);
    }

    /**
     * Setter for <code>tapis_sys.systems.effective_user_id</code>. User name to
     * use when accessing the system
     */
    public void setEffectiveUserId(String value) {
        set(8, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.effective_user_id</code>. User name to
     * use when accessing the system
     */
    public String getEffectiveUserId() {
        return (String) get(8);
    }

    /**
     * Setter for <code>tapis_sys.systems.default_authn_method</code>. How
     * authorization is handled by default
     */
    public void setDefaultAuthnMethod(AuthnMethod value) {
        set(9, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.default_authn_method</code>. How
     * authorization is handled by default
     */
    public AuthnMethod getDefaultAuthnMethod() {
        return (AuthnMethod) get(9);
    }

    /**
     * Setter for <code>tapis_sys.systems.bucket_name</code>. Name of the bucket
     * for an S3 system
     */
    public void setBucketName(String value) {
        set(10, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.bucket_name</code>. Name of the bucket
     * for an S3 system
     */
    public String getBucketName() {
        return (String) get(10);
    }

    /**
     * Setter for <code>tapis_sys.systems.root_dir</code>. Effective root
     * directory path for a Unix system
     */
    public void setRootDir(String value) {
        set(11, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.root_dir</code>. Effective root
     * directory path for a Unix system
     */
    public String getRootDir() {
        return (String) get(11);
    }

    /**
     * Setter for <code>tapis_sys.systems.port</code>. Port number used to
     * access a system
     */
    public void setPort(Integer value) {
        set(12, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.port</code>. Port number used to
     * access a system
     */
    public Integer getPort() {
        return (Integer) get(12);
    }

    /**
     * Setter for <code>tapis_sys.systems.use_proxy</code>. Indicates if system
     * should accessed through a proxy
     */
    public void setUseProxy(Boolean value) {
        set(13, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.use_proxy</code>. Indicates if system
     * should accessed through a proxy
     */
    public Boolean getUseProxy() {
        return (Boolean) get(13);
    }

    /**
     * Setter for <code>tapis_sys.systems.proxy_host</code>. Proxy host name or
     * ip address
     */
    public void setProxyHost(String value) {
        set(14, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.proxy_host</code>. Proxy host name or
     * ip address
     */
    public String getProxyHost() {
        return (String) get(14);
    }

    /**
     * Setter for <code>tapis_sys.systems.proxy_port</code>. Proxy port number
     */
    public void setProxyPort(Integer value) {
        set(15, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.proxy_port</code>. Proxy port number
     */
    public Integer getProxyPort() {
        return (Integer) get(15);
    }

    /**
     * Setter for <code>tapis_sys.systems.dtn_system_id</code>. Alternate system
     * to use as a Data Transfer Node (DTN)
     */
    public void setDtnSystemId(String value) {
        set(16, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.dtn_system_id</code>. Alternate system
     * to use as a Data Transfer Node (DTN)
     */
    public String getDtnSystemId() {
        return (String) get(16);
    }

    /**
     * Setter for <code>tapis_sys.systems.dtn_mount_point</code>. Mount point on
     * local system for the DTN
     */
    public void setDtnMountPoint(String value) {
        set(17, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.dtn_mount_point</code>. Mount point on
     * local system for the DTN
     */
    public String getDtnMountPoint() {
        return (String) get(17);
    }

    /**
     * Setter for <code>tapis_sys.systems.dtn_mount_source_path</code>.
     */
    public void setDtnMountSourcePath(String value) {
        set(18, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.dtn_mount_source_path</code>.
     */
    public String getDtnMountSourcePath() {
        return (String) get(18);
    }

    /**
     * Setter for <code>tapis_sys.systems.is_dtn</code>. Indicates if system is
     * to serve as a data transfer node
     */
    public void setIsDtn(Boolean value) {
        set(19, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.is_dtn</code>. Indicates if system is
     * to serve as a data transfer node
     */
    public Boolean getIsDtn() {
        return (Boolean) get(19);
    }

    /**
     * Setter for <code>tapis_sys.systems.can_exec</code>. Indicates if system
     * can be used to execute jobs
     */
    public void setCanExec(Boolean value) {
        set(20, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.can_exec</code>. Indicates if system
     * can be used to execute jobs
     */
    public Boolean getCanExec() {
        return (Boolean) get(20);
    }

    /**
     * Setter for <code>tapis_sys.systems.can_run_batch</code>. Flag indicating
     * if system supports running jobs using a batch scheduler.
     */
    public void setCanRunBatch(Boolean value) {
        set(21, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.can_run_batch</code>. Flag indicating
     * if system supports running jobs using a batch scheduler.
     */
    public Boolean getCanRunBatch() {
        return (Boolean) get(21);
    }

    /**
     * Setter for <code>tapis_sys.systems.mpi_cmd</code>.
     */
    public void setMpiCmd(String value) {
        set(22, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.mpi_cmd</code>.
     */
    public String getMpiCmd() {
        return (String) get(22);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_runtimes</code>. Runtimes
     * associated with system
     */
    public void setJobRuntimes(JsonElement value) {
        set(23, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_runtimes</code>. Runtimes
     * associated with system
     */
    public JsonElement getJobRuntimes() {
        return (JsonElement) get(23);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_working_dir</code>. Parent
     * directory from which a job is run. Relative to effective root directory.
     */
    public void setJobWorkingDir(String value) {
        set(24, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_working_dir</code>. Parent
     * directory from which a job is run. Relative to effective root directory.
     */
    public String getJobWorkingDir() {
        return (String) get(24);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_env_variables</code>. Environment
     * variables added to shell environment
     */
    public void setJobEnvVariables(JsonElement value) {
        set(25, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_env_variables</code>. Environment
     * variables added to shell environment
     */
    public JsonElement getJobEnvVariables() {
        return (JsonElement) get(25);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_max_jobs</code>. Maximum total
     * number of jobs that can be queued or running on the system at a given
     * time.
     */
    public void setJobMaxJobs(Integer value) {
        set(26, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_max_jobs</code>. Maximum total
     * number of jobs that can be queued or running on the system at a given
     * time.
     */
    public Integer getJobMaxJobs() {
        return (Integer) get(26);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_max_jobs_per_user</code>. Maximum
     * total number of jobs associated with a specific user that can be queued
     * or running on the system at a given time.
     */
    public void setJobMaxJobsPerUser(Integer value) {
        set(27, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_max_jobs_per_user</code>. Maximum
     * total number of jobs associated with a specific user that can be queued
     * or running on the system at a given time.
     */
    public Integer getJobMaxJobsPerUser() {
        return (Integer) get(27);
    }

    /**
     * Setter for <code>tapis_sys.systems.batch_scheduler</code>. Type of
     * scheduler used when running batch jobs
     */
    public void setBatchScheduler(SchedulerType value) {
        set(28, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.batch_scheduler</code>. Type of
     * scheduler used when running batch jobs
     */
    public SchedulerType getBatchScheduler() {
        return (SchedulerType) get(28);
    }

    /**
     * Setter for <code>tapis_sys.systems.batch_logical_queues</code>. Logical
     * queues associated with system
     */
    public void setBatchLogicalQueues(JsonElement value) {
        set(29, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.batch_logical_queues</code>. Logical
     * queues associated with system
     */
    public JsonElement getBatchLogicalQueues() {
        return (JsonElement) get(29);
    }

    /**
     * Setter for <code>tapis_sys.systems.batch_default_logical_queue</code>.
     * Default logical batch queue for the system
     */
    public void setBatchDefaultLogicalQueue(String value) {
        set(30, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.batch_default_logical_queue</code>.
     * Default logical batch queue for the system
     */
    public String getBatchDefaultLogicalQueue() {
        return (String) get(30);
    }

    /**
     * Setter for <code>tapis_sys.systems.batch_scheduler_profile</code>.
     * Scheduler profile for the system
     */
    public void setBatchSchedulerProfile(String value) {
        set(31, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.batch_scheduler_profile</code>.
     * Scheduler profile for the system
     */
    public String getBatchSchedulerProfile() {
        return (String) get(31);
    }

    /**
     * Setter for <code>tapis_sys.systems.job_capabilities</code>. Capabilities
     * associated with system
     */
    public void setJobCapabilities(JsonElement value) {
        set(32, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.job_capabilities</code>. Capabilities
     * associated with system
     */
    public JsonElement getJobCapabilities() {
        return (JsonElement) get(32);
    }

    /**
     * Setter for <code>tapis_sys.systems.tags</code>. Tags for user supplied
     * key:value pairs
     */
    public void setTags(String[] value) {
        set(33, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.tags</code>. Tags for user supplied
     * key:value pairs
     */
    public String[] getTags() {
        return (String[]) get(33);
    }

    /**
     * Setter for <code>tapis_sys.systems.notes</code>. Notes for general
     * information stored as JSON
     */
    public void setNotes(JsonElement value) {
        set(34, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.notes</code>. Notes for general
     * information stored as JSON
     */
    public JsonElement getNotes() {
        return (JsonElement) get(34);
    }

    /**
     * Setter for <code>tapis_sys.systems.import_ref_id</code>. Reference for
     * systems created via import
     */
    public void setImportRefId(String value) {
        set(35, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.import_ref_id</code>. Reference for
     * systems created via import
     */
    public String getImportRefId() {
        return (String) get(35);
    }

    /**
     * Setter for <code>tapis_sys.systems.uuid</code>.
     */
    public void setUuid(UUID value) {
        set(36, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.uuid</code>.
     */
    public UUID getUuid() {
        return (UUID) get(36);
    }

    /**
     * Setter for <code>tapis_sys.systems.deleted</code>. Indicates if system
     * has been soft deleted
     */
    public void setDeleted(Boolean value) {
        set(37, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.deleted</code>. Indicates if system
     * has been soft deleted
     */
    public Boolean getDeleted() {
        return (Boolean) get(37);
    }

    /**
     * Setter for <code>tapis_sys.systems.created</code>. UTC time for when
     * record was created
     */
    public void setCreated(LocalDateTime value) {
        set(38, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.created</code>. UTC time for when
     * record was created
     */
    public LocalDateTime getCreated() {
        return (LocalDateTime) get(38);
    }

    /**
     * Setter for <code>tapis_sys.systems.updated</code>. UTC time for when
     * record was last updated
     */
    public void setUpdated(LocalDateTime value) {
        set(39, value);
    }

    /**
     * Getter for <code>tapis_sys.systems.updated</code>. UTC time for when
     * record was last updated
     */
    public LocalDateTime getUpdated() {
        return (LocalDateTime) get(39);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Integer> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached SystemsRecord
     */
    public SystemsRecord() {
        super(Systems.SYSTEMS);
    }

    /**
     * Create a detached, initialised SystemsRecord
     */
    public SystemsRecord(Integer seqId, String tenant, String id, String description, SystemType systemType, String owner, String host, Boolean enabled, String effectiveUserId, AuthnMethod defaultAuthnMethod, String bucketName, String rootDir, Integer port, Boolean useProxy, String proxyHost, Integer proxyPort, String dtnSystemId, String dtnMountPoint, String dtnMountSourcePath, Boolean isDtn, Boolean canExec, Boolean canRunBatch, String mpiCmd, JsonElement jobRuntimes, String jobWorkingDir, JsonElement jobEnvVariables, Integer jobMaxJobs, Integer jobMaxJobsPerUser, SchedulerType batchScheduler, JsonElement batchLogicalQueues, String batchDefaultLogicalQueue, String batchSchedulerProfile, JsonElement jobCapabilities, String[] tags, JsonElement notes, String importRefId, UUID uuid, Boolean deleted, LocalDateTime created, LocalDateTime updated) {
        super(Systems.SYSTEMS);

        setSeqId(seqId);
        setTenant(tenant);
        setId(id);
        setDescription(description);
        setSystemType(systemType);
        setOwner(owner);
        setHost(host);
        setEnabled(enabled);
        setEffectiveUserId(effectiveUserId);
        setDefaultAuthnMethod(defaultAuthnMethod);
        setBucketName(bucketName);
        setRootDir(rootDir);
        setPort(port);
        setUseProxy(useProxy);
        setProxyHost(proxyHost);
        setProxyPort(proxyPort);
        setDtnSystemId(dtnSystemId);
        setDtnMountPoint(dtnMountPoint);
        setDtnMountSourcePath(dtnMountSourcePath);
        setIsDtn(isDtn);
        setCanExec(canExec);
        setCanRunBatch(canRunBatch);
        setMpiCmd(mpiCmd);
        setJobRuntimes(jobRuntimes);
        setJobWorkingDir(jobWorkingDir);
        setJobEnvVariables(jobEnvVariables);
        setJobMaxJobs(jobMaxJobs);
        setJobMaxJobsPerUser(jobMaxJobsPerUser);
        setBatchScheduler(batchScheduler);
        setBatchLogicalQueues(batchLogicalQueues);
        setBatchDefaultLogicalQueue(batchDefaultLogicalQueue);
        setBatchSchedulerProfile(batchSchedulerProfile);
        setJobCapabilities(jobCapabilities);
        setTags(tags);
        setNotes(notes);
        setImportRefId(importRefId);
        setUuid(uuid);
        setDeleted(deleted);
        setCreated(created);
        setUpdated(updated);
    }
}
