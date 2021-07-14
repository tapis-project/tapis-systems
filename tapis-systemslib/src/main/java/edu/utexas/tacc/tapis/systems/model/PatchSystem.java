package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;
import java.util.ArrayList;
import java.util.List;

/*
 * Class representing an update to a Tapis System.
 * Fields set to null indicate attribute not updated.
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class PatchSystem
{
  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  private String tenant;// No update - reference only
  private String id;// No update - reference only
  private final String description;
  private final String host;
  private final String effectiveUserId;
  private final AuthnMethod defaultAuthnMethod;
  private final Integer port;
  private final Boolean useProxy;
  private final String proxyHost;
  private final Integer proxyPort;
  private final String dtnSystemId;
  private final String dtnMountPoint;
  private final String dtnMountSourcePath;
  private final List<JobRuntime> jobRuntimes;
  private final String jobWorkingDir;
  private final String[] jobEnvVariables;
  private final Integer jobMaxJobs;
  private final Integer jobMaxJobsPerUser;
  private final Boolean jobIsBatch;
  private final SchedulerType batchScheduler;
  private final List<LogicalQueue> batchLogicalQueues;
  private final String batchDefaultLogicalQueue;
  private final List<Capability> jobCapabilities;
  private final String[] tags;
  private final Object notes;

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor setting all final attributes.
   */
  public PatchSystem(String tenant1, String id1, String description1, String host1, String effectiveUserId1,
                     AuthnMethod defaultAuthnMethod1,
                     Integer port1, Boolean useProxy1, String proxyHost1, Integer proxyPort1,
                     String dtnSystemId1, String dtnMountPoint1, String dtnMountSourcePath1,
                     List<JobRuntime> jobRuntimes1, String jobWorkingDir1, String[] jobEnvVariables1,
                     Integer jobMaxJobs1, Integer jobMaxJobsPerUser1, Boolean jobIsBatch1,
                     SchedulerType batchScheduler1, List<LogicalQueue> batchLogicalQueues1, String batchDefaultLogicalQueue1,
                     List<Capability> jobCapabilities1, String[] tags1, Object notes1)
  {
    tenant = tenant1;
    id = id1;
    description = description1;
    host = host1;
    effectiveUserId = effectiveUserId1;
    defaultAuthnMethod = defaultAuthnMethod1;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    dtnSystemId = dtnSystemId1;
    dtnMountPoint = dtnMountPoint1;
    dtnMountSourcePath = dtnMountSourcePath1;
    jobRuntimes = (jobRuntimes1 == null) ? null : new ArrayList<>(jobRuntimes1);
    jobWorkingDir = jobWorkingDir1;
    jobEnvVariables = (jobEnvVariables1 == null) ? null : jobEnvVariables1.clone();
    jobMaxJobs = jobMaxJobs1;
    jobMaxJobsPerUser = jobMaxJobsPerUser1;
    jobIsBatch = jobIsBatch1;
    batchScheduler = batchScheduler1;
    batchLogicalQueues = (batchLogicalQueues1 == null) ? null : new ArrayList<>(batchLogicalQueues1);
    batchDefaultLogicalQueue = batchDefaultLogicalQueue1;
    jobCapabilities = (jobCapabilities1 == null) ? null : new ArrayList<>(jobCapabilities1);
    tags = (tags1 == null) ? null : tags1.clone();
    notes = notes1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public String getTenant() { return tenant; }

  public String getId() { return id; }

  public String getDescription() { return description; }

  public String getHost() { return host; }

  public String getEffectiveUserId() { return effectiveUserId; }

  public AuthnMethod getDefaultAuthnMethod() { return defaultAuthnMethod; }

  public Integer getPort() { return port; }

  public Boolean isUseProxy() { return useProxy; }

  public String getProxyHost() { return proxyHost; }

  public Integer getProxyPort() { return proxyPort; }

  public String getDtnSystemId() { return dtnSystemId; }

  public String getDtnMountPoint() { return dtnMountPoint; }

  public String getDtnMountSourcePath() { return dtnMountSourcePath; }

  public List<JobRuntime> getJobRuntimes() {
    return (jobRuntimes == null) ? null : new ArrayList<>(jobRuntimes);
  }

  public String getJobWorkingDir() { return jobWorkingDir; }

  public String[] getJobEnvVariables() {
    return (jobEnvVariables == null) ? null : jobEnvVariables.clone();
  }

  public Integer getJobMaxJobs() { return jobMaxJobs; }

  public Integer getJobMaxJobsPerUser() { return jobMaxJobsPerUser; }

  public Boolean getJobIsBatch() { return jobIsBatch; }

  public SchedulerType getBatchScheduler() { return batchScheduler; }

  public List<LogicalQueue> getBatchLogicalQueues() {
    return (batchLogicalQueues == null) ? null : new ArrayList<>(batchLogicalQueues);
  }

  public String getBatchDefaultLogicalQueue() { return batchDefaultLogicalQueue; }

  public List<Capability> getJobCapabilities() {
    return (jobCapabilities == null) ? null : new ArrayList<>(jobCapabilities);
  }

  public String[] getTags() {
    return (tags == null) ? null : tags.clone();
  }

  public Object getNotes() {
    return notes;
  }
}
