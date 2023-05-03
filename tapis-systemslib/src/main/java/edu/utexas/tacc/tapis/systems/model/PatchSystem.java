package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;
import java.util.ArrayList;
import java.util.List;

/*
 * Class representing a patch of a Tapis System.
 * Fields set to null indicate attribute not updated.
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class PatchSystem
{
  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
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
  private final Boolean canRunBatch;
  private final Boolean enableCmdPrefix;
  private final String mpiCmd;
  private final List<JobRuntime> jobRuntimes;
  private final String jobWorkingDir;
  private final List<KeyValuePair> jobEnvVariables;
  private final Integer jobMaxJobs;
  private final Integer jobMaxJobsPerUser;
  private final SchedulerType batchScheduler;
  private final List<LogicalQueue> batchLogicalQueues;
  private final String batchDefaultLogicalQueue;
  private final String batchSchedulerProfile;
  private final List<Capability> jobCapabilities;
  private final String[] tags;
  private Object notes;
  private final String importRefId;
  private final Boolean allowChildren;

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor setting all final attributes.
   */
  public PatchSystem(String description1, String host1, String effectiveUserId1,
                     AuthnMethod defaultAuthnMethod1,
                     Integer port1, Boolean useProxy1, String proxyHost1, Integer proxyPort1,
                     String dtnSystemId1, String dtnMountPoint1, String dtnMountSourcePath1,
                     List<JobRuntime> jobRuntimes1, String jobWorkingDir1, List<KeyValuePair> jobEnvVariables1,
                     Integer jobMaxJobs1, Integer jobMaxJobsPerUser1, Boolean canRunBatch1, Boolean enableCmdPrefix1,
                     String mpiCmd1, SchedulerType batchScheduler1, List<LogicalQueue> batchLogicalQueues1,
                     String batchDefaultLogicalQueue1, String batchSchedulerProfile1, List<Capability> jobCapabilities1,
                     String[] tags1, Object notes1, String importRefId1, Boolean allowChildren1)
  {
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
    canRunBatch = canRunBatch1;
    enableCmdPrefix = enableCmdPrefix1;
    mpiCmd = mpiCmd1;
    jobRuntimes = (jobRuntimes1 == null) ? null : new ArrayList<>(jobRuntimes1);
    jobWorkingDir = jobWorkingDir1;
    jobEnvVariables = (jobEnvVariables1 == null) ? null : new ArrayList<>(jobEnvVariables1);
    jobMaxJobs = jobMaxJobs1;
    jobMaxJobsPerUser = jobMaxJobsPerUser1;
    batchScheduler = batchScheduler1;
    batchLogicalQueues = (batchLogicalQueues1 == null) ? null : new ArrayList<>(batchLogicalQueues1);
    batchDefaultLogicalQueue = batchDefaultLogicalQueue1;
    batchSchedulerProfile = batchSchedulerProfile1;
    jobCapabilities = (jobCapabilities1 == null) ? null : new ArrayList<>(jobCapabilities1);
    tags = (tags1 == null) ? null : tags1.clone();
    notes = notes1;
    importRefId = importRefId1;
    allowChildren = allowChildren1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
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

  public List<KeyValuePair> getJobEnvVariables() {
    return (jobEnvVariables == null) ? null : new ArrayList<>(jobEnvVariables);
  }

  public Integer getJobMaxJobs() { return jobMaxJobs; }

  public Integer getJobMaxJobsPerUser() { return jobMaxJobsPerUser; }

  public Boolean getCanRunBatch() { return canRunBatch; }

  public Boolean getEnableCmdPrefix() {
    return enableCmdPrefix;
  }

  public String getMpiCmd() { return mpiCmd; }

  public SchedulerType getBatchScheduler() { return batchScheduler; }

  public List<LogicalQueue> getBatchLogicalQueues() {
    return (batchLogicalQueues == null) ? null : new ArrayList<>(batchLogicalQueues);
  }

  public String getBatchDefaultLogicalQueue() { return batchDefaultLogicalQueue; }

  public String getBatchSchedulerProfile() { return batchSchedulerProfile; }

  public List<Capability> getJobCapabilities() {
    return (jobCapabilities == null) ? null : new ArrayList<>(jobCapabilities);
  }

  public String[] getTags() {
    return (tags == null) ? null : tags.clone();
  }

  public Object getNotes() { return notes; }
  public void setNotes(Object o) { notes = o; }

  public String getImportRefId() { return importRefId; }

  public Boolean getAllowChildren() {
    return allowChildren;
  }
}
