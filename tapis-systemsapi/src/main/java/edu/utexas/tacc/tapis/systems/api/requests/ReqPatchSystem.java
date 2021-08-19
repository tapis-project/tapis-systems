package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.api.utils.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;

import java.util.List;

/*
 * Class representing all system attributes that can be set in an incoming patch request json body.
 * Use classes for attribute types instead of primitives so that null can be use to indicate
 *   that the value has not been included in the update request.
 */
public final class ReqPatchSystem
{
  public String description;
  public String host;
  public String effectiveUserId;
  public AuthnMethod defaultAuthnMethod;
  public Integer port;
  public Boolean useProxy;
  public String proxyHost;
  public Integer proxyPort;
  public String dtnSystemId;
  public String dtnMountPoint;
  public String dtnMountSourcePath;
  public List<JobRuntime> jobRuntimes;
  public String jobWorkingDir;
  public List<KeyValuePair> jobEnvVariables;
  public Integer jobMaxJobs;
  public Integer jobMaxJobsPerUser;
  public Boolean jobIsBatch;
  public SchedulerType batchScheduler;
  public List<LogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public String batchSchedulerProfile;
  public List<Capability> jobCapabilities;
  public String[] tags;
  public Object notes;
}
