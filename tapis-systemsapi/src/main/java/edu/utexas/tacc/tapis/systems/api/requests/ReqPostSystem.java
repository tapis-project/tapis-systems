package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;

import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_EFFECTIVEUSERID;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_ENABLED;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBENV_VARIABLES;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBSPERUSER;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_NOTES;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_OWNER;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYHOST;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYPORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_USEPROXY;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EMPTY_STR_ARRAY;

/*
 * Class representing all system attributes that can be set in an incoming create request json body
 */
public final class ReqPostSystem
{
  public String id;
  public String description;
  public SystemType systemType;
  public String owner = DEFAULT_OWNER;
  public String host;
  public boolean enabled = DEFAULT_ENABLED;
  public String effectiveUserId = DEFAULT_EFFECTIVEUSERID;
  public AuthnMethod defaultAuthnMethod;
  public Credential authnCredential;
  public String bucketName;
  public String rootDir;
  public int port = DEFAULT_PORT;
  public boolean useProxy = DEFAULT_USEPROXY;
  public String proxyHost = DEFAULT_PROXYHOST;
  public int proxyPort = DEFAULT_PROXYPORT;
  public String dtnSystemId;
  public String dtnMountPoint;
  public String dtnMountSourcePath;
  public boolean isDtn;
  public boolean canExec;
  public List<JobRuntime> jobRuntimes;
  public String jobWorkingDir;
  public List<KeyValuePair> jobEnvVariables;
  public int jobMaxJobs = DEFAULT_JOBMAXJOBS;
  public int jobMaxJobsPerUser = DEFAULT_JOBMAXJOBSPERUSER;
  public boolean jobIsBatch;
  public SchedulerType batchScheduler;
  public List<LogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public String batchSchedulerProfile;
  public List<Capability> jobCapabilities;
  public String[] tags = EMPTY_STR_ARRAY;
  public Object notes = DEFAULT_NOTES;
}
