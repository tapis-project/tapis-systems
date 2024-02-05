package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;

import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_EFFECTIVEUSERID;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBSPERUSER;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_NOTES;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYHOST;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYPORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_USEPROXY;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EMPTY_STR_ARRAY;

/*
 * Class representing all system attributes that can be set in an incoming PUT request json body
 */
public final class ReqPutSystem
{
  public String description;
  public String host;
  public String effectiveUserId = DEFAULT_EFFECTIVEUSERID;
  public String parentId;
  public AuthnMethod defaultAuthnMethod;
  public Credential authnCredential;
  public int port = DEFAULT_PORT;
  public boolean useProxy = DEFAULT_USEPROXY;
  public String proxyHost = DEFAULT_PROXYHOST;
  public int proxyPort = DEFAULT_PROXYPORT;
  public String dtnSystemId;
  public boolean canRunBatch;
  public boolean enableCmdPrefix;
  public String mpiCmd;
  public List<JobRuntime> jobRuntimes;
  public String jobWorkingDir;
  public List<KeyValuePair> jobEnvVariables;
  public int jobMaxJobs = DEFAULT_JOBMAXJOBS;
  public int jobMaxJobsPerUser = DEFAULT_JOBMAXJOBSPERUSER;
  public SchedulerType batchScheduler;
  public List<LogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public String batchSchedulerProfile;
  public List<Capability> jobCapabilities;
  public String[] tags = EMPTY_STR_ARRAY;
  public Object notes = DEFAULT_NOTES;
  public String importRefId;
  public boolean allowChildren;
}
