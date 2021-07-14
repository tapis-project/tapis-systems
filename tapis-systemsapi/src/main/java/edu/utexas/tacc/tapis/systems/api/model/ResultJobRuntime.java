package edu.utexas.tacc.tapis.systems.api.model;

import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.JobRuntime.RuntimeType;

/*
    Class representing a JobRuntime result to be returned
 */
public final class ResultJobRuntime
{
  public RuntimeType runtimeType;
  public String version;

  public ResultJobRuntime(JobRuntime rt)
  {
    runtimeType = rt.getRuntimeType();
    version = rt.getVersion();
  }
}
