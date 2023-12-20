package edu.utexas.tacc.tapis.systems.model;

import java.util.Objects;

/*
 * Class representing a runtime supported by the Tapis Jobs service.
 * For example DOCKER, SINGULARITY
 * Includes runtime type and version.
 */
public final class JobRuntime
{
  public enum RuntimeType {DOCKER, SINGULARITY, ZIP}
  private final RuntimeType runtimeType;
  private final String version;

  public JobRuntime(RuntimeType runtimeType1, String version1)
  {
    runtimeType = runtimeType1;
    version = version1;
  }
  public RuntimeType getRuntimeType() { return runtimeType; }
  public String getVersion() { return version; }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) { return true; }
    // Note: no need to check for o==null since instanceof will handle that case
    if (!(o instanceof JobRuntime)) return false;
    var that = (JobRuntime) o;
    return (Objects.equals(this.runtimeType, that.runtimeType) && Objects.equals(this.version, that.version));
  }

  @Override
  public int hashCode()
  {
    int retVal = (runtimeType == null ? 1 : runtimeType.hashCode());
    retVal = 31 * retVal + (version == null ? 0 : version.hashCode());
    return retVal;
  }
}
