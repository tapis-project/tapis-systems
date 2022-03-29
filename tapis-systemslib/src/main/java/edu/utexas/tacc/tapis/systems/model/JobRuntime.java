package edu.utexas.tacc.tapis.systems.model;

/*
 * TODO/TBD: Override equals/hashCode so List.equals can work. For change history checking
 *           Make comparable?
 *           Use google's autoValue annotation for equals/hashCode?
 *           Better yet use java's new record keyword? But turns out this breaks gson.
 */
public final class JobRuntime
{
  public enum RuntimeType {DOCKER, SINGULARITY}

  private final RuntimeType runtimeType;
  private final String version;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public JobRuntime(RuntimeType runtimeType1, String version1)
  {
    runtimeType = runtimeType1;
    version = version1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public RuntimeType getRuntimeType() { return runtimeType; }
  public String getVersion() { return version; }
}
