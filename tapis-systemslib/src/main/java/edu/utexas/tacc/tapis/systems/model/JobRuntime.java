package edu.utexas.tacc.tapis.systems.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 *
 */
public final class JobRuntime
{
  public enum RuntimeType {DOCKER, SINGULARITY}

  private static final Logger _log = LoggerFactory.getLogger(JobRuntime.class);

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
