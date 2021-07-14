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

  private final int seqId; // Unique database sequence number
  private final int systemSeqId;
  private final RuntimeType runtimeType;
  private final String version;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public JobRuntime(int seqId1, int systemSeqId1, RuntimeType runtimeType1, String version1)
  {
    seqId = seqId1;
    systemSeqId = systemSeqId1;
    runtimeType = runtimeType1;
    version = version1;
  }
  public JobRuntime(RuntimeType runtimeType1, String version1)
  {
    seqId = -1;
    systemSeqId = -1;
    runtimeType = runtimeType1;
    version = version1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public int getSeqId() { return seqId; }
  public int getSystemSeqId() { return systemSeqId; }
  public RuntimeType getRuntimeType() { return runtimeType; }
  public String getVersion() { return version; }
}
