package edu.utexas.tacc.tapis.systems.api.model;

import edu.utexas.tacc.tapis.systems.model.LogicalQueue;

/*
    Class representing a LogicalQueue result to be returned
 */
public final class ResultLogicalQueue
{
  public String name;
  public String hpcQueueName;
  public int maxJobs;
  public int maxJobsPerUser;
  public int minNodeCount;
  public int maxNodeCount;
  public int minCoresPerNode;
  public int maxCoresPerNode;
  public int minMemoryMB;
  public int maxMemoryMB;
  public int minMinutes;
  public int maxMinutes;

  public ResultLogicalQueue(LogicalQueue q)
  {
    name = q.getName();
    hpcQueueName = q.getHpcQueueName();
    maxJobs = q.getMaxJobs();
    maxJobsPerUser = q.getMaxJobsPerUser();
    minNodeCount = q.getMinNodeCount();
    maxNodeCount = q.getMaxNodeCount();
    minCoresPerNode = q.getMinCoresPerNode();
    maxCoresPerNode = q.getMaxCoresPerNode();
    minMemoryMB = q.getMinMemoryMB();
    maxMemoryMB = q.getMaxMemoryMB();
    minMinutes = q.getMinMinutes();
    maxMinutes = q.getMaxMinutes();
    // Check for -1 in max values and return Integer.MAX_VALUE instead.
    //   As requested by Jobs service.
    if (maxJobs < 0) maxJobs = Integer.MAX_VALUE;
    if (maxJobsPerUser < 0) maxJobsPerUser = Integer.MAX_VALUE;
    if (maxNodeCount < 0) maxNodeCount = Integer.MAX_VALUE;
    if (maxCoresPerNode < 0) maxCoresPerNode = Integer.MAX_VALUE;
    if (maxMemoryMB < 0) maxMemoryMB = Integer.MAX_VALUE;
    if (maxMinutes < 0) maxMinutes = Integer.MAX_VALUE;
  }
}
