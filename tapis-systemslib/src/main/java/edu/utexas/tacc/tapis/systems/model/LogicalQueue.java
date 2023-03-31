package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/*
 * A queue that maps to a single HPC queue. Provides a uniform front end abstraction for an HPC queue.
 *   Also provides more features and flexibility than is typically provided by an HPC scheduler.
 *   Multiple logical queues may be defined for each HPC queue.
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 * system_id + name must be unique.
 *
 * NOTE: In the database a logical queue also includes system_id, created and updated.
 *       Currently system_id should be known in the context in which this class is used
 *         and the created, updated timestamps are not being used.
 */
public final class LogicalQueue
{

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  public static final String DEFAULT_VALUE = "";
  public static final String DEFAULT_SUBCATEGORY = "";
  public static final int DEFAULT_PRECEDENCE = 100;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(LogicalQueue.class);

  private final String name;   // Name for the logical queue
  private final String hpcQueueName;   // Name for the associated HPC queue
  private final int maxJobs;
  private final int maxJobsPerUser;
  private final int minNodeCount;
  private final int maxNodeCount;
  private final int minCoresPerNode;
  private final int maxCoresPerNode;
  private final int minMemoryMB;
  private final int maxMemoryMB;
  private final int minMinutes;
  private final int maxMinutes;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public LogicalQueue(String name1, String hpcQueueName1, int maxJobs1, int maxJobsPerUser1,
                      int minNodeCount1, int maxNodeCount1, int minCoresPerNode1, int maxCoresPerNode1,
                      int minMemoryMB1, int maxMemoryMB1, int minMinutes1, int maxMinutes1)
  {
    name = name1;
    hpcQueueName = hpcQueueName1;
    maxJobs = maxJobs1 < 0 ? Integer.MAX_VALUE : maxJobs1;
    maxJobsPerUser = maxJobsPerUser1 < 0 ? Integer.MAX_VALUE : maxJobsPerUser1;
    minNodeCount = minNodeCount1;
    maxNodeCount = maxNodeCount1;
    minCoresPerNode = minCoresPerNode1;
    maxCoresPerNode = maxCoresPerNode1;
    minMemoryMB = minMemoryMB1;
    maxMemoryMB = maxMemoryMB1;
    minMinutes = minMinutes1;
    maxMinutes = maxMinutes1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getName() { return name; }
  public String getHpcQueueName() { return hpcQueueName; }
  public int getMaxJobs() { return maxJobs < 0 ? Integer.MAX_VALUE : maxJobs; }
  public int getMaxJobsPerUser() { return maxJobsPerUser < 0 ? Integer.MAX_VALUE : maxJobsPerUser; }
  public int getMinNodeCount() { return minNodeCount; }
  public int getMaxNodeCount() { return maxNodeCount; }
  public int getMinCoresPerNode() { return minCoresPerNode; }
  public int getMaxCoresPerNode() { return maxCoresPerNode; }
  public int getMinMemoryMB() { return minMemoryMB; }
  public int getMaxMemoryMB() { return maxMemoryMB; }
  public int getMinMinutes() { return minMinutes; }
  public int getMaxMinutes() { return maxMinutes; }

  @Override
  public String toString() {return TapisUtils.toString(this);}

  @Override
  public boolean equals(Object o)
  {
    if (o == this) return true;
    // Note: no need to check for o==null since instanceof will handle that case
    if (!(o instanceof LogicalQueue)) return false;
    var that = (LogicalQueue) o;
    return (Objects.equals(this.name, that.name) && Objects.equals(this.hpcQueueName, that.hpcQueueName) &&
            this.maxJobs==that.maxJobs && this.maxJobsPerUser==that.maxJobsPerUser &&
            this.minNodeCount==that.minNodeCount && this.maxNodeCount==that.maxNodeCount &&
            this.minCoresPerNode==that.minCoresPerNode && this.maxCoresPerNode==that.maxCoresPerNode &&
            this.minMemoryMB==that.minMemoryMB && this.maxMemoryMB==that.maxMemoryMB &&
            this.minMinutes==that.minMinutes && this.maxMinutes==that.maxMinutes);
  }

  @Override
  public int hashCode()
  {
    int retVal = (name == null ? 1 : name.hashCode());
    retVal = 31 * retVal + (hpcQueueName == null ? 0 : hpcQueueName.hashCode());
    retVal = 31 * retVal + Integer.hashCode(maxJobs);
    retVal = 31 * retVal + Integer.hashCode(maxJobsPerUser);
    retVal = 31 * retVal + Integer.hashCode(minNodeCount);
    retVal = 31 * retVal + Integer.hashCode(maxNodeCount);
    retVal = 31 * retVal + Integer.hashCode(minCoresPerNode);
    retVal = 31 * retVal + Integer.hashCode(maxCoresPerNode);
    retVal = 31 * retVal + Integer.hashCode(minMemoryMB);
    retVal = 31 * retVal + Integer.hashCode(maxMemoryMB);
    retVal = 31 * retVal + Integer.hashCode(minMinutes);
    retVal = 31 * retVal + Integer.hashCode(maxMinutes);
    return retVal;
  }
}
