package edu.utexas.tacc.tapis.systems.model;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

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
 *       Currently, system_id should be known in the context in which this class is used
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
  private final String description;
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
  private List<ArgSpec> schedulerOptions;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public LogicalQueue(String name1, String description1, String hpcQueueName1, int maxJobs1, int maxJobsPerUser1,
                      int minNodeCount1, int maxNodeCount1, int minCoresPerNode1, int maxCoresPerNode1,
                      int minMemoryMB1, int maxMemoryMB1, int minMinutes1, int maxMinutes1, List<ArgSpec> schedulerOptions1)
  {
    name = LibUtils.stripStr(name1);
    description = description1;
    hpcQueueName = LibUtils.stripStr(hpcQueueName1);
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
    schedulerOptions = (schedulerOptions1 == null) ? null: new ArrayList<>(schedulerOptions1);
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getName() { return name; }
  public String getDescription() { return description; }
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
  public List<ArgSpec> getSchedulerOptions() { return (schedulerOptions == null) ? null : new ArrayList<>(schedulerOptions); }
  public void setSchedulerOptions(List<ArgSpec> so) { schedulerOptions = so; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
