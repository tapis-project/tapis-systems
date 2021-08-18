package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;

import java.util.List;

/*
  Results from a retrieval of SchedulerProfile resources.
 */
public final class RespSchedulerProfiles extends RespAbstract
{
  public List<SchedulerProfile> result;

  public RespSchedulerProfiles(List<SchedulerProfile> spList)
  {
    result = spList;
  }
}
