package edu.utexas.tacc.tapis.systems.api.responses;

import com.google.gson.JsonArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultListMetadata;
import edu.utexas.tacc.tapis.systems.api.responses.results.TapisSystemDTO;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

/*
  Results from a retrieval of SchedulerProfile resources.
 */
public final class RespSchedulerProfiles extends RespAbstract
{
  public JsonArray result;

  public RespSchedulerProfiles(List<SchedulerProfile> spList)
  {
    result = new JsonArray();
//    for (SchedulerProfile sp : sList)
//    {
//      result.add(new TapisSystemDTO(sys).getDisplayObject(selectList));
//    }
  }
}
