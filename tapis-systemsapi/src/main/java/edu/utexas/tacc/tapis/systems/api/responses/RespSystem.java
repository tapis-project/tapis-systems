package edu.utexas.tacc.tapis.systems.api.responses;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.api.responses.results.TapisSystemDTO;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

public final class RespSystem extends RespAbstract
{
  public JsonObject result;

  public RespSystem(TSystem s, List<String> selectList)
  {
    result = new TapisSystemDTO(s).getDisplayObject(selectList);
  }
}
