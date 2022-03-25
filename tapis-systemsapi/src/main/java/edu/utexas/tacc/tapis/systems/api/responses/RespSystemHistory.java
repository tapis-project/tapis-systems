package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;

import java.util.List;

/*
  Results from a retrieval of SystemHistory items resources.
 */
public final class RespSystemHistory extends RespAbstract
{
  public List<SystemHistoryItem> result;

  public RespSystemHistory(List<SystemHistoryItem> shList)
  {
    result = shList;
  }
}
