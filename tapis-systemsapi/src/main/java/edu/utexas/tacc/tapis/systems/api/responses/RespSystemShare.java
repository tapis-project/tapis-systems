package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.SystemShare;

/*
  Results from a retrieval of System Share resource.
 */
public final class RespSystemShare extends RespAbstract
{
  public SystemShare result;

  public RespSystemShare(SystemShare sShare)
  {
    result = sShare;
  }
}
