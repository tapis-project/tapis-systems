package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;

import java.util.List;

/*
  Results from a retrieval of SchedulerProfile resources.
 */
public final class RespCredentialMetadata extends RespAbstract
{
  public List<CredentialInfo> result;

  public RespCredentialMetadata(List<CredentialInfo> ciList)
  {
    result = ciList;
  }
}
