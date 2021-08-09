package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;

public final class RespSchedulerProfile extends RespAbstract
{
    public RespSchedulerProfile(SchedulerProfile result) { this.result = result;}
    
    public SchedulerProfile result;
}
