package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.GlobusAuthInfo;

public final class RespGlobusAuthUrl extends RespAbstract
{
    public RespGlobusAuthUrl(GlobusAuthInfo result) { this.result = result;}
    
    public GlobusAuthInfo result;
}
