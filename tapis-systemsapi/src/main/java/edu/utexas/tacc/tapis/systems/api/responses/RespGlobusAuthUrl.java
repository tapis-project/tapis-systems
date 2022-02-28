package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.GlobusAuthUrl;

public final class RespGlobusAuthUrl extends RespAbstract
{
    public RespGlobusAuthUrl(GlobusAuthUrl result) { this.result = result;}
    
    public GlobusAuthUrl result;
}
