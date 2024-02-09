package edu.utexas.tacc.tapis.systems.model;

/*
 * Class for Globus auth data needed to obtain Globus credentials
 */
public final class GlobusAuthInfo
{
  private final String url;
  private final String sessionId;
  private final String systemId;

  public GlobusAuthInfo(String url1, String sessionId1, String systemId1)
  {
    url = url1;
    sessionId = sessionId1;
    systemId = systemId1;
  }

  public String getUrl() { return url; }
  public String getSessionId() { return sessionId; }
  public String getSystemId() { return systemId; }
}
