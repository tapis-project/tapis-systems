package edu.utexas.tacc.tapis.systems.model;

/*
 * Class for Globus auth data needed to obtain Globus credentials
 */
public final class GlobusAuthInfo
{
  private final String url;
  private final String sessionId;

  public GlobusAuthInfo(String u, String s) { url = u; sessionId = s; }

  public String getUrl() { return url; }
  public String getSessionId() { return sessionId; }
}
