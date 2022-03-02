package edu.utexas.tacc.tapis.systems.model;

/*
 * Class for GlobusAuthUrl data needed to obtain Globus credentials
 */
public final class GlobusAuthUrl
{
  private final String url;
  private final String sessionId;

  public GlobusAuthUrl(String u, String s) { url = u; sessionId = s; }

  public String getUrl() { return url; }
  public String getSessionId() { return sessionId; }
}
