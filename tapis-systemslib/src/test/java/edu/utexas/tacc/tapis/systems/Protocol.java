package edu.utexas.tacc.tapis.systems;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;

/*
 * Protocol contains info for testing convenience
 */
public final class Protocol
{
  private final AuthnMethod authnMethod; // How access authorization is handled.
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public Protocol(AuthnMethod authnMethod1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1)
  {
    authnMethod = authnMethod1;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public AuthnMethod getAuthnMethod() { return authnMethod; }
  public int getPort() { return port; }
  public boolean isUseProxy() { return useProxy; }
  public String getProxyHost() { return proxyHost; }
  public int getProxyPort() { return proxyPort; }
}
