package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import org.glassfish.hk2.api.Factory;

/**
 * HK2 Factory class providing a ServiceClients singleton instance for the systems service.
 * Binding happens in SystemsApplication.java
 */
public class ServiceClientsFactory implements Factory<ServiceClients>
{
  @Override
  public ServiceClients provide() { return ServiceClients.getInstance(); }
  @Override
  public void dispose(ServiceClients c) {}
}
