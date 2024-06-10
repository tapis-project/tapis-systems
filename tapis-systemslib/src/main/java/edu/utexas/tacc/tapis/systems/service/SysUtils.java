package edu.utexas.tacc.tapis.systems.service;

import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.globusproxy.client.GlobusProxyClient;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.*;

/*
   Utility class containing general use methods needed by the service implementation.
 */
public class SysUtils
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(SysUtils.class);

  // Connection timeouts for SKClient
  static final int SK_READ_TIMEOUT_MS = 20000;
  static final int SK_CONN_TIMEOUT_MS = 20000;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private ServiceClients serviceClients;

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /* **************************************************************************** */
  /*                                Package-Private Methods                       */
  /* **************************************************************************** */

  /**
   * Determine the user to be used to access the system.
   * Determine effectiveUserId for static and dynamic (i.e. ${apiUserId}) cases.
   * If effectiveUserId is dynamic then resolve it
   * Take into account loginUser mapping.
   * @param system - the system in question
   * @param tapisUser - tapis user associated with login, this is the oboUser or impersonationId
   * @return Resolved value for effective user.
   */
  String resolveEffectiveUserId(TSystem system, String tapisUser)
          throws TapisException
  {
    String systemId = system.getId();
    String tenant = system.getTenant();
    String effUser = system.getEffectiveUserId();
    // Incoming effectiveUserId should never be blank but for robustness handle that case.
    if (StringUtils.isBlank(effUser)) return effUser;

    // If a static string (i.e. not ${apiUserId} then simply return the string
    if (!effUser.equals(APIUSERID_VAR)) return effUser;

    // At this point we know we have a dynamic effectiveUserId. Figure it out.
    // Determine the loginUser associated with the credential
    // Now see if there is a mapping from that Tapis user to a different login user on the host
    String loginUser = dao.getLoginUser(tenant, systemId, tapisUser);

    // If a mapping then return it, else return oboUser/impersonationId
    return (!StringUtils.isBlank(loginUser)) ? loginUser : tapisUser;
  }

  /**
   * Get GlobusProxy client associated with specified tenant
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @return GlobusProxy client
   * @throws TapisException - for Tapis related exceptions
   */
  GlobusProxyClient getGlobusProxyClient(ResourceRequestUser rUser) throws TapisException
  {
    GlobusProxyClient globusProxyClient;
    String tenantName;
    String userName;
    // If service request then use oboTenant and oboUser in OBO headers
    // else for user request use authenticated username and tenant in OBO headers
    if (rUser.isServiceRequest())
    {
      tenantName = rUser.getOboTenantId();
      userName = rUser.getOboUserId();
    }
    else
    {
      tenantName = rUser.getJwtTenantId();
      userName = rUser.getJwtUserId();
    }
    try
    {
      globusProxyClient = serviceClients.getClient(userName, tenantName, GlobusProxyClient.class);
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_GLOBUSPROXY, tenantName, userName);
      throw new TapisException(msg, e);
    }
    if (globusProxyClient == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SVC_CLIENT_NULL", rUser, TapisConstants.SERVICE_NAME_GLOBUSPROXY, tenantName, userName);
      throw new TapisException(msg);
    }
    return globusProxyClient;
  }

  /**
   * Get Security Kernel client with obo tenant and user set to the service tenant and user.
   * I.e. this is a client where the service calls SK as itself.
   * Note: Systems service always calls SK as itself.
   * Note: The ServiceClients class does caching
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  SKClient getSKClient(ResourceRequestUser rUser) throws TapisException
  {
    SKClient skClient;
    String oboUser = getServiceUserId();
    String oboTenant = getServiceTenantId();
    try { skClient = serviceClients.getClient(oboUser, oboTenant, SKClient.class); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, oboTenant, oboUser);
      throw new TapisException(msg, e);
    }
    if (skClient == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SVC_CLIENT_NULL", rUser, TapisConstants.SERVICE_NAME_SECURITY, oboTenant, oboUser);
      throw new TapisException(msg);
    }
    skClient.setReadTimeout(SK_READ_TIMEOUT_MS);
    skClient.setConnectTimeout(SK_CONN_TIMEOUT_MS);
    return skClient;
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

}
