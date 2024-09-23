package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.globusproxy.client.GlobusProxyClient;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.AuthTokens;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.ResultGlobusAuthInfo;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.*;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;

/*
 * Service level methods for System credentials.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 * TODO manage CredInfo records
 */
@Service
public class CredentialsServiceImpl
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(CredentialsServiceImpl.class);

  // Message keys
  static final String NOT_FOUND = "SYSLIB_NOT_FOUND";

  // Named and typed null values to make it clear what is being passed in to a method
  private static final String nullOwner = null;
  private static final Set<TSystem.Permission> nullPermSet = null;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private AuthUtils authUtils;
  @Inject
  private CredUtils credUtils;
  @Inject
  private SysUtils sysUtils;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Store or update credential for given system and target user.
   * <p>
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   * <p>
   * Required: rUser, systemId, targetUser, credential.
   * <p>
   * Secret path depends on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user and the Credential may contain the optional attribute *loginUser* which will be used to map the
   * Tapis user to a username to be used when accessing the system. If the login user is not provided then there is
   * no mapping and the Tapis user is always used when accessing the system.
   * <p>
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   * <p>
   * For a dynamic TSystem (effUsr=$apiUsr) if targetUser is not the same as the Tapis user and a loginUser has been
   * provided then a loginUser mapping is created.
   * <p>
   * System must exist and not be deleted.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @param cred - Credentials to be stored
   * @param skipCredCheck - Indicates if cred check should happen (for LINUX, S3)
   * @param rawData - Client provided text used to create the credential - secrets should be scrubbed. Saved in update record.
   * @return null if skipping credCheck, else checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  public Credential createUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, Credential cred,
                                         boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.setCred;

    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser) || cred == null)
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // We will need some info from the system, so fetch it now.
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // If system does not exist or has been deleted then throw an exception
    if (system == null)
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use utility method to do most of the work
    return credUtils.createCredentialForUser(rUser, system, targetUser, cred,  skipCredCheck, rawData);
  }

  /**
   * Delete credential for given system and user
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @throws TapisException - for Tapis related exceptions
   */
  public int deleteUserCredential(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.removeCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // If system does not exist or has been deleted then return 0 changes
    if (system == null) return 0;

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use utility method to remove SK records and CredInfo record
    return credUtils.deleteCredentialForUser(rUser, system, targetUser, op);
  }

  /**
   * Check user credential using given authnMethod or system default authnMethod.
   * Required: rUser, systemId, targetUser
   * <p>
   * Secret path depends on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user.
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   * <p>
   * System must exist and not be deleted.
   *  TODO/TBD - sync CredInfo record
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @param authnMethod - (optional) check credentials for specified authn method instead of default authn method
   * @return Checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  public Credential checkUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, TSystem.AuthnMethod authnMethod)
          throws TapisException, TapisClientException, IllegalStateException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.checkCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // We will need some info from the system, so fetch it now.
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // If system does not exist or has been deleted then throw an exception
    if (system == null)
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use utility method to do most of the work
    return credUtils.checkCredentialForUser(rUser, system, targetUser, authnMethod, op);
  }

  /**
   * Get credential for given system, target user and authn method
   * Only certain services are authorized.
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is
   * interpreted as a Tapis user. Note that their may me a mapping of the Tapis user to a host *loginUser*.
   * <p>
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the host *loginUser* that is used when accessing the host.
   * <p>
   * Another way to view static vs dynamic secrets in SK:
   *   If effUsr is static, then secrets stored using the "static" path in SK and static string used to build the path.
   *   If effUsr is dynamic, then secrets stored using the "dynamic" path in SK and a Tapis user
   *      (oboUser or impersonationId) used to build the path.
   * <p>
   * Desired authentication method may be specified using query parameter authnMethod=<method>. If desired
   * authentication method not specified then credentials for the system's default authentication method are returned.
   * <p>
   * The result includes the attribute *authnMethod* indicating the authentication method associated with
   * the returned credentials.
   *  TODO/TBD - sync CredInfo record
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation. May be Tapis user or host user
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @return populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  public Credential getUserCredential(ResourceRequestUser rUser, String systemId, String targetUser,
                                      TSystem.AuthnMethod authnMethod)
          throws TapisException, TapisClientException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.getCred;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // We will need some info from the system, so fetch it.
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // If system does not exist or has been deleted then return null
    if (system == null) return null;

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    // Use utility method to do most of the work
    return credUtils.getCredentialForUser(rUser, system, targetUser, authnMethod);
  }

  // ------------------------------------------------------------------------
  //                         Globus
  // ------------------------------------------------------------------------
  /**
   * Obtain a URL+SessionId that can be used to obtain a Globus Native App Authorization Code associated
   * with given system.
   * The clientId must be configured as a runtime setting.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId System - GlobusProxy requires endpoint/collection ID - stored as host in system definition
   * @return URL to be used for obtaining a Globus Native App Authorization Code
   * @throws TapisException - for Tapis related exceptions
   */
  public GlobusAuthInfo getGlobusAuthInfo(ResourceRequestUser rUser, String systemId)
          throws NotFoundException, TapisException, TapisClientException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.getGlobusAuthInfo;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NULL_INPUT_SYS", rUser, systemId));

    // Get clientId configured for Tapis. If none throw an exception
    String clientId = RuntimeParameters.getInstance().getGlobusClientId();
    if (StringUtils.isBlank(clientId))
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NOCLIENT", rUser, op.name()));

    // We will need info from system, so fetch it now
    // If system does not exist or has been deleted then throw an exception
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, false);
    if (system == null)
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

    // Call Tapis GlobusProxy service and create a GlobusAuthInfo from the client response;
    ResultGlobusAuthInfo r = sysUtils.getGlobusProxyClient(rUser).getAuthInfo(clientId, system.getHost());

    // Check that we got something reasonable.
    if (r == null) throw new TapisException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NULL", rUser, op.name()));
    if (StringUtils.isBlank(r.getUrl()) || StringUtils.isBlank(r.getSessionId()))
    {
      String url = StringUtils.isBlank(r.getUrl()) ? "<empty>" : r.getUrl();
      String sessId = StringUtils.isBlank(r.getSessionId()) ? "<empty>" : r.getSessionId();
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NO_URL", rUser, url, sessId));
    }
    return new GlobusAuthInfo(r.getUrl(), r.getSessionId(), systemId);
  }

  /**
   * Given a Tapis system, user and Globus auth code generate a pair of access
   * and refresh tokens. Then save them to SK for the given user and system.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Id of system
   * @param userName - Target user for operation
   * @param authCode - Globus Native App Authorization Code
   * @param sessionId - Id tracking the oauth2 flow started with the call to getGlobusAuthInfo
   * @throws TapisException - for Tapis related exceptions
   */
  public void generateAndSaveGlobusTokens(ResourceRequestUser rUser, String systemId, String userName,
                                          String authCode, String sessionId)
          throws NotFoundException, TapisException, TapisClientException
  {
    TSystem.SystemOperation op = TSystem.SystemOperation.setAccessRefreshTokens;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NULL_INPUT_SYS", rUser, systemId));

    if (StringUtils.isBlank(userName) || StringUtils.isBlank(authCode) || StringUtils.isBlank(sessionId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NULL_INPUT_TOKENS", rUser,
              userName, authCode, sessionId));

    // Get clientId configured for Tapis. If none throw an exception
    String clientId = RuntimeParameters.getInstance().getGlobusClientId();
    if (StringUtils.isBlank(clientId))
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NOCLIENT", rUser, op.name()));

    // We will need info from system, so fetch it now
    // If system does not exist or has been deleted then throw an exception
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, false);
    if (system == null)
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

    // ------------------------- Check service level authorization -------------------------
    authUtils.checkAuth(rUser, op, systemId, system.getOwner(), userName, null);

    // Call Tapis GlobuxProxy service to get tokens
    GlobusProxyClient globusClient = sysUtils.getGlobusProxyClient(rUser);
    AuthTokens authTokens = globusClient.getTokens(clientId, sessionId, authCode);
    // Check that we got something reasonable.
    if (authTokens == null) throw new TapisException(LibUtils.getMsgAuth("SYSLIB_GLOBUS_NULL", rUser, op.name()));
    String accessToken = authTokens.getAccessToken();
    String refreshToken = authTokens.getRefreshToken();

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // Create credential and save to SK
    Credential credential = new Credential(null, null, null, null, null, null, null, accessToken, refreshToken, null);
    try
    {
      credUtils.createCredential(rUser, credential, systemId, userName, isStaticEffectiveUser);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Construct Json string representing the update, with actual secrets masked out
    Credential maskedCredential = Credential.createMaskedCredential(credential);
    String updateJsonStr = TapisGsonUtils.getGson().toJson(maskedCredential);

    // Create a record of the update
    String updateText = null;
    dao.addUpdateRecord(rUser, systemId, op, updateJsonStr, updateText);
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

}
