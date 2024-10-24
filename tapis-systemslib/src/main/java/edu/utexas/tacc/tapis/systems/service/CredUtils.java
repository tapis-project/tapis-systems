package edu.utexas.tacc.tapis.systems.service;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.*;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.*;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.s3.S3Connection;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.SystemShare;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import static edu.utexas.tacc.tapis.systems.model.Credential.*;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.*;

/*
   Utility class containing Tapis credential related methods needed by the
   service implementation.
 */
public class CredUtils
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(CredUtils.class);

  // Connection timeouts for SKClient
  static final int SK_READ_TIMEOUT_MS = 20000;
  static final int SK_CONN_TIMEOUT_MS = 20000;

  // NotAuthorizedException requires a Challenge, although it serves no purpose here.
  private static final String NO_CHALLENGE = "NoChallenge";
  // String used to detect that credentials are the problem when creating an SSH connection
  private static final String NO_MORE_AUTH_METHODS = "No more authentication methods available";

  // Permission constants
  // Permspec format for systems is "system:<tenant>:<perm_list>:<system_id>"
  public static final String PERM_SPEC_TEMPLATE = "system:%s:%s:%s";
  static final String PERM_SPEC_PREFIX = "system";
  // Sets of individual permissions, for convenience
  static final Set<Permission> ALL_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY, Permission.EXECUTE));
  private static final Set<Permission> READMODIFY_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  private static final Set<Permission> EXECUTE_PERMS = new HashSet<>(Set.of(Permission.EXECUTE));

  // Sharing constants
  static final String OP_SHARE = "share";
  static final String OP_UNSHARE = "unShare";
  static final Set<String> PUBLIC_USER_SET = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"
  static final String SYS_SHR_TYPE = "system";

  // Named and typed null values to make it clear what is being passed in to a method
  private static final String nullOwner = null;
  private static final String nullImpersonationId = null;
  private static final String nullSharedAppCtx = null;
  private static final String nullTargetUser = null;
  private static final Set<Permission> nullPermSet = null;
  private static final SystemShare nullSystemShare = null;
  private static final Credential nullCredential = null;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private ServiceClients serviceClients;
  @Inject
  private SysUtils sysUtils;

  // Wrapper for TmsKeys info.
  public record TmsKeys(String privateKey, String publicKey, String fingerprint) {}

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /* **************************************************************************** */
  /*                                Package-Private Methods                       */
  /* **************************************************************************** */

  /**
   * Get credential for given system, target user and authn method
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
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param targetUser - Target user for operation. May be Tapis user or host user
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @return populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  Credential getCredentialForUser(ResourceRequestUser rUser, TSystem system, String targetUser, AuthnMethod authnMethod)
          throws TapisException
  {
    String systemId = system.getId();
    // Set flag indicating if effectiveUserId is static
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If authnMethod not passed in fill in with default from system
    if (authnMethod == null)
    {
      AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(rUser.getOboTenantId(), systemId);
      if (defaultAuthnMethod == null)
      {
        String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
        log.info(msg);
        throw new NotFoundException(msg);
      }
      authnMethod = defaultAuthnMethod;
    }

    return getCredential(rUser, system, targetUser, authnMethod, isStaticEffectiveUser, null);
  }

  /**
   * Store or update credential for given system and target user.
   *
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   *
   * Secret path depends on whether effUser type is dynamic or static
   *
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user and the Credential may contain the optional attribute *loginUser* which will be used to map the
   * Tapis user to a username to be used when accessing the system. If the login user is not provided then there is
   * no mapping and the Tapis user is always used when accessing the system.
   *
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   *
   * For a dynamic TSystem (effUsr=$apiUsr) if targetUser is not the same as the Tapis user and a loginUser has been
   * provided then a loginUser mapping is created.
   *
   * If createTmsKeys is true then system must be of type LINUX.
   * System must also have a dynamic effectiveUserId and loginUser mapping is not allowed.
   * This is for security reasons. Without these restrictions anyone could create a TMS-enabled system and login
   *   to the TMS-enabled as someone other than their Tapis user id.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param targetUser - Target user for operation
   * @param cred - Credentials to be stored
   * @param createTmsKeys - Indicates if TMS keys should be created and stored
   * @param skipCheck - Indicates if cred check should happen (for LINUX, S3)
   * @param rawData - Client provided text used to create the credential - secrets should be scrubbed. Saved in update record.
   * @return null if skipping credCheck, else checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  Credential createCredentialForUser(ResourceRequestUser rUser, TSystem system, String targetUser,
                                     Credential cred, boolean createTmsKeys, boolean skipCheck, String rawData)
          throws TapisException, IllegalStateException
  {
    SystemOperation op = SystemOperation.setCred;
    Credential retCred = null;
    // Extract some attributes for convenience and clarity
    String oboTenant = rUser.getOboTenantId();
    String loginUser = cred.getLoginUser();
    String systemId = system.getId();
    SystemType systemType = system.getSystemType();

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If private SSH key is set check that we have a compatible key.
    if (!StringUtils.isBlank(cred.getPrivateKey()) && !cred.isValidPrivateSshKey())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2", rUser, systemId, targetUser);
      log.warn(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }

    // If TMS keys requested check that system allows for it and create the keys
    // Note that we must create the keys in the TMS server before verifying the credentials.
    TmsKeys tmsKeys = null;
    if (createTmsKeys)
    {
      if (!SystemType.LINUX.equals(systemType))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_INVALID_SYS_TYPE", rUser, systemId, systemType);
        throw new BadRequestException(msg);
      }
      if (!StringUtils.isBlank(loginUser) || isStaticEffectiveUser)
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NOT_ALLOWED", rUser, systemId, loginUser, isStaticEffectiveUser);
        throw new BadRequestException(msg);
      }
      tmsKeys = createTmsKeys(rUser, system); // TODO
    }

    // Skip check if not LINUX or S3
    if (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)) skipCheck = true;

    // ---------------- Verify credentials ------------------------
    // If not skipping credential validation then do it now
    if (!skipCheck)
    {
      retCred = verifyCredentials(rUser, system, cred, loginUser, system.getDefaultAuthnMethod());
      // If call returns null credential or null validation result then something went wrong.
      if (retCred == null || retCred.getValidationResult() == null) return retCred;
      // Check result. If validation failed return now.
      if (Boolean.FALSE.equals(retCred.getValidationResult())) return retCred;
    }

    // Create credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    try
    {
      createCredential(rUser, cred, tmsKeys, systemId, targetUser, isStaticEffectiveUser);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // If dynamic and an alternate loginUser has been provided that is not the same as the Tapis user
    //   then record the mapping
    if (!isStaticEffectiveUser && !StringUtils.isBlank(loginUser) && !targetUser.equals(loginUser))
    {
      dao.createOrUpdateLoginUserMapping(oboTenant, systemId, targetUser, loginUser);
    }

    // Construct Json string representing the update, with actual secrets masked out
    Credential maskedCredential = Credential.createMaskedCredential(cred);
    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredCreate(systemId, targetUser, skipCheck, maskedCredential);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);

    return retCred;
  }

  /**
   * Check user credential using given authnMethod or system default authnMethod.
   * <p>
   * Secret path depends on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user.
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param targetUser - Target user for operation
   * @param authnMethod - (optional) check credentials for specified authn method instead of default authn method
   * @return Checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  Credential checkCredentialForUser(ResourceRequestUser rUser, TSystem system, String targetUser,
                                    AuthnMethod authnMethod, SystemOperation op)
          throws TapisException, TapisClientException, IllegalStateException
  {
    String oboTenant = rUser.getOboTenantId();
    String systemId = system.getId();

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If authnMethod not passed in fill in with default from system
    if (authnMethod == null)
    {
      AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(oboTenant, systemId);
      if (defaultAuthnMethod == null)
        throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_AUTHN_METHOD_NOT_FOUND", rUser, systemId));
      authnMethod = defaultAuthnMethod;
    }

    // ---------------- Fetch credentials ------------------------
    // Use private internal method instead of public API to skip auth and other checks not needed here.
    Credential cred = getCredential(rUser, system, targetUser, authnMethod, isStaticEffectiveUser, null);
    if (cred == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op, systemId, system.getSystemType(),
              targetUser, authnMethod.name());
      log.info(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }
    // ---------------- Verify credentials using defaultAuthnMethod --------------------
    return verifyCredentials(rUser, system, cred, cred.getLoginUser(), authnMethod);
  }

  /**
   * Delete credential for given system and user
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system Tapis system
   * @param targetUser - Target user for operation
   * @throws TapisException - for Tapis related exceptions
   */
  int deleteCredentialForUser(ResourceRequestUser rUser, TSystem system, String targetUser, SystemOperation op)
          throws TapisException
  {
    String systemId = system.getId();
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // Delete credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    int changeCount;
    try
    {
      changeCount = deleteCredential(rUser, systemId, targetUser, isStaticEffectiveUser);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // If dynamic then remove any mapping from loginUser to tapisUser
    if (!isStaticEffectiveUser)
    {
      dao.deleteLoginUserMapping(rUser, rUser.getOboTenantId(), systemId, targetUser);
    }

    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredDelete(systemId, targetUser);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
    return changeCount;
  }

  /**
   * Verify that effectiveUserId can connect to the system using provided credentials and authnMethod
   * <p>
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   * <p>
   * If loginUser is set then use it for connection,
   * else if effectiveUserId is ${apUserId} then use rUser.oboUser for connection
   * else use static effectiveUserId from TSystem for connection
   * <p>
   * TSystem and Credential must be provided. If authnMethod not provided it is taken from the System.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param cred - credentials to check
   * @param loginUser - host login user from mapping
   * @param authnMethod - AuthnMethod to verify
   * @throws IllegalStateException - if credentials not verified
   */
  Credential verifyCredentials(ResourceRequestUser rUser, TSystem tSystem1, Credential cred,
                               String loginUser, AuthnMethod authnMethod)
          throws TapisException
  {
    String op = "verifyCredentials";
    // We must have the system and credentials to check.
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (tSystem1 == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    if (cred == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_CRED1", rUser));

    // If authnMethod not passed in fill in with default from system.
    if (authnMethod == null) authnMethod = tSystem1.getDefaultAuthnMethod();
    // Should always have an authnMethod by now, but just in case
    if (authnMethod == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_CRED2", rUser));

    SystemType systemType = tSystem1.getSystemType();
    String systemId = tSystem1.getId();
    // Determine user to check
    // None of the public methods that call this support impersonation so use null for impersonationId
    String effectiveUser;
    if (!StringUtils.isBlank(loginUser)) effectiveUser = loginUser;
    else effectiveUser = sysUtils.resolveEffectiveUserId(tSystem1, rUser.getOboUserId());

    // Make sure it is supported for the system type
    if (SystemType.GLOBUS.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      // Not supported. Return now.
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, effectiveUser, authnMethod);
      log.info(msg);
      return new Credential(AuthnMethod.PKI_KEYS, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
              cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
              cred.getAccessToken(), cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(),
              cred.getTmsFingerprint(), cred.getCertificate(), Boolean.FALSE, msg);
    }
    return verifyConnection(rUser, op, tSystem1, authnMethod, cred, effectiveUser);
  }

  /*
   * Create or update a credential using SKClient.
   * No checks are done for incoming arguments and the system must exist
   *
   * When the Systems service calls SK to create secrets it calls with a JWT as itself,
   *   jwtTenantId = admin tenant (Site Tenant Admin)
   *   jwtUserId = TapisConstants.SERVICE_NAME_SYSTEMS ("systems")
   *   and AccountType = TapisThreadContext.AccountType.service
   *
   * For Systems the secret needs to be scoped by the tenant associated with the system,
   *   the system id, the target user (i.e. the user associated with the secret) and
   *   whether the effectiveUserId is static or dynamic.
   *   This provides for separate namespaces for the two cases, so there will be no conflict if a static
   *      user and dynamic (i.e. ${apiUserId}) user happen to have the same value.
   *
   * The target user may be a Tapis user or login user associated with the host.
   * Secrets for a system follow the format
   *   secret/tapis/tenant/<tenant_id>/<system_id>/user/<static|dynamic>/<target_user>/<key_type>/S1
   * where tenant_id, system_id, user_id, key_type and <static|dynamic> are filled in at runtime.
   *   key_type is sshkey, password, accesskey, token, tmskey or cert
   *   and S1 is the reserved SecretName associated with the Systems.
   * Hence, the following code
   *     new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME)
   *     sParms.setSysId(systemId).setSysUser(targetUserPath)
   *     skClient.writeSecret(reqPayloadTenant, getServiceUserId(), sParms);
   *
   * In the SKClient code the tenant value in SKSecretWriteParms is ignored.
   * See method writeSecret(String tenant, String user, SKSecretWriteParms parms) in SKClient.java
   * SK uses tenant from payload when constructing the full path for the secret. User from payload not used.
   */
  void createCredential(ResourceRequestUser rUser, Credential credential, TmsKeys tmsKeys,
                        String systemId, String targetUser, boolean isStatic)
          throws TapisClientException, TapisException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    // Construct basic SK secret parameters including tenant, system and Tapis user for credential
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    // Fill in systemId and targetUserPath for the path to the secret.
    String targetUserPath = getTargetUserSecretPath(targetUser, isStatic);

    sParms.setSysId(systemId).setSysUser(targetUserPath);
    Map<String, String> dataMap;
    // Check for each secret type and write values if they are present
    // Note that multiple secrets may be present.
    // NOTE: For secrets of type "system" the oboUser in the writeSecret() calls is not used in the path,
    //       but SK requires that it be set. The oboTenant is used in the path for the secret.

    // Store password if present
    if (!StringUtils.isBlank(credential.getPassword()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.password);
      dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
      sParms.setData(dataMap);
      // First 2 parameters correspond to tenant and user from request payload
      // Tenant is used in constructing full path for secret, user is not used.
      sysUtils.getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store PKI keys if both present
    if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.sshkey);
      dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
      dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
      sParms.setData(dataMap);
      sysUtils.getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store Access key and secret if both present
    if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.accesskey);
      dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
      dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
      sParms.setData(dataMap);
      sysUtils.getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store Access token and Refresh token if both present
    if (!StringUtils.isBlank(credential.getAccessToken()) && !StringUtils.isBlank(credential.getRefreshToken()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.token);
      dataMap.put(SK_KEY_ACCESS_TOKEN, credential.getAccessToken());
      dataMap.put(SK_KEY_REFRESH_TOKEN, credential.getRefreshToken());
      sParms.setData(dataMap);
      sysUtils.getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // TODO Store TmsKeys if both public and private keys are present
    if (!StringUtils.isBlank(tmsKeys.privateKey) && !StringUtils.isBlank(tmsKeys.publicKey))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.tmskey);
      dataMap.put(SK_KEY_TMS_PUBLIC_KEY, tmsKeys.publicKey);
      dataMap.put(SK_KEY_TMS_PRIVATE_KEY, tmsKeys.privateKey);
      dataMap.put(SK_KEY_TMS_FINGERPRINT, tmsKeys.fingerprint);
      sParms.setData(dataMap);
      sysUtils.getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // NOTE if necessary handle ssh certificate when supported
  }

  /**
   * Delete a credential
   * No checks are done for incoming arguments and the system must exist
   */
  int deleteCredential(ResourceRequestUser rUser, String systemId, String targetUser, boolean isStatic)
          throws TapisClientException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();

    // Determine targetUserPath for the path to the secret.
    String targetUserPath = getTargetUserSecretPath(targetUser, isStatic);

    // Return 0 if credential does not exist
    var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
    sMetaParms.setTenant(oboTenant).setUser(oboUser);
    sMetaParms.setSysId(systemId).setSysUser(targetUserPath);
    // NOTE: To be sure we know that the secret does not exist we need to check each key type
    //       By default keyType is sshkey which may not exist
    boolean secretNotFound = true;
    sMetaParms.setKeyType(KeyType.password);
    try { sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.token);
    try { sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.tmskey);
    try { sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    if (secretNotFound) return 0;

    // Construct basic SK secret parameters and attempt to destroy each type of secret.
    // If destroy attempt throws an exception then log a message and continue.
    sMetaParms.setKeyType(KeyType.password);
    try { sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.token);
    try { sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.tmskey);
    try { sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    return 1;
  }

  /**
   * Get a credential given system, targetUser, isStatic and authnMethod
   * No checks are done for incoming arguments and the system must exist
   * resourceTenant used when a service is calling as itself and needs to specify the tenant for the resource
   */
  Credential getCredential(ResourceRequestUser rUser, TSystem system, String targetUser,
                           AuthnMethod authnMethod, boolean isStaticEffectiveUser, String resourceTenant)
          throws TapisException
  {
    String oboTenant = StringUtils.isBlank(resourceTenant) ? rUser.getOboTenantId() : resourceTenant;
    String oboUser = rUser.getOboUserId();
    String systemId = system.getId();

    // If authnMethod not passed in fill in with default from system
    if (authnMethod == null) authnMethod = system.getDefaultAuthnMethod();

    /*
     * When the Systems service calls SK to read secrets it calls with a JWT as itself,
     *   jwtTenantId = admin tenant (Site Tenant Admin)
     *   jwtUserId = TapisConstants.SERVICE_NAME_SYSTEMS ("systems")
     *   and AccountType = TapisThreadContext.AccountType.service
     *
     * For Systems the secret needs to be scoped by the tenant associated with the system,
     *   the system id, the target user (i.e. the user associated with the secret) and
     *   whether the effectiveUserId is static or dynamic.
     *   This provides for separate namespaces for the two cases, so there will be no conflict if a static
     *      user and dynamic (i.e. ${apiUserId}) user happen to have the same value.
     * The target user may be a Tapis user or login user associated with the host.
     * Secrets for a system follow the format
     *   secret/tapis/tenant/<tenant_id>/<system_id>/user/<static|dynamic>/<target_user>/<key_type>/S1
     * where tenant_id, system_id, user_id, key_type and <static|dynamic> are filled in at runtime.
     *   key_type is sshkey, password, accesskey, token, tmskey or cert
     *   and S1 is the reserved SecretName associated with the Systems.
     *
     * Hence, the following code
     *     new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME)
     *     sParms.setTenant(rUser.getOboTenantId()).setSysId(systemId).setSysUser(targetUserPath);
     *
     */
    Credential credential = null;
    try
    {
      // Construct basic SK secret parameters
      // Establish secret type ("system") and secret name ("S1")
      var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);

      // Fill in systemId and targetUserPath for the path to the secret.
      String targetUserPath = getTargetUserSecretPath(targetUser, isStaticEffectiveUser);

      // Set tenant, system and user associated with the secret.
      // These values are used to build the vault path to the secret.
      sParms.setTenant(oboTenant).setSysId(systemId).setSysUser(targetUserPath);

      // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
      sParms.setUser(oboUser);
      // Set key type based on authn method
      if (authnMethod.equals(AuthnMethod.PASSWORD))        sParms.setKeyType(KeyType.password);
      else if (authnMethod.equals(AuthnMethod.PKI_KEYS))   sParms.setKeyType(KeyType.sshkey);
      else if (authnMethod.equals(AuthnMethod.ACCESS_KEY)) sParms.setKeyType(KeyType.accesskey);
      else if (authnMethod.equals(AuthnMethod.TOKEN))      sParms.setKeyType(KeyType.token);
      else if (authnMethod.equals(AuthnMethod.TMS_KEYS))   sParms.setKeyType(KeyType.tmskey);
      else if (authnMethod.equals(AuthnMethod.CERT))       sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      SkSecret skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
      if (skSecret == null) return null;
      Map<String, String> dataMap = skSecret.getSecretMap();
      if (dataMap == null) return null;

      // Determine the loginUser associated with the credential.
      // If static or dynamic and there is no mapping then it is targetUser
      //   else look up mapping
      String loginUser;
      if (isStaticEffectiveUser)
      {
        loginUser = targetUser;
      }
      else
      {
        // This is the dynamic case, so targetUser must be a Tapis user.
        // See if the target Tapis user has a mapping to a host login user.
        String mappedLoginUser = dao.getLoginUser(oboTenant, systemId, targetUser);
        // If so then the mapped value becomes loginUser, else loginUser=targetUser
        if (!StringUtils.isBlank(mappedLoginUser))
          loginUser = mappedLoginUser;
        else
          loginUser = targetUser;
      }

      // Create a credential
      credential = new Credential(authnMethod, loginUser,
              dataMap.get(SK_KEY_PASSWORD),
              dataMap.get(SK_KEY_PRIVATE_KEY),
              dataMap.get(SK_KEY_PUBLIC_KEY),
              dataMap.get(SK_KEY_ACCESS_KEY),
              dataMap.get(SK_KEY_ACCESS_SECRET),
              dataMap.get(SK_KEY_ACCESS_TOKEN),
              dataMap.get(SK_KEY_REFRESH_TOKEN),
              dataMap.get(SK_KEY_TMS_PRIVATE_KEY), // TODO
              dataMap.get(SK_KEY_TMS_PUBLIC_KEY), // TODO
              dataMap.get(SK_KEY_TMS_FINGERPRINT), // TODO
              null); //dataMap.get(CERT) NOTE: get ssh certificate when supported
    }
    catch (TapisClientException tce)
    {
      // If tapis client exception then log error but continue so null is returned.
      log.warn(tce.toString());
      credential = null;
    }
    return credential;
  }

  /**
   * Build a TapisSystem client credential based on the TSystem model credential
   * Needed for shared code that expects to use the java wrapper client generated credential model.
   *
   * @param cred Credential from Systems service model object
   * @param authnMethod Authentication method
   * @return TapisSystem Java wrapper client credential
   */
  static edu.utexas.tacc.tapis.systems.client.gen.model.Credential buildAuthnCred(Credential cred, AuthnMethod authnMethod)
  {
    if (cred == null) return null;
    var c = new edu.utexas.tacc.tapis.systems.client.gen.model.Credential();
    // Convert the service enum to the client enum.
    var am = EnumUtils.getEnum(AuthnEnum.class, authnMethod.name());
    c.setAuthnMethod(am);
    c.setAccessKey(cred.getAccessKey());
    c.setAccessSecret(cred.getAccessSecret());
    c.setPassword(cred.getPassword());
    c.setPublicKey(cred.getPublicKey());
    c.setPrivateKey(cred.getPrivateKey());
    c.setAccessToken(cred.getAccessToken());
    c.setRefreshToken(cred.getRefreshToken());
    c.setTmsPublicKey(cred.getTmsPublicKey());
    c.setTmsPrivateKey(cred.getTmsPrivateKey());
    c.setTmsFingerprint(cred.getTmsFingerprint());
    c.setCertificate(cred.getCertificate());
    c.setLoginUser(cred.getLoginUser());
    return c;
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /*
   * Call the TMS server to generate a TMS keypair and fingerprint.
   */
  private TmsKeys createTmsKeys(ResourceRequestUser rUser, TSystem system)
  {
    // TODO
    return null;
  }

  /*
   * Verify connection based on authentication method
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   */
  private Credential verifyConnection(ResourceRequestUser rUser, String op, TSystem tSystem1, AuthnMethod authnMethod,
                                      Credential cred, String effectiveUser)
  {
    log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_START", rUser, tSystem1.getId(), tSystem1.getSystemType(),
             effectiveUser, authnMethod));
    Credential retCred;
    String systemId = tSystem1.getId();
    String host = tSystem1.getHost();
    int port = tSystem1.getPort();
    SystemType systemType = tSystem1.getSystemType();
    String bucket = tSystem1.getBucketName();
    // For convenience and clarity, set a few booleans
    boolean doingLinux = AuthnMethod.PKI_KEYS.equals(authnMethod) || AuthnMethod.PASSWORD.equals(authnMethod);
    boolean doingPki = AuthnMethod.PKI_KEYS.equals(authnMethod);
    boolean doingPassword = AuthnMethod.PASSWORD.equals(authnMethod);
    boolean doingAccessKey = AuthnMethod.ACCESS_KEY.equals(authnMethod);
    // TODO/TBD: Add support for TMS_KEYS?
    String msg = "No Errors";
    String validationResult;
    if ((doingLinux && !SystemType.LINUX.equals(systemType)) || (doingAccessKey && !SystemType.S3.equals(systemType)))
    {
      // System is not LINUX. Not supported.
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, effectiveUser, authnMethod);
      retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
              cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
              cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
              cred.getCertificate(), Boolean.FALSE, msg);
      validationResult = "FAILED";
    }
    else if ((doingPki && (StringUtils.isBlank(cred.getPublicKey()) || StringUtils.isBlank(cred.getPrivateKey()))) ||
            (doingPassword && StringUtils.isBlank(cred.getPassword())) ||
            (doingAccessKey && (StringUtils.isBlank(cred.getAccessKey()) || StringUtils.isBlank(cred.getAccessSecret()))))
    {
      // We do not have the credentials we need
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op, systemId, systemType, effectiveUser, authnMethod);
      retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
              cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
              cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
              cred.getCertificate(), Boolean.FALSE, msg);
      validationResult = "FAILED";
    }
    else
    {
      // Make the connection attempt
      // Try to handle as many exceptions as we can. For this reason, in each case there is a final catch of Exception
      //   which is re-thrown as a TapisException.
      log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_CONN", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
              effectiveUser, port, authnMethod));
      TapisException te = null;
      switch(authnMethod)
      {
        case PASSWORD:
          try (SSHConnection c = new SSHConnection(host, port, effectiveUser, cred.getPassword())) { te = null; }
          catch (TapisException e) { te = e; }
          catch (Exception e) { te = new TapisException(e.getMessage(), e); }
          break;
        case PKI_KEYS:
          try (SSHConnection c = new SSHConnection(host, port, effectiveUser, cred.getPublicKey(), cred.getPrivateKey())) { te = null; }
          catch (TapisException e) { te = e; }
          catch (Exception e) { te = new TapisException(e.getMessage(), e); }
          break;
        case ACCESS_KEY:
          try (S3Connection c = new S3Connection(host, port, bucket, effectiveUser, cred.getAccessKey(), cred.getAccessSecret()))
          {
            // For S3 we need to actually try to use the connection to know that the credentials are valid.
            String testKey = PathUtils.getAbsoluteKey(tSystem1.getRootDir(), "thisKeyIsUnlikelyToExistButIfItDoesThatIsOkay");
            S3Client client = c.getClient();
            try
            {
              HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(testKey).build();
              client.headObject(req);
            }
            catch (NoSuchKeyException ex) { /* This indicates credentials are valid */ }
            // An S3 exception containing a status of 403 indicates invalid credentials?
            catch (S3Exception e) { throw new TapisException(e.getMessage(), e); }
            catch (Exception e) { throw new TapisException(e.getMessage(), e); }
          }
          catch (TapisException e)
          {
            te = e;
          }
          break;
        default:
          // We should never get here, but just in case fail the verification
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, effectiveUser, authnMethod);
          log.error(msg);
          return new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                  cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                  cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
                  cred.getCertificate(), Boolean.FALSE, msg);
      }

      // We have made the connection attempt. Check the result.
      if (te == null)
      {
        validationResult = "SUCCESS";
        // No problem with connection. Set result to TRUE
        retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
                cred.getCertificate(), Boolean.TRUE, null);
      }
      else
      {
        //
        // There was a problem. Try to figure out why. Set result to FALSE
        //
        validationResult = "FAILED";
        Throwable cause = te.getCause();
        String eMsg = te.getMessage();
        if (te instanceof TapisSSHAuthException && cause != null && cause.getMessage().contains(NO_MORE_AUTH_METHODS))
        {
          // There was a special message in an SSH connection exception indicating credentials invalid.
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_VALID_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                  effectiveUser, authnMethod, cause.getMessage());
        }
        else if (cause instanceof S3Exception && Response.Status.FORBIDDEN.getStatusCode() == ((S3Exception) cause).statusCode())
        {
          // S3 connections return status of 403 when credentials invalid.
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_VALID_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                  effectiveUser, authnMethod, cause.getMessage());
        }
        else
        {
          // There was a general connection failure that we do not specifically detect.
          // Are there any other special messages for S3 or SSH?
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                  effectiveUser, authnMethod, eMsg);
        }
        retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
                cred.getCertificate(), Boolean.FALSE, msg);
      }
    }
    log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_END", rUser, tSystem1.getId(), tSystem1.getSystemType(),
            effectiveUser, authnMethod, validationResult, msg));
    return retCred;
  }

  /*
   * Return segment of secret path for target user, including static or dynamic scope
   * Note that SK uses + rather than / to create sub-folders.
   */
  static private String getTargetUserSecretPath(String targetUser, boolean isStatic)
  {
    return String.format("%s+%s", isStatic ? "static" : "dynamic", targetUser);
  }
}
