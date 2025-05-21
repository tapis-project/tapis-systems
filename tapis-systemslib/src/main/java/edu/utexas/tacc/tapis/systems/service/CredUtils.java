package edu.utexas.tacc.tapis.systems.service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.BadRequestException;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import okhttp3.*;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.*;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.s3.S3Connection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.*;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

import static edu.utexas.tacc.tapis.systems.model.Credential.*;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.NOT_FOUND;

/*
   Utility class containing Tapis credential related methods needed by the
   service implementation and maintenance task.
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

  // TMS server configuration
  public static boolean tmsEnabled = false;
  private static String tmsServerUrl;
  private static String tmsServerReqUrl;
  private static String tmsTenant;
  private static String tmsClientId;
  private static String tmsClientSecret;
  public static final String TMS_CREATEKEYS_ENDPOINT = "v1/tms/pubkeys/creds";
  public static final String TMS_GETPUBKEY_ENDPOINT = "v1/tms/pubkeys/creds/retrieve";
  public static final String TMS_KEY_TYPE_RSA = "rsa";
  public static final String TMS_KEY_TYPE_ED25519 = "ed25519";

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

  // Http client used to call TMS server
  private static final OkHttpClient httpClient = new OkHttpClient();

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private SysUtils sysUtils;

  // Global ConcurrentHashMap.newKeySet() used as in-memory records for CredentialInfo objects that
  //   also serve as mutexes.
  Map<String,CredentialInfo> credInfoConcurrentMap = new ConcurrentHashMap<>();

  // Wrapper for TmsKeys info.
  public record TmsKeys(String privateKey, String publicKey, String fingerprint) {}

  // Wrapper for TmsRequest info used when creating a key pair
  public record TmsRequest(String client_user_id, String host, String host_account,
                           String key_type, int num_uses, int ttl_minutes) {}

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /* **************************************************************************** */
  /*                                Package-Private Methods                       */
  /* **************************************************************************** */

  /*-------------------------------------------------------------------------*/
  /*                 Methods for Credentials/SK                              */
  /*-------------------------------------------------------------------------*/

  /**
   * Get credential for given system, target user and authentication method
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
// TODO/TBD Should we check and create/update CredentialInfo record as part of this?
    return getCredential(rUser, system, targetUser, authnMethod, isStaticEffectiveUser, null);
  }

  /**
   * Store or update credential for given system and target user.
   * Optionally verify the credential. If verification fails, credentials are not registered.
   * Return null if skipping cred check, else return checked credential with validation result set
   * NOTE: Instead of returning null we should always return a cred. Make validation result an enum
   *       instead of boolean. The enum values could be PASS, FAIL, SKIPPED (TBD: and ERROR? and UNSET?)
   * <p>
   * NOTE that a credential is returned even if validation fails. Caller must check Credential.getValidationResult()
   * Path to secrets in SK depend on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user and the Credential may contain the optional attribute *loginUser* which will be used to map the
   * Tapis user to a username to be used when accessing the system. If the login user is not provided then there is
   * no mapping and the Tapis user is always used when accessing the system.
   * Note that what we call the Tapis user comes from the username claim in the Tapis JWT.
   * <p>
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   * <p>
   * For a dynamic TSystem (effUsr=$apiUsr) if targetUser is not the same as the Tapis user and a loginUser has been
   * provided then a loginUser mapping is created.
   * <p>
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
          throws TapisException
  {
    SystemOperation op = SystemOperation.setCred;
    Credential retCred; // The full Credential that is returned, including TMS keys info if generated.
    String msg;
    // Extract some attributes for convenience and clarity
    String credLoginUserMapping = cred.getLoginUser(); // Host login mapping from provided credential
    String systemId = system.getId();
    String sysTenant = system.getTenant();
    SystemType systemType = system.getSystemType();
    AuthnMethod sysAuthnMethod = system.getDefaultAuthnMethod();
    String sysHost = system.getHost();

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If TMS keys requested check that system allows for it, create the keys and add the keys to the Credential
    // Note that we must create the keys in the TMS server before verifying the credentials.
    if (createTmsKeys)
    {
      // Make sure we are configured for TMS keys and that system allows for it
      validateTmsConfig(rUser, sysTenant, systemId, systemType, credLoginUserMapping, isStaticEffectiveUser);
      // Call TMS to create the keypair and fingerprint
      TmsKeys tmsKeys = createTmsKeys(rUser, system, targetUser);
      // Add TMS keys info to the full credential
      retCred = new Credential(cred.getAuthnMethod(), cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                               cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
                               cred.getAccessToken(), cred.getRefreshToken(),
                               tmsKeys.privateKey, tmsKeys.publicKey, tmsKeys.fingerprint, cred.getCertificate());
    }
    else
    {
      // No TMS keys, create the retCred based on the credential passed in
      retCred = new Credential(cred.getAuthnMethod(), cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                               cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
                               cred.getAccessToken(), cred.getRefreshToken(), cred.getTmsPrivateKey(),
                               cred.getTmsPublicKey(), cred.getTmsFingerprint(), cred.getCertificate());
    }

    // Skip check if not LINUX or S3
    if (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)) skipCheck = true;

    // Determine hostLoginUser resulting from the update.
    String hostLoginUser = determineHostloginUser(system, targetUser, credLoginUserMapping, isStaticEffectiveUser);

    // ---------------- Verify credentials ------------------------
    // If not skipping credential validation then do it now
    if (!skipCheck)
    {
      // When creating a cred requesting user does not specify authMethod, so use the one from the system.
      retCred = verifyCredentials(rUser, system, retCred, hostLoginUser, sysAuthnMethod);
      // If call returns null credential or null validation result then something went very wrong.
      if (retCred == null || retCred.getValidationResult() == null)
      {
        msg = LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_ERROR", rUser,
                                  systemId, systemType, sysHost, hostLoginUser, sysAuthnMethod);
        throw new WebApplicationException(msg);
      }
      // Check result. If validation failed return now.
      if (Boolean.FALSE.equals(retCred.getValidationResult())) return retCred;
    }

    // Create credential. Create or update SK records and CredentialInfo record
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    createCredential(rUser, retCred, system, targetUser, hostLoginUser, isStaticEffectiveUser, op);

    // If skipping check return null, else return the verified credential
    if (skipCheck) return null;
    else return retCred;
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
          throws TapisException, IllegalStateException
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
    // If no credentials then we cannot check, treat it as an error
    if (cred == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op, systemId, system.getSystemType(),
              targetUser, authnMethod.name());
      log.info(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }
    //  TODO CredInfo record updates
    // ---------------- Verify credentials using defaultAuthnMethod --------------------
    // Determine hostLoginUser.
    String hostLoginUser;
    //  If static use targetUser, else dynamic so use call to resolveEffUsr
    if (isStaticEffectiveUser)
    {
      hostLoginUser = targetUser;
    }
    else
    {
      // Dynamic eff user, there may be a mapping. Note that targetUser is interpreted as a Tapis user.
      hostLoginUser = sysUtils.resolveEffectiveUserId(system, targetUser);
    }
    // Check credentials
// TODO Should we check and create/update CredentialInfo record as part of this?
    return verifyCredentials(rUser, system, cred, hostLoginUser, authnMethod);
  }

  /**
   * Delete credential for given system and user
   * Remove SK records and CredentialInfo record
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
      // Remove SK records and CredentialInfo record
    changeCount = deleteCredential(rUser, system, targetUser, isStaticEffectiveUser);

    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredDelete(systemId, targetUser);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
    return changeCount;
  }

  /**
   * Verify that hostLoginUser can connect to the system using provided credentials and authnMethod
   * May be called during sysCreate, credCreate. Always called during credVerify.
   * <p>
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   * <p>
   * TSystem, credential and hostLoginUser must be provided. If authnMethod not provided it is taken from the System.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param cred - credentials to check
   * @param hostLoginUser - username for connection
   * @param authnMethod - AuthnMethod to verify
   * @throws IllegalStateException - if credentials not verified
   */
  Credential verifyCredentials(ResourceRequestUser rUser, TSystem tSystem1, Credential cred, String hostLoginUser,
                               AuthnMethod authnMethod)
  {
    String op = "verifyCredentials";
    // We must have the system and credentials to check.
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (tSystem1 == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    if (cred == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_CRED1", rUser));
    if (StringUtils.isBlank(hostLoginUser)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_HOST_LOGIN", rUser));

    // If authnMethod not passed in fill in with default from system.
    if (authnMethod == null) authnMethod = tSystem1.getDefaultAuthnMethod();
    // Should always have an authnMethod by now, but just in case
    if (authnMethod == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_CRED2", rUser));

    SystemType systemType = tSystem1.getSystemType();
    String systemId = tSystem1.getId();

    // Make sure it is supported for the system type
    if (SystemType.GLOBUS.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      // Not supported. Return now.
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, hostLoginUser, authnMethod);
      log.info(msg);
      return new Credential(cred.getAuthnMethod(), cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
              cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
              cred.getAccessToken(), cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(),
              cred.getTmsFingerprint(), cred.getCertificate(), Boolean.FALSE, msg);
    }
    return verifyConnection(rUser, op, tSystem1, authnMethod, cred, hostLoginUser);
  }

  /*
   * Create or update a credential using SKClient.
   * Write credentials to SK and create or update the CredentialInfo in DB and in-memory.
   * If operation is not System.create then record update in SYSTEMS_UPDATE table
   *
   * No checks are done for incoming arguments and the system must exist
   */
  void createCredential(ResourceRequestUser rUser, Credential credential, TSystem system, String targetUser,
                        String hostLoginUser, boolean isStatic, SystemOperation op)
  {
    String oboUser = rUser.getOboUserId();
    String loginUserMapping = credential.getLoginUser();
    // Use a synchronized method to make sure we have a DB record and in-memory object for the CredInfo record.
    // If record does not already exist in memory or in DB then create it with status of PENDING
    // The CredentialInfo record returned is already locked. This ensures we have exclusive access (BUT must unlock)
    CredentialInfo credInfo = addCredInfoRecordAndLock(rUser, system, oboUser, hostLoginUser, loginUserMapping, isStatic);
    // Now we have a locked record so no other threads will attempt an update during this update
    // This is basically the equivalent of a selectForUpdate DB type operation.
    // Note that this also synchronizes SK operations, which is good. Before this, multiple concurrent SK operations
    // were possible.
    try
    {
      // Update status to IN_PROGRESS. Method will also update syncStatus of in-memory credInfo.
      updateCredentialInfoStatus(credInfo, SyncStatus.IN_PROGRESS);

      // Write secrets to SK and read from SK to update the in-memory CredentialInfo record
      syncCredentialInfoToSK(rUser, credential, credInfo, system, targetUser, isStatic);
      // If it is not a system create, then record the update
      if (!SystemOperation.create.equals(op))
      {
        // Construct Json string representing the update, with actual secrets masked out
        Credential maskedCredential = Credential.createMaskedCredential(credential);
        // Get a complete and succinct description of the update.
        String changeDescription = LibUtils.getChangeDescriptionCredCreate(system.getId(), targetUser, maskedCredential);
        // Create a record of the update
        String rawUpdateData = null;
        dao.addUpdateRecord(rUser, system.getId(), op, changeDescription, rawUpdateData);
      }
      // Update the credInfo record to COMPLETED. Also updates DB.
      updateCredentialInfoToCompleted(credInfo);
      // Log successful update
      String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_OK", rUser, credInfo.getTenant(),
              credInfo.getSystemId(), credInfo.getTapisUser(), credInfo.getLoginUserMapping(), credInfo.isStatic());
      log.debug(msg);
    }
    catch (TapisSecurityException tse)
    {
      // Issue with SK. Not much we can do.
      // Log error, update the credInfo record to FAILED and throw a runtime exception.
      String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_FAIL", rUser, credInfo.getTenant(),
            credInfo.getSystemId(), credInfo.getTapisUser(), credInfo.getLoginUserMapping(), credInfo.isStatic(),
            credInfo.getSyncFailCount(), tse.getMessage());
      log.error(msg);
      // Update the credInfo record to FAILED. Also updates DB.
      updateCredentialInfoToFailed(credInfo, tse.getMessage());
      throw new TapisRuntimeException(tse);
    }
    finally
    {
      // Unlock the record
      credInfo.mutex.unlock();
    }
  }

  /**
   * Delete a credential.
   * Remove CredentialInfo record and SK records.
   * No checks are done for incoming arguments and the system must exist
   */
  int deleteCredential(ResourceRequestUser rUser, TSystem system, String targetUser, boolean isStatic)
          throws TapisClientException, TapisException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String systemId = system.getId();
    int retCode;
    // Use a synchronized method to make sure we have a DB record and in-memory object
    // If not already in memory or in DB it is created with status of PENDING
    // The CredentialInfo record returned is already locked. This ensures we have exclusive access
    CredentialInfo credInfo = addCredInfoRecordAndLock(rUser, system.getSeqId(), oboTenant, systemId, oboUser, isStatic);

    // Now we have a locked record so no other threads will attempt an update during this update
    // This is basically the equivalent of a selectForUpdate DB type operation.
    // Note that this also synchronizes SK operations, which is good. Before this, multiple concurrent SK operations
    // were possible.
    try
    {
      // Update the status to DELETED. NOTE: Method will also update syncStatus of credInfo
      updateCredentialInfoStatus(credInfo, SyncStatus.DELETED);

      // Remove secrets from SK
      retCode = removeSKSecrets(rUser, system, targetUser, isStatic);

      // Log successful update
      String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_DEL", rUser, credInfo.getTenant(), systemId,
                                       credInfo.getTapisUser(), credInfo.getLoginUserMapping(), credInfo.isStatic());
      log.debug(msg);
    }
    finally
    {
      // Unlock the record
      credInfo.mutex.unlock();
    }
    return retCode;
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
              dataMap.get(SK_KEY_TMS_PRIVATE_KEY),
              dataMap.get(SK_KEY_TMS_PUBLIC_KEY),
              dataMap.get(SK_KEY_TMS_FINGERPRINT),
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

  /*-------------------------------------------------------------------------*/
  /*                 Methods for SYSTEMS_CRED_INFO table                     */
  /*-------------------------------------------------------------------------*/

  /**
   * Check the systems_cred_info table and update as needed
   * NOTE: This method should only be called at startup when there is only a single thread running.
   *  - Check that IN_PROGRESS to FAILED transition is allowed
   *  - Mark all IN_PROGRESS records as FAILED
   *  - Remove deleted records
   *  - Create records as needed for undeleted systems that have a static effectiveUserId
   */
    void credInfoInit() throws TapisException
  {
    // Mark all IN_PROGRESS records as FAILED
    // First check that transition is valid. If not valid then abort startup by throwing an exception.
//    transition = "NoSuchTransition"; // TODO temp, for testing
    CredInfoFSM.checkForAllowedTransition(SyncStatus.IN_PROGRESS, SyncStatus.FAILED);

    String failMsg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_MARK_FAILED_BEGIN");
    log.info(failMsg);
    int numRecords = dao.credInfoMarkInProgressAsFailed(failMsg);
    String msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_MARK_FAILED_END", numRecords);
    log.info(msg);

    // Remove any deleted records
    // Any deleted records can be removed at start-up. No other threads might be in the process of changing the state.
    msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_REMOVE_DELETED_BEGIN");
    log.info(msg);
    numRecords = dao.credInfoRemoveDeletedRecords();
    msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_REMOVE_DELETED_END", numRecords);
    log.info(msg);

    // Create records as needed for undeleted systems that have a static effectiveUserId
    msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_STATIC_BEGIN");
    log.info(msg);
    numRecords = dao.credInfoInitStaticSystems();
    msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_STATIC_END", numRecords);
    log.info(msg);
  }

  /*
   * Return segment of secret path for target user, including static or dynamic scope
   * Note that SK uses + rather than / to create sub-folders.
   */
  static String getTargetUserSecretPath(String targetUser, boolean isStatic)
  {
    return String.format("%s+%s", isStatic ? "static" : "dynamic", targetUser);
  }
  /*
   * Check to see if TMS is configured. Set flag.
   */
  public static void initTmsConfiguration()
  {
    RuntimeParameters runtimeParms = RuntimeParameters.getInstance();
    tmsEnabled = runtimeParms.getTmsEnalbed();
    tmsServerUrl = runtimeParms.getTmsServerUrl();
    tmsTenant = runtimeParms.getTmsTenant();
    tmsClientId = runtimeParms.getTmsClientId();
    tmsClientSecret = runtimeParms.getTmsClientSecret();
    // String to log for secret, if it is set log the first and last 3 characters of the string
    String tmsClientSecretMasked = "";
    if (!StringUtils.isBlank(tmsClientSecret))
    {
      // Secret is set. Trim whitespace
      String trimmedSecret = tmsClientSecret.trim();
      int secretLen = trimmedSecret.length();
      // Make sure we have enough characters so we mask at least a few characters
      if (secretLen > 10)
      {
        tmsClientSecretMasked =
                String.format("%s***%s", trimmedSecret.substring(0, 3), trimmedSecret.substring(secretLen - 3));
      }
      else tmsClientSecretMasked = SECRETS_MASK;
    }
    if (tmsEnabled && !StringUtils.startsWith(tmsServerUrl, "http"))
    {
      System.out.println(LibUtils.getMsg("SYSLIB_INIT_TMS_URL_ERR", tmsServerUrl));
      tmsEnabled = false;
    }
    tmsServerReqUrl = String.format("%s/%s", tmsServerUrl, TMS_CREATEKEYS_ENDPOINT);
    // Log final result
    System.out.println(LibUtils.getMsg("SYSLIB_INIT_TMS_CFG", tmsEnabled, tmsServerUrl, tmsTenant, tmsClientId,
                                       tmsClientSecretMasked));
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Determine final host login user value when caller has provided a credential
   * @param sys - Tapis system
   * @param targetUser - target user associated with the create operation
   * @param credHostLoginUser - login user mapping (if any) provided as part of credential.
   * @param isStaticEffectiveUser - whether eff user is static
   * @return host login user
   */
  private String determineHostloginUser(TSystem sys, String targetUser, String credHostLoginUser,
                                        boolean isStaticEffectiveUser)
  {
    // Determine hostLoginUser. If static or dynamic and no mapping, then use targetUser.
    String hostLoginUser = targetUser;
    // If dynamic need to check for host login user mapping.
    if (!isStaticEffectiveUser)
    {
      // Since this is a create operation, the host login user mapping might be in the DB or part of the incoming
      //   credential or both. The one in the credential has priority because it will be replacing the DB record
      String mappedLoginUser = credHostLoginUser;
      if (StringUtils.isBlank(mappedLoginUser)) mappedLoginUser = dao.getLoginUser(sys.getTenant(), sys.getId(), targetUser);
      // mappedLoginUser may or may not be blank. If not blank update the hostLoginUser.
      if (!StringUtils.isBlank(mappedLoginUser)) hostLoginUser = mappedLoginUser;
    }
    return hostLoginUser;
  }

  /*
   * Make sure we are configured for TMS keys and that system allows for it
   * Check:
   *  - TMS is allowed for tenant
   *  - we are configured for TMS
   *  - system type allows for TMS
   *  - there is no login user mapping
   *  - effectiveUserId is not static
   */
   private void validateTmsConfig(ResourceRequestUser rUser, String sysTenant, String sysId, SystemType sysType,
                                  String loginUserMapping, boolean isStaticEffUsr )
   {
     String msg;
     // Check if TMS is allowed for the tenant. Not all tenants are allowed to create TMS credentials
     if (!RuntimeParameters.getInstance().getTmsAllowedTenants().contains(sysTenant))
     {
       msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_TENANT_NOT_ALLOWED", rUser, sysTenant, sysId);
       throw new BadRequestException(msg);
     }
     // Make sure we are configured for TMS support
     if (!CredUtils.tmsEnabled)
     {
       msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NOT_CFG", rUser, sysId);
       throw new BadRequestException(msg);
     }
     if (!SystemType.LINUX.equals(sysType))
     {
       msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_INVALID_SYS_TYPE", rUser, sysId, sysType);
       throw new BadRequestException(msg);
     }
     if (!StringUtils.isBlank(loginUserMapping) || isStaticEffUsr)
     {
       msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NOT_ALLOWED", rUser, sysId, loginUserMapping, isStaticEffUsr);
       throw new BadRequestException(msg);
     }
   }

  /**
   * Call the TMS server to generate a TMS keypair and fingerprint.
   * Example: curl -k -X POST -H "content-type: application/json" \
   *           -H "X-TMS-TENANT: $TMS_TENANT"
   *           -H "X-TMS-CLIENT-ID: $TMS_CLIENT_ID" \
   *           -H "X-TMS-CLIENT-SECRET: $TMS_CLIENT_KEY" \
   *           $TMS_URL/v1/tms/pubkeys/creds -d @$1
   * Example req body
   * { "client_user_id": "testuser1", "host": "testhost1", "host_account": "testhostaccount1",
   *   "num_uses": -1, "ttl_minutes": -1}
   *
   * @param rUser ResourceRequest user
   * @param system Tapis system
   * @param targetUser Host account user
   * @return tms key info
   * @throws TapisException on error
   */
  private TmsKeys createTmsKeys(ResourceRequestUser rUser, TSystem system, String targetUser)
          throws TapisException
  {
    // Call TMS to generate the keypair and fingerprint
    // Example:
    //    tmsServerReqUrl = "https://tms-server-stage.tacc.utexas.edu:3000/v1/tms/pubkeys/creds";
    //    tmsTenant = "test";
    //    tmsClientId = "testclient1";
    //    tmsClientSecret = "secret1";
    //    String tmsClientUser = "testuser1";
    //    String tmsHost = "testhost1";
    //    String tmsHostAccount = "testhostaccount1";
    String tmsClientUser = rUser.getOboUserId();
    String tmsHost = system.getHost();
    String tmsHostAccount = targetUser;
    int numUses = -1;
    int ttlMinutes = -1;
    // Build the request
    var tmsRequest = new TmsRequest(tmsClientUser, tmsHost, tmsHostAccount, TMS_KEY_TYPE_ED25519, numUses, ttlMinutes);
    String reqJsonStr = TapisGsonUtils.getGson(true).toJson(tmsRequest);
    RequestBody body = RequestBody.create(reqJsonStr, MediaType.parse("application/json"));
    Request.Builder requestBuilder = new Request.Builder().url(tmsServerReqUrl).post(body);

    // Add headers for tenant, client id and client secret
    Request request = requestBuilder.addHeader("X-TMS-TENANT", tmsTenant)
            .addHeader("X-TMS-CLIENT-ID", tmsClientId)
            .addHeader("X-TMS-CLIENT-SECRET", tmsClientSecret)
            .build();
    Call call = httpClient.newCall(request);
    String msg = null;
    String respBodyStr = null;
    int httpRespCode = -1;
    try
    {
      // Send the request to the REST endpoint
      // Use try-with-resources to auto-close the response.
      log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_REQ", rUser, system.getId(), targetUser, tmsServerUrl));
      try (okhttp3.Response response = call.execute())
      {
        // Get the response body as a string
        if (response.body() != null) respBodyStr = response.body().string();
        // If response status code is not in the 200s it is an error
        httpRespCode = response.code();
        if (httpRespCode < 200 || httpRespCode >= 300)
        {
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_HTTP_ERR", rUser, system.getId(), targetUser,
                                    tmsServerUrl, httpRespCode, respBodyStr);
          log.error(msg);
        }
      }
    }
    catch (IOException e)
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_ERR", rUser, system.getId(), targetUser, tmsServerUrl, e.getMessage());
      log.error(msg);
    }

    // On error throw TapisException
    if (!StringUtils.isBlank(msg))
    {
      throw new TapisException(msg);
    }

    // If response body was empty or null it is an error
    if (StringUtils.isBlank(respBodyStr))
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NO_BODY", rUser, system.getId(), targetUser, tmsServerUrl, httpRespCode);
      log.error(msg);
      throw new TapisException(msg);
    }

    // We should have a json response body with the keypair and fingerprint. Extract them.
    JsonObject respBodyJson = TapisGsonUtils.getGson().fromJson(respBodyStr, JsonObject.class);
    var privateKeyObj = respBodyJson.get("private_key");
    var publicKeyObj = respBodyJson.get("public_key");
    var publicKeyFingerprintObj = respBodyJson.get("public_key_fingerprint");
    // If any are null it is an error
    if (privateKeyObj == null || publicKeyObj == null || publicKeyFingerprintObj == null)
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NULL_FIELD", rUser, system.getId(), targetUser, tmsServerUrl, httpRespCode,
                                 privateKeyObj == null ? "null" : "non-null",
                                 publicKeyObj == null ? "null" : "non-null",
                                 publicKeyFingerprintObj == null ? "null" : "non-null");
      log.error(msg);
      throw new TapisException(msg);
    }
    String tmsPrivateKey = privateKeyObj.getAsString();
    String tmsPublicKey = publicKeyObj.getAsString();
    String tmsPublicKeyFingerprint = publicKeyFingerprintObj.getAsString();
    String privateKeyMasked = StringUtils.isBlank(tmsPrivateKey) ? null : SECRETS_MASK;
    // If any are empty it is an error
    if (StringUtils.isBlank(tmsPrivateKey) || StringUtils.isBlank(tmsPublicKey) || StringUtils.isBlank(tmsPublicKeyFingerprint))
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_EMPTY_FIELD", rUser, system.getId(), targetUser, tmsServerUrl,
                                httpRespCode, privateKeyMasked, tmsPublicKey, tmsPublicKeyFingerprint);
      log.error(msg);
      throw new TapisException(msg);
    }

    // Log extracted data
    msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_DATA", rUser, system.getId(), targetUser, tmsServerReqUrl,
                              httpRespCode, privateKeyMasked, tmsPublicKey, tmsPublicKeyFingerprint);
    log.debug(msg);
    return new TmsKeys(tmsPrivateKey, tmsPublicKey, tmsPublicKeyFingerprint);
  }

  /*
   * Verify connection based on authentication method
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   */
  private Credential verifyConnection(ResourceRequestUser rUser, String op, TSystem tSystem1, AuthnMethod authnMethod,
                                      Credential cred, String hostLoginUser)
  {
    log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_START", rUser, tSystem1.getId(), tSystem1.getSystemType(),
             hostLoginUser, authnMethod));
    Credential retCred;
    String systemId = tSystem1.getId();
    String host = tSystem1.getHost();
    int port = tSystem1.getPort();
    SystemType systemType = tSystem1.getSystemType();
    String bucket = tSystem1.getBucketName();
    // For convenience and clarity, set a few booleans
    boolean doingLinux = AuthnMethod.PKI_KEYS.equals(authnMethod) ||
                         AuthnMethod.PASSWORD.equals(authnMethod) ||
                         AuthnMethod.TMS_KEYS.equals(authnMethod);
    boolean doingPki = AuthnMethod.PKI_KEYS.equals(authnMethod);
    boolean doingPassword = AuthnMethod.PASSWORD.equals(authnMethod);
    boolean doingAccessKey = AuthnMethod.ACCESS_KEY.equals(authnMethod);
    boolean doingTms = AuthnMethod.TMS_KEYS.equals(authnMethod);
    String msg = "No Errors";
    String validationResult;
    if ((doingLinux && !SystemType.LINUX.equals(systemType)) || (doingAccessKey && !SystemType.S3.equals(systemType)))
    {
      // System type not supported.
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, hostLoginUser, authnMethod);
      retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
              cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
              cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
              cred.getCertificate(), Boolean.FALSE, msg);
      validationResult = "FAILED";
    }
    else if ((doingPki && (StringUtils.isBlank(cred.getPublicKey()) || StringUtils.isBlank(cred.getPrivateKey()))) ||
            (doingPassword && StringUtils.isBlank(cred.getPassword())) ||
            (doingAccessKey && (StringUtils.isBlank(cred.getAccessKey()) || StringUtils.isBlank(cred.getAccessSecret()))) ||
            (doingTms && (StringUtils.isBlank(cred.getTmsPrivateKey()) || StringUtils.isBlank(cred.getTmsPublicKey()))))
    {
      // We do not have the credentials we need
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op, systemId, systemType, hostLoginUser, authnMethod);
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
              hostLoginUser, port, authnMethod));
      TapisException te = null;
      switch(authnMethod)
      {
        case PASSWORD:
          try (SSHConnection c = new SSHConnection(host, port, hostLoginUser, cred.getPassword())) { te = null; }
          catch (TapisException e) { te = e; }
          catch (Exception e) { te = new TapisException(e.getMessage(), e); }
          break;
        case PKI_KEYS:
          try (SSHConnection c = new SSHConnection(host, port, hostLoginUser, cred.getPublicKey(), cred.getPrivateKey())) { te = null; }
          catch (TapisException e) { te = e; }
          catch (Exception e) { te = new TapisException(e.getMessage(), e); }
          break;
        case ACCESS_KEY:
          try (S3Connection c = new S3Connection(host, port, bucket, hostLoginUser, cred.getAccessKey(), cred.getAccessSecret()))
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
        case TMS_KEYS:
          try (SSHConnection c = new SSHConnection(host, port, hostLoginUser, cred.getTmsPublicKey(), cred.getTmsPrivateKey())) { te = null; }
          catch (TapisException e) { te = e; }
          catch (Exception e) { te = new TapisException(e.getMessage(), e); }
          break;
        default:
          // We should never get here, but just in case fail the verification
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, hostLoginUser, authnMethod);
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
                  hostLoginUser, authnMethod, cause.getMessage());
        }
        else if (cause instanceof S3Exception && Response.Status.FORBIDDEN.getStatusCode() == ((S3Exception) cause).statusCode())
        {
          // S3 connections return status of 403 when credentials invalid.
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_VALID_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                  hostLoginUser, authnMethod, cause.getMessage());
        }
        else
        {
          // There was a general connection failure that we do not specifically detect.
          // Are there any other special messages for S3 or SSH?
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                  hostLoginUser, authnMethod, eMsg);
        }
        retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                cred.getRefreshToken(), cred.getTmsPrivateKey(), cred.getTmsPublicKey(), cred.getTmsFingerprint(),
                cred.getCertificate(), Boolean.FALSE, msg);
      }
    }
    log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_END", rUser, tSystem1.getId(), tSystem1.getSystemType(),
            hostLoginUser, authnMethod, validationResult, msg));
    return retCred;
  }

  /**
   * Update CredentialInfo status for in-memory and DB record
   * WARNING ***** CredInfo object MUST be locked before calling this method ****
   * Check that transition from current status to new status is allowed.
   */
  private void updateCredentialInfoStatus(CredentialInfo credInfo, SyncStatus newSyncStatus)
  {
    // CredInfo must be locked
    if (!credInfo.mutex.isLocked())
      throw new IllegalStateException(LibUtils.getMsg("SYSLIB_CRED_INFO_NOT_LOCKED_ERROR", "updateCredentialInfoStatus"));

    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(credInfo.getSyncStatus(), newSyncStatus);
    // Update CredInfo attributes
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    credInfo.setSyncStatus(newSyncStatus);
    // Persist the update
    dao.updateCredInfoStatus(credInfo, newSyncStatus, updated);
  }

  /*
   * Update CredentialInfo to COMPLETE for in-memory and DB record
   * WARNING ***** CredInfo object MUST be locked before calling this method ****
   * Check that transition from current status to COMPLETED is allowed.
   */
  private void updateCredentialInfoToCompleted(CredentialInfo credInfo)
  {
    // CredInfo must be locked
    if (!credInfo.mutex.isLocked())
      throw new IllegalStateException(LibUtils.getMsg("SYSLIB_CRED_INFO_NOT_LOCKED_ERROR", "updateCredentialInfoToCompleted"));

    SyncStatus newSyncStatus = SyncStatus.COMPLETED;
    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(credInfo.getSyncStatus(), newSyncStatus);

    // Update CredInfo attributes
    credInfo.setSyncFailCount(0);
    credInfo.setSyncFailMessage("");
    credInfo.setSyncFailed(null);
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setSyncStatus(newSyncStatus);
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    // Persist the update
    dao.updateCredInfoRecord(credInfo, updated);
  }

  /**
   * Update CredentialInfo to FAILED for in-memory and DB record
   * WARNING ***** CredInfo object MUST be locked before calling this method ****
   * Check that transition from current status to new status is allowed.
   */
  private void updateCredentialInfoToFailed(CredentialInfo credInfo, String errorMsg)
  {
    // CredInfo must be locked
    if (!credInfo.mutex.isLocked())
      throw new IllegalStateException(LibUtils.getMsg("SYSLIB_CRED_INFO_NOT_LOCKED_ERROR", "updateCredentialInfoToFailed"));

    SyncStatus newSyncStatus = SyncStatus.FAILED;
    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(credInfo.getSyncStatus(), newSyncStatus);
    // Update CredInfo attributes
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setSyncFailed(updated.toInstant(ZoneOffset.UTC));
    credInfo.setSyncFailMessage(errorMsg);
    credInfo.incrementSyncFailCount();
    credInfo.setSyncStatus(newSyncStatus);
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    // Persist the update
    dao.updateCredInfoRecord(credInfo, updated);
  }


//  /**
//   * TODO Synchronized method to ensure a CredentialInfo record is present in the DB and in-memory
//   * @param rUser - ResourceRequestUser containing tenant, user and request info
//   */
//  private void updateCredentialInfo(ResourceRequestUser rUser, CredentialInfo credInfo, CredentialInfo skCredInfo) throws TapisException
//  {
//    String oboTenant = rUser.getOboTenantId();
//    String oboUser = rUser.getOboUserId();
//    // TODO Transition the record to PENDING
//    // TODO/TBD But if this is the initial record, it will already be in PENDING state. Allow PENDING->PENDING?
//
//    // TODO/TBD: First check that transition is valid. If not valid then log error and throw exception
//    credInfoCheckTransition(credInfo, CredentialInfo.SyncStatus.IN_PROGRESS);
//    // TODO Now move to IN_PROGRESS
//    // TODO/TBD: First check that transition is valid. If not valid then log error and throw exception
//// ????
//    String transition = CredInfoFSM.InProgressToFailed;
//    transition = "NoSuchTransition"; // tODO temp, for testing
//    if (!CredInfoFSM.allowedEvents.contains(transition))
//    {
//      String msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FSM_INVALID_TRANSITION", transition);
//      log.error(msg);
//      throw new TapisException(msg);
//    }
//    String failMsg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_MARK_FAILED");
//    dao.credInfoMarkInProgressAsFailed(failMsg);
//// ????
//
//    // TODO Get latest CredentialInfo data from SK
//
//    // TODO Update CredentialInfo in DB and in-memory
//
//    // TODO Now move to COMPLETED
//    // TODO/TBD: First check that transition is valid. If not valid then log error and throw exception
//    // TODO
//  }

  /**
   * Method to write credentials to SK and update in-memory CredentialInfo object by reading data from SK.
   * Following CredentialInfo attributes need updating based on current SK data:
   *   hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys
   * <p>
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
   * <p>
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
   * <p>
   * In the SKClient code the tenant value in SKSecretWriteParms is ignored.
   * See method writeSecret(String tenant, String user, SKSecretWriteParms parms) in SKClient.java
   * SK uses tenant from payload when constructing the full path for the secret. User from payload not used.

   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param credential - the Credential
   * @param system - Tapis system
   * @param targetUser - User associated with the credential
   * @param isStatic - indicates if effectiveUserId is static or dynamic
   * @throws TapisSecurityException on SK error
   */
  private void syncCredentialInfoToSK(ResourceRequestUser rUser, Credential credential, CredentialInfo credInfo,
                                      TSystem system, String targetUser, boolean isStatic)
          throws TapisSecurityException
  {
    // Set some variables for convenience and clarity
    String oboUser = rUser.getOboUserId();
    String tenant = system.getTenant();
    String systemId = system.getId();
    AuthnMethod defaultAuthnMethod = system.getDefaultAuthnMethod();

    // Flags used for building CredentialInfo
    Boolean hasCredentials = null, hasPassword = null, hasPkiKeys = null, hasAccessKey = null, hasToken = null,
            hasTmsKeys = null;

    // Surround all SK related code in a try block. Catch any SK errors and throw a TapisSecurityException
    try
    {
      // Persist the credential data to SK
      // Construct basic SK secret parameters including tenant, system and Tapis user for credential
      // Establish secret type ("system") and secret name ("S1")
      var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      // Fill in systemId and targetUserPath for the path to the secret.
      String targetUserPath = getTargetUserSecretPath(targetUser, isStatic);
      sParms.setSysId(systemId).setSysUser(targetUserPath);
      // Map used to store secret data when writing to SK
      Map<String, String> dataMap;

      // NOTE: For secrets of type "system" the oboUser in the writeSecret() calls is not used in the path,
      //       but SK requires that it be set. The oboTenant is used in the path for the secret.
      // Check for each secret type and write values if they are present
      // Note that multiple secrets may be present.
      // Store password if present
      if (!StringUtils.isBlank(credential.getPassword()))
      {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.password);
        dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
        sParms.setData(dataMap);
        // First 2 parameters correspond to tenant and user from request payload
        // Tenant is used in constructing full path for secret, user is not used.
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasPassword = true;
      }
      // Store PKI keys if both present
      if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()))
      {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.sshkey);
        dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
        dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
        sParms.setData(dataMap);
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasPkiKeys = true;
      }
      // Store Access key and secret if both present
      if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret()))
      {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.accesskey);
        dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
        dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
        sParms.setData(dataMap);
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasAccessKey = true;
      }
      // Store Access token and Refresh token if both present
      if (!StringUtils.isBlank(credential.getAccessToken()) && !StringUtils.isBlank(credential.getRefreshToken()))
      {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.token);
        dataMap.put(SK_KEY_ACCESS_TOKEN, credential.getAccessToken());
        dataMap.put(SK_KEY_REFRESH_TOKEN, credential.getRefreshToken());
        sParms.setData(dataMap);
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasToken = true;
      }
      // Store TmsKeys if both public and private keys are present
      if (!StringUtils.isBlank(credential.getTmsPrivateKey()) && !StringUtils.isBlank(credential.getTmsPublicKey()))
      {
        dataMap = new HashMap<>();
        sParms.setKeyType(KeyType.tmskey);
        dataMap.put(SK_KEY_TMS_PUBLIC_KEY, credential.getTmsPublicKey());
        dataMap.put(SK_KEY_TMS_PRIVATE_KEY, credential.getTmsPrivateKey());
        dataMap.put(SK_KEY_TMS_FINGERPRINT, credential.getTmsFingerprint());
        sParms.setData(dataMap);
        String privKey = StringUtils.isBlank(credential.getTmsPrivateKey()) ? null : "*****";
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasTmsKeys = true;
      }
      // NOTE if necessary handle ssh certificate when supported

      // Determine CredentialInfo properties that are based on SK and not set above
      // For each case check to see if not set above. If not then read from SK and set it
      var sReadParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      sReadParms.setTenant(tenant).setSysId(systemId).setSysUser(targetUserPath);
      sReadParms.setUser(targetUser);
      SkSecret skSecret;
      // PASSWORD
      if (hasPassword == null)
      {
        sReadParms.setKeyType(KeyType.password);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasPassword = false;
        else {
          dataMap = skSecret.getSecretMap();
          if (dataMap == null) hasPassword = false;
          else hasPassword = !StringUtils.isBlank(dataMap.get(SK_KEY_PASSWORD));
        }
      }
      // PKI_KEYS
      if (hasPkiKeys == null)
      {
        sReadParms.setKeyType(KeyType.sshkey);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasPkiKeys = false;
        else
        {
          dataMap = skSecret.getSecretMap();
          if (dataMap == null) hasPkiKeys = false;
          else hasPkiKeys = !StringUtils.isBlank(dataMap.get(SK_KEY_PRIVATE_KEY));
        }
      }
      // ACCESS_KEY
      if (hasAccessKey == null)
      {
        sReadParms.setKeyType(KeyType.accesskey);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasAccessKey = false;
        else
        {
          dataMap = skSecret.getSecretMap();
          if (dataMap == null) hasAccessKey = false;
          else hasAccessKey = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_KEY));
        }
      }
      // TOKEN
      if (hasToken == null)
      {
        sReadParms.setKeyType(KeyType.token);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasToken = false;
        else
        {
          dataMap = skSecret.getSecretMap();
          if (dataMap == null) hasToken = false;
          else hasToken = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_TOKEN));
        }
      }
      // TMS_KEYS
      if (hasTmsKeys == null)
      {
        sReadParms.setKeyType(KeyType.tmskey);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasTmsKeys = false;
        else
        {
          dataMap = skSecret.getSecretMap();
          if (dataMap == null) hasTmsKeys = false;
          else hasTmsKeys = !StringUtils.isBlank(dataMap.get(SK_KEY_TMS_PRIVATE_KEY));
        }
      }
    }
    catch ( TapisException | TapisClientException te) { throw new TapisSecurityException(te); }

    // Determine if credentials are registered for defaultAuthnMethod of the system
    hasCredentials = (AuthnMethod.PASSWORD.equals(defaultAuthnMethod) && hasPassword) ||
            (AuthnMethod.PKI_KEYS.equals(defaultAuthnMethod) && hasPkiKeys) ||
            (AuthnMethod.ACCESS_KEY.equals(defaultAuthnMethod) && hasAccessKey) ||
            (AuthnMethod.TOKEN.equals(defaultAuthnMethod) && hasToken) ||
            (AuthnMethod.TMS_KEYS.equals(defaultAuthnMethod) && hasTmsKeys);

    // Update the in-memory CredentialInfo object
    credInfo.setHasCredentials(hasCredentials);
    credInfo.setHasPassword(hasPassword);
    credInfo.setHasPkiKeys(hasPkiKeys);
    credInfo.setHasAccessKey(hasAccessKey);
    credInfo.setHasToken(hasToken);
    credInfo.setHasTmsKeys(hasTmsKeys);
  }

  /**
   * Synchronized method to ensure a CredentialInfo record is present in the DB and in memory
   * To ensure calling thread has exclusive access, the CredentialInfo record is locked before being returned.
   *
   * Synchronizing this is a potential bottleneck, but we do not expect that much activity around updating credentials.
   * NOTE: **************************************************************************
   * NOTE: All callers must unlock the record when finished with it
   * NOTE: **************************************************************************
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - Tapis system
   * @param tapisUser - Tapis user
   * @param hostLoginUser - computed host login user TODO review
   * @param loginUserMapping - user mapping from Credential TODO review
   * @param isStatic - indicates if effectiveUserId is static or dynamic
   * @return the CredentialInfo record
   */
  private synchronized CredentialInfo addCredInfoRecordAndLock(ResourceRequestUser rUser, TSystem sys, String tapisUser,
                                                               String hostLoginUser, String loginUserMapping, boolean isStatic)
  {
    CredentialInfo credInfo;
    String key = String.format("%s:%s:%s:%s", sys.getTenant(), sys.getId(), tapisUser, isStatic);
    // Determine as fast as possible if we already have a record.
    if (credInfoConcurrentMap.containsKey(key))
    {
      // We already have it in memory, lock it and return
      credInfo = credInfoConcurrentMap.get(key);
      credInfo.mutex.lock();
      return credInfo;
    }
    // We do not already have an in-memory record. Look for record in DB.
    credInfo = dao.getCredInfo(rUser, sys.getTenant(), sys.getId(), tapisUser, isStatic);
    // If no record in DB then create in-memory record and DB record
    if (credInfo == null)
    {
      credInfo = new CredentialInfo(sys.getSeqId(), sys.getTenant(), tapisUser, sys.getId(), hostLoginUser,
                                    loginUserMapping, isStatic, SyncStatus.PENDING);
      credInfo = dao.createCredInfo(rUser, credInfo);
    }
    // We fetched it from the DB or just created it, now add it to the in-memory map, lock it and return
    credInfoConcurrentMap.put(key, credInfo);
    // Lock the record so calling thread has exclusive access
    credInfo.mutex.lock();
    return credInfo;
  }

  /**
   * Remove all secrets from SK for given user, tenant and system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param targetUser - User associated with the credential
   * @param isStatic - indicates if effectiveUserId is static or dynamic
   * @return 1 if secrets removed, 0 if no secrets removed
   */
  private int removeSKSecrets(ResourceRequestUser rUser, TSystem system, String targetUser, boolean isStatic)
          throws TapisClientException
  {
    // Set some variables for convenience and clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = system.getTenant();
    String systemId = system.getId();
    // Remove SK records
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
}
