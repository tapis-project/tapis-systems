package edu.utexas.tacc.tapis.systems.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import com.google.gson.JsonObject;
import com.opencsv.CSVReader;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecretVersionMetadata;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.s3.S3Connection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.migrate.CredInfoInitJob;
import edu.utexas.tacc.tapis.systems.model.CredInfoFSM;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.CredInfoFSM.CREDINFO_INIT_TMP_CSV_FILE;
import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_SECRET;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PASSWORD;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_REFRESH_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_TMS_FINGERPRINT;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_TMS_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_TMS_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.NOT_FOUND;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.nullLoginUserMapping;

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

  // NotAuthorizedException requires a Challenge, although it serves no purpose here.
  private static final String NO_CHALLENGE = "NoChallenge";
  // String used to detect that credentials are the problem when creating an SSH connection
  private static final String NO_MORE_AUTH_METHODS = "No more authentication methods available";

  public static final String TMS_CREATEKEYS_ENDPOINT = "v1/tms/pubkeys/creds";
  public static final String TMS_GETPUBKEY_ENDPOINT = "v1/tms/pubkeys/creds/retrieve";
  public static final String TMS_KEY_TYPE_RSA = "rsa";
  public static final String TMS_KEY_TYPE_ED25519 = "ed25519";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Http client used to call TMS server
  private static final OkHttpClient httpClient = new OkHttpClient();

  // TMS server configuration
  public static boolean tmsEnabled = false;
  private static String tmsServerUrl;
  private static String tmsServerReqUrl;
  private static String tmsTenant;
  private static String tmsClientId;
  private static String tmsClientSecret;

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private SysUtils sysUtils;

  // Wrapper for TmsKeys info.
  public record TmsKeys(String privateKey, String publicKey, String fingerprint) {}

  // Wrapper for TmsRequest info used when creating a key pair
  public record TmsRequest(String client_user_id, String host, String host_account,
                           String key_type, int num_uses, int ttl_minutes) {}

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /*
   * Given attributes read directly from Vault, create or update a CredInfo record.
   * Final status will be COMPLETED.
   *
   * NOTE/WARNING
   *   It is possible for the vault record to have isStatic=true even though the system is defined with
   *   effectiveUserId=${apiUserId}. This means the credential was created when isStatic=true and then
   *   the system definition was updated to have effectiveUserId=${apiUserId}. So we must detect this
   *   and set hostLoginUser to the static user registered at the time of credential creation.
   *
   * From the vault attributes we have some of the primary key values for table: tenant, sysId, isStatic
   * We also have values for the credential metadata, (has_credentials, has_pki_keys, etc.).
   *
   * But we still need to figure out values for tapisUser, hostLoginUser and loginUserMapping
   * tapisUser is fairly straightforward, see below. For others:
   *
   * Two cases:
   *    a. CredInfo in the DB:
   *         loginUserMapping : from DB, might be null
   *         hostLoginUser : if isStatic=true use effectiveUserId from system
   *                         if isStatic=false and loginUserMapping!=null, use loginUserMapping from the DB
   *                         if isStatic=false and loginUserMapping=null, use credTargetUser from the record
   *    b. CredInfo not in DB:
   *         loginUserMapping : not available, use null
   *         hostLoginUser : if isStatic=true use effectiveUserId from system
   *                         if isStatic=false use credTargetUser from the record
   */
  public CredentialInfo initCredInfoRecordFromVaultMetadata(ResourceRequestUser rUser, String tenant, TSystem sys,
                                                            boolean isStatic, CredInfoInitJob.SecretMetaInfo sm)
  {
    String opName = "createCredInfoRecordFromVaultMetadata";
    CredentialInfo credInfo;
    AuthnMethod authnMethod = sys.getDefaultAuthnMethod();
    String credTargetUser = sm.targetUser();
    // Determine if credentials are registered for defaultAuthnMethod of the system
    boolean hasCredentials = (AuthnMethod.PASSWORD.equals(authnMethod) && sm.hasPassword()) ||
          (AuthnMethod.PKI_KEYS.equals(authnMethod) && sm.hasPkiKeys()) ||
          (AuthnMethod.ACCESS_KEY.equals(authnMethod) && sm.hasAccessKey()) ||
          (AuthnMethod.TOKEN.equals(authnMethod) && sm.hasToken() ) ||
          (AuthnMethod.TMS_KEYS.equals(authnMethod) && sm.hasTmsKeys());





    int sysSeqId = sys.getSeqId();

    // Compute tapisUser, hostLoginUser and loginUserMapping
    String tapisUser, hostLoginUser, loginUserMapping;
    // tapisUser.
    // For dynamic always credTargetUser.
    // For static use system owner, that is who will most likely have registered the credential.
    //   In practice, if it was not the owner, but instead it was a tenant admin, for example, it should not matter
    //   since anyone using the system will get the credential for the static effUserId.
    if (!isStatic) tapisUser = credTargetUser; else tapisUser = sys.getOwner();

    // We are mutating a CredInfo record so synchronize around the class
    synchronized (CredUtils.class)
    {
      String msg;
      CredentialInfo credInfoDB = dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStatic);
      if (credInfoDB != null)
      {
        msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_VAULT", credInfoDB.getTenant(), credInfoDB.getSystemId(),
                              credInfoDB.getTapisUser(), credInfoDB.getHostLoginUser(), credInfoDB.isStatic(),
                              credInfoDB.getLoginUserMapping(), opName);
        log.warn(String.format("%s IN-DB", msg));
        // Record is already in the DB, set status to PENDING and then IN_PROGRESS.
        updateCredInfoStatus(rUser, credInfoDB, SyncStatus.PENDING, opName);
        updateCredInfoStatus(rUser, credInfoDB, SyncStatus.IN_PROGRESS, opName);
        // Compute loginUserMapping and hostLoginUser.
        loginUserMapping = credInfoDB.getLoginUserMapping();

        // Determine hostLoginUser
        if (isStatic && APIUSERID_VAR.equals(sys.getEffectiveUserId()))
        {
          // Exceptional case. Vault record is static but system has effectiveUserId = ${apiUserId} This means that
          //   although the system is currently dynamic we still need to use credTargetUser as the host login user.
          hostLoginUser = credTargetUser;
        }
        else
        {
          // Normal case. If static use system effUsr else use loginUserMapping or credTargetUser
          if (isStatic) hostLoginUser = sys.getEffectiveUserId();
          else hostLoginUser = (loginUserMapping != null) ? loginUserMapping : credTargetUser;
        }
        // We now have all attributes, use them to create a CredInfo record in memory
        credInfo = new CredentialInfo(sysSeqId, credInfoDB.getTenant(), credInfoDB.getSystemId(),
                                      tapisUser, isStatic, hostLoginUser, loginUserMapping,
                                      hasCredentials, sm.hasPassword(), sm.hasPkiKeys(), sm.hasAccessKey(), sm.hasToken(),
                                      sm.hasTmsKeys(), credInfoDB.getSyncStatus(), credInfoDB.getSyncFailCount(),
                                      credInfoDB.getSyncFailMessage(), credInfoDB.getSyncFailed(),
                                      credInfoDB.getCreated(), credInfoDB.getCreated());
        dao.updateCredInfoRecord(credInfo, null);
      }
      else
      {
        // No record in DB, create one with status of IN_PROGRESS
        // Compute loginUserMapping and hostLoginUser.
        loginUserMapping = null;
        // Determine hostLoginUser
        if (isStatic && APIUSERID_VAR.equals(sys.getEffectiveUserId()))
        {
          // Exceptional case. Vault record is static but system has effectiveUserId = ${apiUserId}. This means that
          //   although the system is currently dynamic we still need to use credTargetUser as the host login user.
          hostLoginUser = credTargetUser;
        }
        else
        {
          // Normal case. If static use system effUsr else use credTargetUser
          hostLoginUser = (isStatic) ? sys.getEffectiveUserId() : credTargetUser;
        }
        int syncFailCount = 0;
        String syncFailMsg = null;
        Instant syncFailTimestamp = null;
        Instant utcNow = TapisUtils.getUTCTimeNow().toInstant(ZoneOffset.UTC);
        // We now have all attributes, use them to create a CredInfo record in memory
        credInfo = new CredentialInfo(sysSeqId, tenant, sys.getId(), tapisUser, isStatic, hostLoginUser, loginUserMapping,
                                 hasCredentials, sm.hasPassword(), sm.hasPkiKeys(), sm.hasAccessKey(), sm.hasToken(),
                                 sm.hasTmsKeys(), SyncStatus.IN_PROGRESS, syncFailCount, syncFailMsg, syncFailTimestamp,
                                 utcNow, utcNow);
        msg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_VAULT", credInfo.getTenant(), credInfo.getSystemId(),
                              credInfo.getTapisUser(), credInfo.getHostLoginUser(), credInfo.isStatic(),
                              credInfo.getLoginUserMapping(), opName);
        log.warn(String.format("%s NOT-IN-DB", msg));
        credInfo = dao.createCredInfo(rUser, credInfo);
      }
      // Update record to COMPLETED
      updateCredInfoToCompleted(rUser, credInfo);
    }
    return credInfo;
  }

  /* **************************************************************************** */
  /*                                Package-Private Methods                       */
  /* **************************************************************************** */

  /*
   * Check to see if TMS is configured. Set flag.
   */
  static void initTmsConfiguration()
  {
    RuntimeParameters runtimeParms = RuntimeParameters.getInstance();
    tmsEnabled = runtimeParms.getTmsEnalbed();
    tmsServerUrl = runtimeParms.getTmsServerUrl();
    tmsTenant = runtimeParms.getTmsTenant();
    tmsClientId = runtimeParms.getTmsClientId();
    tmsClientSecret = runtimeParms.getTmsClientSecret();
    if (tmsClientSecret!=null) tmsClientSecret = tmsClientSecret.trim();
    // If enabled, do some validation of config
    if (tmsEnabled)
    {
      // Check that URL at least has a chance of working
      if (!Strings.CI.startsWith(tmsServerUrl, "http"))
      {
        System.out.println(LibUtils.getMsg("SYSLIB_INIT_TMS_URL_ERR", tmsServerUrl));
        tmsEnabled = false;
      }
      // Check that secret is configured
      if (StringUtils.isBlank(tmsClientSecret))
      {
        System.out.println(LibUtils.getMsg("SYSLIB_INIT_TMS_NO_SECRET_ERR"));
        tmsEnabled = false;
      }
    }
    // String to log for secret, if it is set log the first and last 3 characters of the string
    String tmsClientSecretMasked = "";
    if (!StringUtils.isBlank(tmsClientSecret))
    {
      // Secret is set.
      int secretLen = tmsClientSecret.length();
      // Make sure we have enough characters so we mask at least a few characters
      if (secretLen > 10)
      {
        tmsClientSecretMasked =
              String.format("%s***%s", tmsClientSecret.substring(0, 3), tmsClientSecret.substring(secretLen - 3));
      }
      else tmsClientSecretMasked = SECRETS_MASK;
    }
    tmsServerReqUrl = String.format("%s/%s", tmsServerUrl, TMS_CREATEKEYS_ENDPOINT);
    // Log final result
    System.out.println(LibUtils.getMsg("SYSLIB_INIT_TMS_CFG", tmsEnabled, tmsServerUrl, tmsTenant, tmsClientId,
          tmsClientSecretMasked));
  }

  /**
   * Get credential for given system, target user and authentication method
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *credTargetUser* is
   * interpreted as a Tapis user. Note that their may me a mapping of the Tapis user to a host *loginUser*.
   * <p>
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *credTargetUser* is interpreted
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
   * @param credTargetUser - Target user for operation. May be Tapis user or host user
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @return populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  Credential getCredentialForUser(ResourceRequestUser rUser, TSystem system, String credTargetUser, AuthnMethod authnMethod)
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
    return getCredential(rUser, system, credTargetUser, authnMethod, isStaticEffectiveUser, null);
  }

  /**
   * Store or update credential for given system and target user.
   * Optionally verify the credential. If verification fails, credentials are not registered.
   * Return null if skipping cred check, else return checked credential with validation result set
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   * <p>
   * NOTE Return null if we skip cred check.
   * <p>
   * Path to secrets in SK depend on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *credTargetUser* is interpreted
   * as a Tapis user and the Credential may contain the optional attribute *loginUser* which will be used to map the
   * Tapis user to a username to be used when accessing the system. If the login user is not provided then there is
   * no mapping and the Tapis user is always used when accessing the system.
   * Note that what we call the Tapis user comes from the username claim in the Tapis JWT.
   * <p>
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *credTargetUser* is interpreted
   * as the login user to be used when accessing the host.
   * <p>
   * A CredentialInfo record is created and persisted, unless skipCheck=false and the verification fails.
   * <p>
   * If createTmsKeys is true then system must:
   *    - be of type LINUX
   *    - have a dynamic effectiveUserId
   *    - NOT have a loginUser mapping
   * This is for security reasons. Without these restrictions anyone could create a TMS-enabled system and login
   *   to the TMS-enabled as someone other than their Tapis user id.
   * <p>
   * If createTmsKeys is false and defaultAuthnMethod for system is TMS then it is an error.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param credTargetUser - Target user for operation
   * @param cred - Credentials to be stored
   * @param createTmsKeys - Indicates if TMS keys should be created and stored
   * @param skipCheck - Indicates if cred check should happen (for LINUX, S3)
   * @param rawData - Client provided text used to create the credential - secrets should be scrubbed. Saved in update record.
   * @return null if skipping credCheck, else checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  Credential createCredentialForUser(ResourceRequestUser rUser, TSystem system, String credTargetUser,
                                     Credential cred, boolean createTmsKeys, boolean skipCheck, String rawData)
          throws TapisException
  {
    SystemOperation op = SystemOperation.setCred;
    Credential retCred; // Unless skipping credCheck, the full Credential that is returned, including TMS keys if generated.
    String msg;
    // Extract some attributes for convenience and clarity
    String credLoginUserMapping = cred.getLoginUser(); // Host login mapping from provided credential
    String oboTenant = rUser.getOboTenantId();
    String loginUserMapping = cred.getLoginUser();
    String systemId = system.getId();
    String sysTenant = system.getTenant();
    SystemType systemType = system.getSystemType();
    AuthnMethod sysAuthnMethod = system.getDefaultAuthnMethod();
    String sysHost = system.getHost();

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If createTmsKeys is false and defaultAuthnMethod for system is TMS then it is an error.
    if (!createTmsKeys && AuthnMethod.TMS_KEYS.equals(system.getDefaultAuthnMethod()))
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_BAD_ARG", rUser, systemId);
      throw new BadRequestException(msg);
    }
    // If TMS keys requested check that system allows for it, create the keys and add the keys to the Credential
    // Note that we must create the keys in the TMS server before verifying the credentials.
    if (createTmsKeys)
    {
      // Make sure we are configured for TMS keys and that system allows for it
      validateTmsConfig(rUser, sysTenant, systemId, systemType, credLoginUserMapping, isStaticEffectiveUser);
      // Call TMS to create the keypair and fingerprint
      TmsKeys tmsKeys = createTmsKeys(rUser, system, credTargetUser);
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

    // Determine the host login user, i.e. the resolved effectiveUserId
    // Determine hostLoginUser. If static or dynamic and no mapping, then use targetUser.
    String hostLoginUser = getHostLoginUserAtCreate(oboTenant, systemId, credTargetUser, loginUserMapping, isStaticEffectiveUser);
    // There are other checks for missing hostLoginUser. This is a good backup check in case the code changes.
    // If missing it is a hard error.
    if (StringUtils.isBlank(hostLoginUser)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_HOST_LOGIN", rUser));

    // Skip check if not LINUX or S3
    if (!skipCheck && (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)))
    {
      skipCheck = true;
      log.warn(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_SKIP", rUser, systemId, systemType, sysHost, hostLoginUser, sysAuthnMethod));
    }

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
    createCredential(rUser, retCred, system, credTargetUser, isStaticEffectiveUser, hostLoginUser, skipCheck, op);

    // Construct Json string representing the update, with actual secrets masked out
    Credential maskedCredential = Credential.createMaskedCredential(retCred);
    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredCreate(systemId, credTargetUser, skipCheck, maskedCredential);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);

    if (skipCheck) return null;
    else return retCred;
  }

  /**
   * Check user credential using given authnMethod or system default authnMethod.
   * <p>
   * Secret path depends on whether effUser type is dynamic or static
   * <p>
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *credTargetUser* is interpreted
   * as a Tapis user.
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *credTargetUser* is interpreted
   * as the login user to be used when accessing the host.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param credTargetUser - Target user for operation
   * @param authnMethod - (optional) check credentials for specified authn method instead of default authn method
   * @return Checked credential with validation result set
   */
  Credential checkCredentialForUser(ResourceRequestUser rUser, TSystem system, String credTargetUser,
                                    AuthnMethod authnMethod, SystemOperation op)
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
    Credential cred = getCredential(rUser, system, credTargetUser, authnMethod, isStaticEffectiveUser, null);
    // If no credentials then we cannot check, treat it as an error
    if (cred == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op.name(), systemId, system.getSystemType(),
              credTargetUser, authnMethod.name());
      log.info(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }
    // ---------------- Verify credentials using defaultAuthnMethod --------------------
    // Determine hostLoginUser.
    String hostLoginUser;
    //  If static use targetUser, else dynamic so use call to resolveEffUsr
    if (isStaticEffectiveUser)
    {
      hostLoginUser = credTargetUser;
    }
    else
    {
      // Dynamic eff user, there may be a mapping. Note that targetUser is interpreted as a Tapis user.
      hostLoginUser = sysUtils.resolveEffectiveUserId(system, credTargetUser);
    }
    // Check credentials
    return verifyCredentials(rUser, system, cred, hostLoginUser, authnMethod);
  }

  /**
   * Delete credential for given system and user
   * Remove SK records and CredentialInfo record
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system Tapis system
   * @param credTargetUser - Target user for operation
   */
  int deleteCredentialForUser(ResourceRequestUser rUser, TSystem system, String credTargetUser, SystemOperation op)
  {
    String systemId = system.getId();
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // Delete credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.

    // Remove SK records and CredentialInfo record
    int changeCount = deleteCredential(rUser, system, credTargetUser, isStaticEffectiveUser, op);

    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredDelete(systemId, credTargetUser);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
    return changeCount;
  }

  /**
   * Delete all credentials for given system and user.
   * Remove SK secrets and CredInfo records
   * NOTE: May not need to be synchronized but currently only called by hardDelete which is only used during testing.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system Tapis system
   * @param op operation
   */
  synchronized void deleteAllCredentialsForSystem(ResourceRequestUser rUser, TSystem system, SystemOperation op)
  {
    // Get all CredInfo records associated with the system. This gives a full list of registered credentials.
    List<CredentialInfo> ciList = dao.getCredInfoRecordsForSystem(system.getTenant(), system.getId());
    // For each record remove all SK secrets and the CredInfo record.
    for (CredentialInfo credInfo : ciList)
    {
      deleteCredential(rUser, system, credInfo.getCredTargetUser(), credInfo.isStatic(), op);
    }
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
    String opName = "verifyCredentials";
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
    return verifyConnection(rUser, opName, tSystem1, authnMethod, cred, hostLoginUser);
  }

  /**
   * Reject the LoginUser field for a static effective user.
   * LoginUser field should not be provided in the credential if the system was created with a static effective user.
   * This is because the static effective user is already a LoginUser for the system,
   * and there is no need to map a static effective user to another user. Allowing this would be misleading.
   * There are 2 cases:
   *    1. a credential is being created for a system that was created with a static effective user; Or,
   *    2. a system is being created with a static effective user and a credential.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - the TSystem to check
   * @param cred - credentials to check
   */
  void checkCredentialForInvalidLoginUser(ResourceRequestUser rUser, TSystem sys, Credential cred)
  {
    if (!sys.isDynamicEffectiveUser() && cred != null && !StringUtils.isBlank(cred.getLoginUser()))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_LOGINUSER", rUser, sys.getId());
      log.warn(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /*
   * Create or update a credential using SKClient.
   * Write credentials to SK and create or update the CredentialInfo in DB.
   * If operation is not System.create then record update in SYSTEMS_UPDATE table
   *
   * Return a CredInfo record.
   *
   * No checks are done for incoming arguments (except hostLoginUser) and the system must exist
   *
   * Note that this method contains a synchronized block.
   * Synchronizing this is a potential bottleneck, but we do not expect that much activity around updating credentials.
   */
  CredentialInfo createCredential(ResourceRequestUser rUser, Credential credential, TSystem sys, String credTargetUser,
                                  boolean isStatic, String hostLoginUser, boolean skipCredCheck, SystemOperation op)
  {
    // There are other checks for missing hostLoginUser. This is a good backup check in case the code changes.
    // If missing it is a hard error.
    if (StringUtils.isBlank(hostLoginUser)) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_HOST_LOGIN", rUser));
    String oboUser = rUser.getOboUserId();
    String loginUserMapping = credential.getLoginUser();


    // LoginUser field should not be provided if the system was created with a static effective user. 
    // This is because the static effective user is already a LoginUser for the system,
    // and there is no need to map a static effective user to a login user again.
    this.checkCredentialForInvalidLoginUser(rUser, sys, credential);

    // For CredentialInfo record, if static then tapisUser is oboUser, if dynamic then tapisUser is targetUser
    // NOTE: targetUser is never from loginUserMapping.
    String tapisUser = isStatic ? oboUser : credTargetUser;

    // Use a synchronized block for the update operation.
    // This is basically the equivalent of a selectForUpdate DB type operation.
    // Note that this also synchronizes SK operations, which is good. Before this, multiple concurrent SK operations
    // were possible.
    CredentialInfo credInfo;
    synchronized (CredUtils.class)
    {
      // 1. Create/update CredInfo record to status PENDING
      CredentialInfo credInfoDB = dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStatic);
      if (credInfoDB == null)
      {
        // Record does not already exist, create it with status of PENDING
        credInfo = new CredentialInfo(sys.getSeqId(), sys.getTenant(), sys.getId(), tapisUser, isStatic, hostLoginUser,
                                      loginUserMapping, SyncStatus.PENDING);
        credInfo = dao.createCredInfo(rUser, credInfo);
      }
      else
      {
        // Already exists, set status to PENDING and update record.
        credInfo = new CredentialInfo(sys.getSeqId(), sys.getTenant(), sys.getId(), tapisUser, isStatic, hostLoginUser,
                                      loginUserMapping, SyncStatus.PENDING);
        updateCredInfoStatus(rUser, credInfoDB, SyncStatus.PENDING, op.name());
        // Must update entire record, not just status. Otherwise, could lose updated loginUserMapping info passed in
        //   as part of Credential.
        dao.updateCredInfoRecord(credInfo, null);
      }

      // 2. Update status to IN_PROGRESS.
      credInfo = updateCredInfoStatus(rUser, credInfo, SyncStatus.IN_PROGRESS, op.name());

      // 3. Sync. Write secrets to SK and read from SK
      try
      {
        credInfo = writeAndSyncCredInfoToSK(rUser, credential, credInfo, sys, credTargetUser, isStatic);
      }
      catch (TapisSecurityException tse)
      {
        // Issue with SK. Not much we can do.
        // Log error, update the credInfo record to FAILED and throw a runtime exception.
        String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_FAIL", rUser, credInfo.getTenant(), credInfo.getSystemId(),
                                         credInfo.getTapisUser(), credInfo.isStatic(), credInfo.getSyncFailCount()+1,
                                         tse.getMessage(), op.name());
        log.error(msg);
        // Update the credInfo record to FAILED.
        updateCredInfoToFailed(rUser, credInfo, tse.getMessage());
        throw new TapisRuntimeException(tse);
      }

      // 4. Update the credInfo record to COMPLETED. Also updates DB.
      credInfo = updateCredInfoToCompleted(rUser, credInfo);
    }

    // If it is not a system create, then record the update
    if (!SystemOperation.create.equals(op))
    {
      // Construct Json string representing the update, with actual secrets masked out
      Credential maskedCredential = Credential.createMaskedCredential(credential);
      // Get a complete and succinct description of the update.
      String changeDescription = LibUtils.getChangeDescriptionCredCreate(sys.getId(), credTargetUser, skipCredCheck,
                                                                         maskedCredential);
      // Create a record of the update
      String rawUpdateData = null;
      dao.addUpdateRecord(rUser, sys.getId(), op, changeDescription, rawUpdateData);
    }
    // Log successful update
    String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_OK", rUser, credInfo.getTenant(), credInfo.getSystemId(),
                                     credInfo.getTapisUser(), credInfo.isStatic());
    log.debug(msg);
    return credInfo;
  }

  /*
   * Delete a credential.
   * Remove SK records and CredentialInfo DB record.
   * If records do not exist that is OK.
   * No checks are done for incoming arguments and the system must exist
   *
   * Note that this method contains a synchronized block.
   * Synchronizing this is a potential bottleneck, but we do not expect that much activity around updating credentials.
   */
  int deleteCredential(ResourceRequestUser rUser, TSystem sys, String credTargetUser, boolean isStatic, SystemOperation op)
  {
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    String sysTenant = sys.getTenant();
    int changeCount;
    // If static then tapisUser is oboUser, if dynamic then tapisUser is targetUser
    String tapisUser = isStatic ? oboUser : credTargetUser;

    // Use a synchronized block for the update operation.
    // This is basically the equivalent of a selectForUpdate DB type operation.
    // Note that this also synchronizes SK operations, which is good. Before this, multiple concurrent SK operations
    // were possible.
    synchronized (CredUtils.class)
    {
      CredentialInfo credInfoDB = null;
      try
      {
        // Get CredInfo record from DB. If not there that is OK.
        credInfoDB = dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStatic);
        // Remove secrets from SK
        changeCount = removeSKSecrets(rUser, sys, credTargetUser, isStatic);
        // Remove CredInfo record from DB
        dao.deleteCredInfo(sysTenant, sysId, tapisUser, isStatic);
      }
      catch (TapisSecurityException tse)
      {
        // Issue with SK. Not much we can do.
        // Log error, update the credInfo record to FAILED and throw a runtime exception.
        int syncFailCount = credInfoDB == null ? -1 : credInfoDB.getSyncFailCount() + 1;
        String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_FAIL", rUser, sysTenant, sysId, tapisUser, isStatic,
                                         syncFailCount, tse.getMessage(), op.name());
        log.error(msg);
        // If credInfo record exists update it to FAILED.
        if (credInfoDB != null) { updateCredInfoToFailed(rUser, credInfoDB, tse.getMessage()); }
        throw new TapisRuntimeException(tse);
      }
    }
    // Log successful update
    String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_DEL", rUser, sysTenant, sysId, tapisUser, isStatic);
    log.debug(msg);
    return changeCount;
  }

  /**
   * Get a credential given system, targetUser, isStatic and authnMethod
   * No checks are done for incoming arguments and the system must exist
   * resourceTenant used when a service is calling as itself and needs to specify the tenant for the resource
   */
  Credential getCredential(ResourceRequestUser rUser, TSystem system, String credTargetUser,
                           AuthnMethod authnMethod, boolean isStaticEffectiveUser, String resourceTenant)
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
      String targetUserPath = getTargetUserSecretPath(credTargetUser, isStaticEffectiveUser);

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
        loginUser = credTargetUser;
      }
      else
      {
        // This is the dynamic case, so targetUser must be a Tapis user.
        // See if the target Tapis user has a mapping to a host login user.
        String mappedLoginUser = dao.getLoginUserMapping(oboTenant, systemId, credTargetUser, isStaticEffectiveUser);
        // If so then the mapped value becomes loginUser, else loginUser=targetUser
        if (!StringUtils.isBlank(mappedLoginUser))
          loginUser = mappedLoginUser;
        else
          loginUser = credTargetUser;
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
    catch (TapisClientException | TapisException tce)
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

  /*
   * Check the CredInfo records and update as needed
   * NOTE: This method should only be called at startup when there is only a single thread running.
   *  - Mark IN_PROGRESS records as FAILED
   *  - Create PENDING records as needed for undeleted systems that have a static effectiveUserId
   *  - Update FAILED records to PENDING
   */
  synchronized void initCredInfo(ResourceRequestUser rUser)
  {
    // Log startup and number of records in DB
    int totalCount = dao.getCredInfoTotalCount();
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_BEGIN", totalCount));

    // Mark all IN_PROGRESS records as FAILED
    String failMsg = LibUtils.getMsg("SYSLIB_CREDINFO_INIT_MARK_FAILED_BEGIN");
    log.info(failMsg);
    int numRecords = dao.credInfoMarkAllInProgressAsFailed(rUser, failMsg);
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_MARK_FAILED_END", numRecords));

    // Create records as needed for undeleted systems that have a static effectiveUserId
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_STATIC_BEGIN"));
    numRecords = dao.credInfoCreatePendingForStaticSystems();
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_STATIC_END", numRecords));

    // Update FAILED records to PENDING
    log.info(LibUtils.getMsgAuth("SYSLIB_CREDINFO_INIT_FAILED_PENDING_BEGIN", rUser));
    numRecords = dao.credInfoMarkAllFailedAsPending(rUser);
    log.info(LibUtils.getMsgAuth("SYSLIB_CREDINFO_INIT_FAILED_PENDING_END", rUser, numRecords));

    // Process PENDING records. Sync up Systems DB with SK records.
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_SYNC_PENDING_BEGIN"));
    syncPendingCredInfoRecords(rUser);
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_SYNC_PENDING_END"));

    // Log end and current number of records in table
    totalCount = dao.getCredInfoTotalCount();
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_END", totalCount));
  }

  /*
   * Update CredentialInfo status. Use this for most updates of status so status transition is validated.
   * NOTE Other methods that update status and check transition: updateCredInfoToCompleted, updateCredInfoToFailed
   * If old and new status are the same then it is a NO-OP, simply return.
   *
   * The provided credInfo object is updated and returned.
   *
   * Other syncStatus and updated timestamp, other attributes are not changed.
   * Check that transition from current status to new status is allowed.
   */
  CredentialInfo updateCredInfoStatus(ResourceRequestUser rUser, CredentialInfo credInfo, SyncStatus newSyncStatus, String opName)
  {
    SyncStatus oldSyncStatus = credInfo.getSyncStatus();
    if (oldSyncStatus.equals(newSyncStatus)) return credInfo;

    log.trace(LibUtils.getMsgAuth("SYSLIB_CREDINFO_STAT_CHANGE", rUser, credInfo.getTenant(), credInfo.getSystemId(),
          credInfo.getTapisUser(), credInfo.getHostLoginUser(), credInfo.isStatic(), oldSyncStatus, newSyncStatus, opName));

    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(rUser, oldSyncStatus, newSyncStatus);
    // Update CredInfo attributes
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    credInfo.setSyncStatus(newSyncStatus);
    // Persist the update
    dao.updateCredInfoStatus(credInfo, newSyncStatus, updated);
    return credInfo;
  }

  /*
   * Given a CredentialInfo record in the PENDING state, sync it with SK.
   * Note that this is run during startup (single-threaded) and during maintenance (multithreaded).
   *
   * If record does not exist or is not in PENDING state that is OK, simply return.
   * If record exists and still in PENDING, after this update it will be in COMPLETED or FAILED state.
   *
   * NOTE: This happens during service startup and at regular intervals when maintenance task runs.
   */
  void syncPendingCredInfo(ResourceRequestUser rUser, CredentialInfo credInfo)
  {
    String opName = "syncPendingCredInfo";
    // We are mutating a CredInfo record so synchronize around the class
    synchronized (CredUtils.class)
    {
      // If record does not exist or is not in PENDING state that is OK, simply return.
      CredentialInfo credInfoDB = dao.getCredInfo(credInfo);
      if (credInfoDB == null || !SyncStatus.PENDING.equals(credInfoDB.getSyncStatus())) return;

      // Update status to IN_PROGRESS
      credInfo = updateCredInfoStatus(rUser, credInfo, SyncStatus.IN_PROGRESS, opName);

      // Do the hard work, sync the record with SK
      try
      {
        // Sync records. Attributes updated: hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys
        // This method does not update the CredInfo table, just the credInfo in-memory object.
        credInfo = readCredInfoFromSK(rUser, credInfo);
        // NOTE: For service startup and the maintenance task, the values of hostLoginUser and loginUserMapping from the
        //       DB should be correct. Unlike for cred create, there should be no need to sync those 2 attributes.

        // Update CredInfo record in table, including reset of failed attributes and setting status to COMPLETED
        updateCredInfoToCompleted(rUser, credInfo);
      }
      catch (Exception e)
      {
        // Update failure related attributes of the credInfo
        credInfo.setSyncFailed(TapisUtils.getUTCTimeNow().toInstant(ZoneOffset.UTC));
        credInfo.incrementSyncFailCount();
        credInfo.setSyncFailMessage(e.getMessage());
        credInfo.setSyncStatus(SyncStatus.FAILED);
        // Log error, update the credInfo record to FAILED.
        String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_FAIL", rUser, credInfo.getTenant(), credInfo.getSystemId(),
              credInfo.getTapisUser(), credInfo.getHostLoginUser(), credInfo.isStatic(),
              credInfo.getSyncFailCount() + 1, e.getMessage(), opName);
        log.error(msg);
        // Update the credInfo record to FAILED.
        updateCredInfoToFailed(rUser, credInfo, e.getMessage());
      }
    }
  }

  /*
   * Sync of all CredInfo PENDING records with SK.
   * Note that this is run during startup (single-threaded) and during maintenance (multithreaded).
   */
  void syncPendingCredInfoRecords(ResourceRequestUser rUser)
  {
    // Find all PENDING records
    List<CredentialInfo> pendingRecords = dao.credInfoGetRecordsInStatus(SyncStatus.PENDING);
    log.info(LibUtils.getMsg("SYSLIB_MAINT_CREDINFO_PENDING_COUNT", pendingRecords.size()));
    // For each record sync it with SK
    for (CredentialInfo credInfo: pendingRecords)
    {
       syncPendingCredInfo(rUser, credInfo);
    }
  }

  /*
   * Update of all CredInfo FAILED records to PENDING
   */
  void credInfoMarkFailedAsPending(ResourceRequestUser rUser)
  {
    String opName = "credInfoMarkFailedAsPending";
    // Find all FAILED records
    List<CredentialInfo> failedRecords = dao.credInfoGetRecordsInStatus(SyncStatus.FAILED);
    String msg = LibUtils.getMsg("SYSLIB_MAINT_CREDINFO_FAIL_COUNT", failedRecords.size());
    log.info(msg);
    // For each record update the status
    for (CredentialInfo credInfo: failedRecords)
    {
      // We are mutating a CredInfo record so synchronize around the class
      synchronized (CredUtils.class)
      {
        // If record does not exist or is not in FAILED state that is OK, simply return.
        CredentialInfo credInfoDB = dao.getCredInfo(credInfo);
        if (credInfoDB == null || !SyncStatus.FAILED.equals(credInfoDB.getSyncStatus())) continue;
        // Update status to FAILED
        updateCredInfoStatus(rUser, credInfo, SyncStatus.IN_PROGRESS, opName);
      }
    }
  }

  /*
   * Update CredentialInfo hasCredentials attribute based on current defaultAuthnMethod for the system.
   * All records associated with the system will be updated unless updates are currently in progress
   *
   * NOTE: Since we are synchronizing here no updates should be IN_PROGRESS.
   *       Any FAILED or PENDING records will get updated later by the maintenance task.
   */
  void updateCredInfoHasCredentials(ResourceRequestUser rUser, TSystem sys)
  {
    String opName = "updateCredInfoHasCredentials";
    AuthnMethod authnMethod = sys.getDefaultAuthnMethod();
    // We are mutating a CredInfo record so synchronize around the class
    synchronized (CredUtils.class)
    {
      // Get all CredInfo records associated with the system.
      List<CredentialInfo> ciList = dao.getCredInfoRecordsForSystem(sys.getTenant(), sys.getId());
      // For each record update hasCredentials
      for (CredentialInfo ci : ciList)
      {
        // If not in COMPLETED state move on
        if (!SyncStatus.COMPLETED.equals(ci.getSyncStatus())) continue;

        // Update status to IN_PROGRESS
        ci = updateCredInfoStatus(rUser, ci, SyncStatus.IN_PROGRESS, opName);

        // Determine if credentials are registered for defaultAuthnMethod of the system
        boolean hasCredentials = (AuthnMethod.PASSWORD.equals(authnMethod) && ci.hasPassword()) ||
              (AuthnMethod.PKI_KEYS.equals(authnMethod) && ci.hasPkiKeys()) ||
              (AuthnMethod.ACCESS_KEY.equals(authnMethod) && ci.hasAccessKey()) ||
              (AuthnMethod.TOKEN.equals(authnMethod) && ci.hasToken() ) ||
              (AuthnMethod.TMS_KEYS.equals(authnMethod) && ci.hasTmsKeys());

        // Update hasCredentials
        dao.updateCredInfoHasCredentials(ci, hasCredentials);
        log.trace(LibUtils.getMsgAuth("SYSLIB_CREDINFO_SET_HASCREDS", rUser, ci.getTenant(), ci.getSystemId(),
                                 ci.getTapisUser(), ci.getHostLoginUser(), ci.isStatic(), hasCredentials, opName));


        // Update status to COMPLETED
        updateCredInfoStatus(rUser, ci, SyncStatus.COMPLETED, opName);
      }
    }
  }

  /*
   * Update CredentialInfo record based on changes to defaultAuthnMethod or effUser.
   * Used as part of patch and put update operations.
   * All records associated with the system will be updated unless updates are currently in progress
   *
   * NOTE: Since we are synchronizing here no updates should be IN_PROGRESS.
   *       Any FAILED or PENDING records will get updated later by the maintenance task.
   */
  void updateCredInfoRecordsForSystem(ResourceRequestUser rUser, TSystem sys, AuthnMethod origDefaultAuthnMethod, String origEffUser)
  {
    String opName = "updateCredInfoRecord";
    AuthnMethod authnMethod = sys.getDefaultAuthnMethod();
    String effUser = sys.getEffectiveUserId();

    boolean authnChanged = !authnMethod.equals(origDefaultAuthnMethod);
    boolean effUserChanged = !effUser.equals(origEffUser);

    // If no changes then simply return
    if (!authnChanged && !effUserChanged) return;

    boolean isStaticEffUser = !effUser.equals(APIUSERID_VAR);

    // Something has changed:
    //    1. Start synchronized block
    //    2. Make sure we have at least one record, for system owner.
    //    3. For each record update hasCredentials base on current authnMethod.
    synchronized (CredUtils.class)
    {
      // Fetch credInfo record for owner, if it exists. Use possibly new value of effUser
      // We might check for and then create a credInfo record, so synchronize
      CredentialInfo ownerCredInfo;
      synchronized (CredUtils.class)
      {
        ownerCredInfo = dao.getCredInfo(sys.getTenant(), sys.getId(), sys.getOwner(), isStaticEffUser);
        // If it did not yet exist then create it
        if (ownerCredInfo == null)
        {
          // No record yet existed, so logUserMapping is null
          createCredInfoRecordAsNeeded(rUser, sys, sys.getOwner(), isStaticEffUser, effUser, nullLoginUserMapping, opName);
        }

        // Update CredentialInfo hasCredentials attribute based on current defaultAuthnMethod for the system.
        updateCredInfoHasCredentials(rUser, sys);
      }
    }
  }

  /*
   * Given a TSystem and user making the request, fetch a credInfo record.
   * Returns null if no record exists.
   */
  CredentialInfo getCredInfo(ResourceRequestUser rUser, TSystem sys, String oboOrImpersonatedUser,
                             boolean isStaticEffUsr)
  {
    // Determine tapisUser for looking up CredInfo
    // If static use effectiveUserId, else use oboOrImpersonatedUser
    String credTargetUser = (isStaticEffUsr) ? sys.getEffectiveUserId(): oboOrImpersonatedUser;
    String tapisUser = isStaticEffUsr ? rUser.getOboUserId() : credTargetUser;
    return dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStaticEffUsr);
  }

  /*
   * Given a TSystem, tapisUser, hostLoginUser and isStatic create a CredInfo record if none exist.
   * If record already exists then existing record is returned.
   * There are two cases where we want to make sure at least one record exists:
   *   1. During system create when credentials are not provided and effUser is static.
   *   2. During a put or patch update when authnMethod or effUser have changed.
   */
   CredentialInfo createCredInfoRecordAsNeeded(ResourceRequestUser rUser, TSystem sys, String tapisUser,
                                               boolean isStaticEffUser, String hostLoginUser,
                                               String loginUserMapping, String opName)
   {
     CredentialInfo credInfo = null;
     // Use a synchronized block for the operation.
     synchronized (CredUtils.class)
     {
       // 1. Create/update CredInfo record
       credInfo = dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStaticEffUser);
       if (credInfo == null)
       {
         // Record does not already exist, create it. Note we go through all states PENDING->IN_PROGRESS->COMPLETED
         //   because we want to make sure we never violate allowed state transitions.
         credInfo = new CredentialInfo(sys.getSeqId(), sys.getTenant(), sys.getId(), tapisUser, isStaticEffUser,
                                       hostLoginUser, loginUserMapping, SyncStatus.PENDING);
         credInfo = dao.createCredInfo(rUser, credInfo);
         updateCredInfoStatus(rUser, credInfo, SyncStatus.IN_PROGRESS, opName);
         updateCredInfoStatus(rUser, credInfo, SyncStatus.COMPLETED, opName);
         // Log successful operation
         String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_CREATED", rUser, credInfo.getTenant(), credInfo.getSystemId(),
                                          credInfo.getTapisUser(), isStaticEffUser, hostLoginUser, loginUserMapping);
         log.debug(msg);
       }
     }
     return credInfo;
   }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

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
   * @param credTargetUser Host account user
   * @return tms key info
   * @throws TapisException on error
   */
  private TmsKeys createTmsKeys(ResourceRequestUser rUser, TSystem system, String credTargetUser)
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
    String tmsHostAccount = credTargetUser;
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
      log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_REQ", rUser, system.getId(), credTargetUser, tmsServerUrl));
      try (okhttp3.Response response = call.execute())
      {
        // Get the response body as a string
        if (response.body() != null) respBodyStr = response.body().string();
        // If response status code is not in the 200s it is an error
        httpRespCode = response.code();
        if (httpRespCode < 200 || httpRespCode >= 300)
        {
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_HTTP_ERR", rUser, system.getId(), credTargetUser,
                                    tmsServerUrl, httpRespCode, respBodyStr);
          log.error(msg);
        }
      }
    }
    catch (IOException e)
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_ERR", rUser, system.getId(), credTargetUser, tmsServerUrl, e.getMessage());
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
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NO_BODY", rUser, system.getId(), credTargetUser, tmsServerUrl, httpRespCode);
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
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_NULL_FIELD", rUser, system.getId(), credTargetUser, tmsServerUrl, httpRespCode,
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
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_EMPTY_FIELD", rUser, system.getId(), credTargetUser, tmsServerUrl,
                                httpRespCode, privateKeyMasked, tmsPublicKey, tmsPublicKeyFingerprint);
      log.error(msg);
      throw new TapisException(msg);
    }

    // Log extracted data
    msg = LibUtils.getMsgAuth("SYSLIB_CRED_TMS_KEYS_DATA", rUser, system.getId(), credTargetUser, tmsServerReqUrl,
                              httpRespCode, privateKeyMasked, tmsPublicKey, tmsPublicKeyFingerprint);
    log.debug(msg);
    return new TmsKeys(tmsPrivateKey, tmsPublicKey, tmsPublicKeyFingerprint);
  }

  /*
   * Verify connection based on authentication method
   * NOTE that credential returned even if invalid. Caller must check Credential.getValidationResult()
   */
  private Credential verifyConnection(ResourceRequestUser rUser, String opName, TSystem tSystem1,
                                      AuthnMethod authnMethod, Credential cred, String hostLoginUser)
  {
    log.info(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_BEGIN", rUser, tSystem1.getId(), tSystem1.getSystemType(),
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
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, opName, systemId, systemType, hostLoginUser, authnMethod);
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

  /*
   * For credential creation operation, determine the host login user, i.e. the resolved effectiveUserId.
   */
  private String getHostLoginUserAtCreate(String sysTenant, String sysId, String credTargetUser, String loginUserMapping, boolean isStatic)
  {
    // Determine hostLoginUser. If static or dynamic and no mapping, then use targetUser.
    String hostLoginUser = credTargetUser;
    // If dynamic need to check for host login user mapping.
    if (!isStatic)
    {
      // Since this is a cred create operation, the host login user mapping might be in the DB or part of the incoming
      //   credential or both. The one in the credential has priority because it will be replacing the DB record
      if (StringUtils.isBlank(loginUserMapping)) loginUserMapping = dao.getLoginUserMapping(sysTenant, sysId, credTargetUser, isStatic);
      if (!StringUtils.isBlank(loginUserMapping)) hostLoginUser = loginUserMapping;
    }
    return hostLoginUser;
  }

  /*
   * NOTE: Original plan used this during service startup. Instead, now it is handled by the standalone utility
   * CredInfoInitJob.java
   * Originally called from initCredInfo() using:
   *    // Read list of records from a file and create in PENDING state.
   *    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_BEGIN"));
   *    int numRecords = credInfoInitFromFile(rUser);
   *    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_END", numRecords));
   * Read list of records from a file and create/update CredInfo records in PENDING state.
   * Log errors but otherwise ignore them.
   * Look for records in file /tmp/tapis_sys_cred_info_init.csv
   * Records must have this format:
   *     tenant,sysId,credTargetUser,isStatic,authnMethod
   * Note that authnMethod is not used.
   * Note that the cvs file can (and usually will be) generated by running the SKUtility program.
   * Code is in tapis-security repo. For code and instructions please see SKUtility.java and SkUtilityParameters.java.
   *
   * From the record in the file we have the primary key values for table: tenant, sysId, isStatic
   * Most of the other values (has_credentials, has_pki_keys, etc.) will be filled in from SK during a sync.
   *
   * But we still need to figure out values for tapisUser, hostLoginUser, loginUserMapping
   *
   * Two scenarios:
   *
   * 1. This method is mainly intended for initializing the CredInfo table during the initial upgrade of Tapis
   *    from a previous version that did not support CredInfo tracking. In this case of a first run any existing
   *    CredInfo records will have been migrated from the old loginUserMapping table. They will all have a
   *    loginUserMapping and isStatic=false.
   *   For each record in the CVS file a CredInfo record may or may not be in the DB:
   *    a. CredInfo in the DB: From previous loginUserMapping record. In this case
   *         loginUserMapping : from DB, should never be null
   *         hostLoginUser    : if isStatic=true use effectiveUserId from system
   *                            if isStatic=false use loginUserMapping from the DB
   *    b. CredInfo not in DB. In this case
   *         loginUserMapping : never set by tapisUser, use null
   *         hostLoginUser    : if isStatic=true use effectiveUserId from system
   *                            if isStatic=false use credTargetUser from the record
   *
   * 2. In addition to support for the initial upgrade, this method can also be used to force records into the
   *    pending state or add records that somehow have been missed.
   *    a. CredInfo in the DB: In this case
   *         loginUserMapping : from DB, might be null
   *         hostLoginUser    : if isStatic=true use effectiveUserId from system
   *                            if isStatic=false and loginUserMapping!=null, use loginUserMapping from the DB
   *                            if isStatic=false and loginUserMapping=null, use credTargetUser from the record
   *    b. CredInfo not in DB. In this case
   *         loginUserMapping : not available, use null
   *         hostLoginUser    : if isStatic=true use effectiveUserId from system
   *                            if isStatic=false use credTargetUser from the record
   *
   * Note that the second algorithm can be used for both scenarios, which is good, since there is no way to
   * detect the scenario, and we want to support both scenarios.
   */
  private int credInfoInitFromFile(ResourceRequestUser rUser)
  {
    int retCount = 0;
    Path filePath = Path.of(CREDINFO_INIT_TMP_CSV_FILE);
    // If no file then we are done
    if (!Files.isRegularFile(filePath))
    {
      log.info(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_NOFILE", filePath.toString()));
      return retCount;
    }
    // Read CSV records from file. If incorrect format log message and continue.
    try (BufferedReader reader = Files.newBufferedReader(filePath))
    {
      CSVReader csvReader = new CSVReader(reader);
      CredentialInfo credInfo;
      // Read and process lines until done
      do
      {
        credInfo = csvReadLineAndCreateCredInfoRecord(rUser, csvReader);
      }
      while (credInfo != null);
    }
    catch (Exception e)
    {
      log.error(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_ERR", e.getMessage()));
    }
    return retCount;
  }

  /*
   * Use openCSV library to read a line and parse the fields.
   * One record per line, a CredentialInfo object is created from the data.
   * If a CredentialInfo object already exists in the DB then status is updated to PENDING,
   * else a new CredentialInfo record is persisted to the DB.
   * Records must have this format:
   *     tenant,sysId,targetUser,isStatic,authnMethod
   * Strings are trimmed before being processed
   * If there is an error processing a line a non-null CredInfo is still returned so processing will continue.
   */
  private CredentialInfo csvReadLineAndCreateCredInfoRecord(ResourceRequestUser rUser, CSVReader reader)
  {
    String opName = "csvReadLineAndCreateRecord";
    String [] nextRecord;
    CredentialInfo credInfo;
    try
    {
      // Get and parse next line
      nextRecord = reader.readNext();
      // If last record processed then return
      if (nextRecord == null) return null;

      // Extract and validate attributes from the record
      if (nextRecord.length != 5)
      {
        throw new Exception(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_LINE_PARSE_ERR", "Incorrect number of csv fields"));
      }
      String tenant = nextRecord[0].trim();
      String sysId = nextRecord[1].trim();
      String credTargetUser = nextRecord[2].trim();
      boolean isStatic = Boolean.parseBoolean(nextRecord[3].trim());
      String authnMethod = nextRecord[4].trim(); // Not used, ignore
      credInfo = createUpdatePendingCredInfoRecordFromCSVRecord(rUser, tenant, sysId, credTargetUser, isStatic);
    }
    catch (Exception e)
    {
      // On error log message but continue;
      log.error(LibUtils.getMsg("SYSLIB_CREDINFO_INIT_FROM_FILE_LINE_ERR", e.getMessage()));
      // Return non-null credInfo so calling loop will continue.
      credInfo = new CredentialInfo(-1, "", "", "", true, "", "", SyncStatus.FAILED);
    }
    return credInfo;
  }

  /*
   * Create or update a CredInfo record with status of PENDING.
   * Called when service is starting up and CredInfo records are being initialized by reading csv data from a file.
   *
   * From the record in the file we have some of the primary key values for table: tenant, sysId, isStatic
   * Most of the other values (has_credentials, has_pki_keys, etc.) will be filled in from SK during a sync.
   *
   * But we still need to figure out values for tapisUser, hostLoginUser and loginUserMapping
   * tapisUser is fairly straightforward, see below. For others:
   *
   * Two cases:
   *    a. CredInfo in the DB:
   *         loginUserMapping : from DB, might be null
   *         hostLoginUser : if isStatic=true use effectiveUserId from system
   *                         if isStatic=false and loginUserMapping!=null, use loginUserMapping from the DB
   *                         if isStatic=false and loginUserMapping=null, use credTargetUser from the record
   *    b. CredInfo not in DB:
   *         loginUserMapping : not available, use null
   *         hostLoginUser : if isStatic=true use effectiveUserId from system
   *                         if isStatic=false use credTargetUser from the record
   */
  private CredentialInfo createUpdatePendingCredInfoRecordFromCSVRecord(ResourceRequestUser rUser, String tenant, String sysId,
                                                                        String credTargetUser, boolean isStatic)
  {
    String opName = "createUpdatePendingCredInfoRecordFromCSVRecord";
    CredentialInfo credInfo;
    // Fetch the system, we will use the seqId and owner
    TSystem sys = dao.getSystem(tenant, sysId); // For seqId, owner

    // Compute tapisUser, hostLoginUser and loginUserMapping
    String tapisUser, hostLoginUser, loginUserMapping;
    // tapisUser.
    // For dynamic always credTargetUser.
    // For static use system owner, that is who will most likely have registered the credential.
    //   In practice, if it was not the owner, but instead it was a tenant admin, for example, it should not matter
    //   since anyone using the system will get the credential for the static effUserId.
    if (!isStatic) tapisUser = credTargetUser; else tapisUser = sys.getOwner();

    // We are mutating a CredInfo record so synchronize around the class
    synchronized (CredUtils.class)
    {
      CredentialInfo credInfoDB = dao.getCredInfo(sys.getTenant(), sys.getId(), tapisUser, isStatic);
      if (credInfoDB != null)
      {
        // Record is already in the DB, just need to update status to PENDING
        credInfo = updateCredInfoStatus(rUser, credInfoDB, SyncStatus.PENDING, opName);
      }
      else
      {
        // No record in DB, create one with status of PENDING.
        // loginUserMapping not available, use null
        loginUserMapping = null;

        // Determine hostLoginUser. If dynamic, credTargetUser, if static, system effectiveUserId
        if (!isStatic) hostLoginUser = credTargetUser; else hostLoginUser = sys.getEffectiveUserId();

        // Create and persist the record
        credInfo = new CredentialInfo(sys.getSeqId(), sys.getTenant(), sys.getId(), tapisUser, isStatic,
              hostLoginUser, loginUserMapping, SyncStatus.PENDING);
        credInfo = dao.createCredInfo(rUser, credInfo);
      }
    }
    return credInfo;
  }

  /*
   * Update CredentialInfo record to COMPLETED
   * Check that transition from current status to COMPLETED is allowed.
   */
  private CredentialInfo updateCredInfoToCompleted(ResourceRequestUser rUser, CredentialInfo credInfo)
  {
    String opName = "updateCredInfoToCompleted";
    SyncStatus oldSyncStatus = credInfo.getSyncStatus();
    SyncStatus newSyncStatus = SyncStatus.COMPLETED;
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREDINFO_STAT_CHANGE", rUser, credInfo.getTenant(), credInfo.getSystemId(),
              credInfo.getTapisUser(), credInfo.getHostLoginUser(), credInfo.isStatic(), oldSyncStatus, newSyncStatus, opName));

    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(rUser, oldSyncStatus, newSyncStatus);

    // Update CredInfo attributes, including reset of Failure info.
    credInfo.setSyncFailCount(0);
    credInfo.setSyncFailMessage("");
    credInfo.setSyncFailed(null);
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setSyncStatus(newSyncStatus);
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    // Persist the update
    dao.updateCredInfoRecord(credInfo, updated);
    return credInfo;
  }

  /*
   * Update CredentialInfo to FAILED
   * Check that transition from current status to new status is allowed.
   */
  private CredentialInfo updateCredInfoToFailed(ResourceRequestUser rUser, CredentialInfo credInfo, String errorMsg)
  {
    String opName = "updateCredInfoToFailed";
    SyncStatus oldSyncStatus = credInfo.getSyncStatus();
    SyncStatus newSyncStatus = SyncStatus.FAILED;
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREDINFO_STAT_CHANGE", rUser, credInfo.getTenant(), credInfo.getSystemId(),
          credInfo.getTapisUser(), credInfo.getHostLoginUser(), credInfo.isStatic(), oldSyncStatus, newSyncStatus, opName));
    // Validate transition from current state to new state
    CredInfoFSM.checkForAllowedTransition(rUser, oldSyncStatus, newSyncStatus);
    // Update CredInfo attributes
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    credInfo.setSyncFailed(updated.toInstant(ZoneOffset.UTC));
    credInfo.setSyncFailMessage(errorMsg);
    credInfo.incrementSyncFailCount();
    credInfo.setSyncStatus(newSyncStatus);
    credInfo.setUpdated(updated.toInstant(ZoneOffset.UTC));
    // Persist the update
    dao.updateCredInfoRecord(credInfo, updated);
    return credInfo;
  }

  /**
   * Given CredentialInfo record call SK to get latest data.
   * The given credInfo object is updated and returned.
   * Attributes updated: hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys
   * No exceptions are caught.
   *
   * @param rUser ResourceRequest user, for logging purposes
   * @param credInfo CredentialInfo object with current data from Systems server datastore
   * @throws TapisClientException - on SK error
   * @throws TapisException - on getSKClient error
   */
  private CredentialInfo readCredInfoFromSK(ResourceRequestUser rUser, CredentialInfo credInfo)
        throws TapisClientException, TapisException
  {
    boolean hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys;
    String tenant = credInfo.getTenant();
    String credTargetUser = credInfo.getCredTargetUser();
    String systemId = credInfo.getSystemId();
    boolean isStatic = credInfo.isStatic();
    TSystem.AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(tenant, systemId);
    // Construct basic SK secret parameters
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);

    // Fill in systemId and targetUserPath for the path to the secret.
    String targetUserPath = CredUtils.getTargetUserSecretPath(credTargetUser, isStatic);

    // Set tenant, system and user associated with the secret.
    // These values are used to build the vault path to the secret.
    sParms.setTenant(tenant).setSysId(systemId).setSysUser(targetUserPath);

    // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
    sParms.setUser(credTargetUser);

    Map<String, String> dataMap;
    SkSecret skSecret;
    // PASSWORD
    // Attempt to read the secret, if not found (404) that is OK, but any other exception is an SK error.
    sParms.setKeyType(KeyType.password);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasPassword = false;
    else
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasPassword = false;
      else hasPassword = !StringUtils.isBlank(dataMap.get(SK_KEY_PASSWORD));
    }
    // PKI_KEYS
    sParms.setKeyType(KeyType.sshkey);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasPkiKeys = false;
    else
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasPkiKeys = false;
      else hasPkiKeys = !StringUtils.isBlank(dataMap.get(SK_KEY_PRIVATE_KEY));
    }
    // ACCESS_KEY
    sParms.setKeyType(KeyType.accesskey);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasAccessKey = false;
    else
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasAccessKey = false;
      else hasAccessKey = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_KEY));
    }
    // TOKEN
    sParms.setKeyType(KeyType.token);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasToken = false;
    else
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasToken = false;
      else hasToken = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_TOKEN));
    }
    // TMS_KEYS
    sParms.setKeyType(KeyType.tmskey);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasTmsKeys = false;
    else
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasTmsKeys = false;
      else hasTmsKeys = !StringUtils.isBlank(dataMap.get(SK_KEY_TMS_PRIVATE_KEY));
    }

    // Determine if credentials are registered for defaultAuthnMethod of the system
    hasCredentials = (TSystem.AuthnMethod.PASSWORD.equals(defaultAuthnMethod) && hasPassword) ||
          (TSystem.AuthnMethod.PKI_KEYS.equals(defaultAuthnMethod) && hasPkiKeys) ||
          (TSystem.AuthnMethod.ACCESS_KEY.equals(defaultAuthnMethod) && hasAccessKey) ||
          (TSystem.AuthnMethod.TOKEN.equals(defaultAuthnMethod) && hasToken);
    // Update the CredentialInfo object
    credInfo.setHasCredentials(hasCredentials);
    credInfo.setHasPassword(hasPassword);
    credInfo.setHasPkiKeys(hasPkiKeys);
    credInfo.setHasAccessKey(hasAccessKey);
    credInfo.setHasToken(hasToken);
    credInfo.setHasTmsKeys(hasTmsKeys);
    return credInfo;
  }

  /**
   * Method to write credentials to SK.
   * Provided CredentialInfo object is updated by reading data from SK and then returned.
   * Following CredentialInfo attributes need updating based on current SK data:
   *   hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys
   * <p>
   * When the Systems service calls SK to create secrets it calls with a JWT as itself,
   *   jwtTenantId = admin tenant (Site Tenant Admin)
   *   jwtUserId = TapisConstants.SERVICE_NAME_SYSTEMS ("systems")
   *   and AccountType = TapisThreadContext.AccountType.service
   * <p>
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
   * @param credTargetUser - User associated with the credential
   * @param isStatic - indicates if effectiveUserId is static or dynamic
   * @throws TapisSecurityException on SK error
   */
  private CredentialInfo writeAndSyncCredInfoToSK(ResourceRequestUser rUser, Credential credential, CredentialInfo credInfo,
                                        TSystem system, String credTargetUser, boolean isStatic)
          throws TapisSecurityException
  {
    // Set some variables for convenience and clarity
    String oboUser = rUser.getOboUserId();
    String tenant = system.getTenant();
    String systemId = system.getId();
    AuthnMethod defaultAuthnMethod = system.getDefaultAuthnMethod();

    // Flags used for building CredentialInfo
    boolean hasCredentials;
    Boolean hasPassword = null, hasPkiKeys = null, hasAccessKey = null, hasToken = null, hasTmsKeys = null;

    // Surround all SK related code in a try block. Catch any SK errors and throw a TapisSecurityException
    try
    {
      // Persist the credential data to SK
      // Construct basic SK secret parameters including tenant, system and Tapis user for credential
      // Establish secret type ("system") and secret name ("S1")
      var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      // Fill in systemId and targetUserPath for the path to the secret.
      String targetUserPath = getTargetUserSecretPath(credTargetUser, isStatic);
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
        sysUtils.getSKClient(rUser).writeSecret(tenant, oboUser, sParms);
        hasTmsKeys = true;
      }
      // NOTE if necessary handle ssh certificate when supported

      // Determine CredentialInfo properties that are based on SK and not set above
      // For each case check to see if not set above. If not then read from SK and set it
      var sReadParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      sReadParms.setTenant(tenant).setSysId(systemId).setSysUser(targetUserPath);
      sReadParms.setUser(credTargetUser);

      SkSecret skSecret;
      // PASSWORD
      if (hasPassword == null)
      {
        // Attempt to read the secret, if not found (404) that is OK, but any other exception is an SK error.
        sParms.setKeyType(KeyType.password);
        skSecret = sysUtils.getSKClient(rUser).readSecret(sReadParms);
        if (skSecret == null) hasPassword = false;
        else
        {
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

    // Update the provided CredentialInfo object
    credInfo.setHasCredentials(hasCredentials);
    credInfo.setHasPassword(hasPassword);
    credInfo.setHasPkiKeys(hasPkiKeys);
    credInfo.setHasAccessKey(hasAccessKey);
    credInfo.setHasToken(hasToken);
    credInfo.setHasTmsKeys(hasTmsKeys);
    return credInfo;
  }

  /**
   * Remove all secrets from SK for given target user, tenant and system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param credTargetUser - User associated with the credential
   * @param isStatic - indicates if effectiveUserId is static or dynamic
   * @return 1 if secrets removed, 0 if no secrets removed
   * @throws TapisSecurityException on SK error
   */
  private int removeSKSecrets(ResourceRequestUser rUser, TSystem system, String credTargetUser, boolean isStatic)
          throws TapisSecurityException
  {
    // Set some variables for convenience and clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = system.getTenant();
    String systemId = system.getId();
    // Remove SK records
    // Determine targetUserPath for the path to the secret.
    String targetUserPath = getTargetUserSecretPath(credTargetUser, isStatic);

    // Surround all SK related code in a try block. Catch any SK errors and throw a TapisSecurityException
    try
    {
      // Return 0 if credential does not exist
      var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
      // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
      sMetaParms.setTenant(oboTenant).setUser(oboUser);
      sMetaParms.setSysId(systemId).setSysUser(targetUserPath);
      // NOTE: To be sure we know that the secret does not exist we need to check each key type
      //       By default keyType is sshkey which may not exist
      boolean secretNotFound = true;
      SkSecretVersionMetadata sksm;
      // Attempt to read the secret, if not found (404) that is OK, but any other exception is an SK error.
      sMetaParms.setKeyType(KeyType.password);
      try { sksm=sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); if (sksm!=null) secretNotFound = false; }
      catch (TapisClientException tce) { if (tce.getCode() != 404 ) throw tce; }

      sMetaParms.setKeyType(KeyType.sshkey);
      try { sksm=sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); if (sksm!=null) secretNotFound = false; }
      catch (TapisClientException tce) { if (tce.getCode() != 404 ) throw tce; }

      sMetaParms.setKeyType(KeyType.accesskey);
      try { sksm=sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); if (sksm!=null) secretNotFound = false; }
      catch (TapisClientException tce) { if (tce.getCode() != 404 ) throw tce; }

      sMetaParms.setKeyType(KeyType.token);
      try { sksm=sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); if (sksm!=null) secretNotFound = false; }
      catch (TapisClientException tce) { if (tce.getCode() != 404 ) throw tce; }

      sMetaParms.setKeyType(KeyType.tmskey);
      try { sksm=sysUtils.getSKClient(rUser).readSecretMeta(sMetaParms); if (sksm!=null) secretNotFound = false; }
      catch (TapisClientException tce) { if (tce.getCode() != 404 ) throw tce; }

      if (secretNotFound) return 0;

      // Construct basic SK secret parameters and attempt to destroy each type of secret.
      // If destroy attempt throws an exception then log a message and continue.
      sMetaParms.setKeyType(KeyType.password);
      try {sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms);} catch (Exception e) { log.trace(e.getMessage()); }
      sMetaParms.setKeyType(KeyType.sshkey);
      try {sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms);} catch (Exception e) { log.trace(e.getMessage()); }
      sMetaParms.setKeyType(KeyType.accesskey);
      try {sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms);} catch (Exception e) { log.trace(e.getMessage()); }
      sMetaParms.setKeyType(KeyType.token);
      try {sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms);} catch (Exception e) { log.trace(e.getMessage()); }
      sMetaParms.setKeyType(KeyType.tmskey);
      try {sysUtils.getSKClient(rUser).destroySecretMeta(sMetaParms);} catch (Exception e) { log.trace(e.getMessage()); }
    }
    catch ( TapisException | TapisClientException te) { throw new TapisSecurityException(te); }
    return 1;
  }

  /*
   * Return segment of secret path for target user, including static or dynamic scope
   * Note that SK uses + rather than / to create sub-folders.
   */
  private static String getTargetUserSecretPath(String credTargetUser, boolean isStatic)
  {
    return String.format("%s+%s", isStatic ? "static" : "dynamic", credTargetUser);
  }
}
