package edu.utexas.tacc.tapis.systems.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;

import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.globusproxy.client.GlobusProxyClient;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.AuthTokens;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.ResultGlobusAuthInfo;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqShareResource;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareGetSharesParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareHasPrivilegeParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHAuthException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.s3.S3Connection;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.*;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.SchedulerProfileOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import static edu.utexas.tacc.tapis.shared.TapisConstants.SYSTEMS_SERVICE;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_REFRESH_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_SECRET;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PASSWORD;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 */
@Service
public class SystemsServiceImpl implements SystemsService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  // Permspec format for systems is "system:<tenant>:<perm_list>:<system_id>"
  public static final String PERM_SPEC_TEMPLATE = "system:%s:%s:%s";
  private static final String PERM_SPEC_PREFIX = "system";
  
  private static final Set<Permission> ALL_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY, Permission.EXECUTE));
  private static final Set<Permission> READMODIFY_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  private static final Set<Permission> EXECUTE_PERMS = new HashSet<>(Set.of(Permission.EXECUTE));

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String FILES_SERVICE = TapisConstants.SERVICE_NAME_FILES;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;
  private static final Set<String> SVCLIST_GETCRED = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(FILES_SERVICE, APPS_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_SHAREDAPPCTX = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_RESOURCETENANT = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));

  // Message keys
  private static final String ERROR_ROLLBACK = "SYSLIB_ERROR_ROLLBACK";
  private static final String NOT_FOUND = "SYSLIB_NOT_FOUND";

  // NotAuthorizedException requires a Challenge, although it serves no purpose here.
  private static final String NO_CHALLENGE = "NoChallenge";

  // String used to detect that credentials are the problem when creating an SSH connection
  private static final String NO_MORE_AUTH_METHODS = "No more authentication methods available";
  // SFTP client throws IOException containing this string if a path does not exist.
  private static final String NO_SUCH_FILE = "no such file";

  // Compiled regex for splitting around ":"
  private static final Pattern COLON_SPLIT = Pattern.compile(":");

  // Named and typed null values to make it clear what is being passed in to a method
  private static final String nullOwner = null;
  private static final String nullImpersonationId = null;
  private static final String nullSharedAppCtx = null;
  private static final String nullTargetUser = null;
  private static final Set<Permission> nullPermSet = null;
  private static final SystemShare nullSystemShare = null;
  private static final Credential nullCredential = null;
  
  // Sharing constants
  private static final String OP_SHARE = "share";
  private static final String OP_UNSHARE = "unShare";
  private static final Set<String> publicUserSet = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"
  private static final String SYS_SHR_TYPE = "system";

  // Connection timeouts for SKClient
  private static final int SK_READ_TIMEOUT_MS = 20000;
  private static final int SK_CONN_TIMEOUT_MS = 20000;

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum AuthListType  {OWNED, SHARED_PUBLIC, ALL}
  public static final AuthListType DEFAULT_LIST_TYPE = AuthListType.OWNED;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private ServiceClients serviceClients;
  @Inject
  private ServiceContext serviceContext;

  // We must be running on a specific site and this will never change
  // These are initialized in method initService()
  private static String siteId;
  private static String siteAdminTenantId;
  public static String getSiteId() {return siteId;}
  public static String getServiceTenantId() {return siteAdminTenantId;}
  public static String getServiceUserId() {return SERVICE_NAME;}

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Initialize the service:
   *   init service context
   *   migrate DB
   */
  public void initService(String siteId1, String siteAdminTenantId1, String svcPassword) throws TapisException, TapisClientException
  {
    // Initialize service context and site info
    siteId = siteId1;
    siteAdminTenantId = siteAdminTenantId1;
    serviceContext.initServiceJWT(siteId, SYSTEMS_SERVICE, svcPassword);
    // Make sure DB is present and updated to latest version using flyway
    dao.migrateDB();
  }

  /**
   * Check that we can connect with DB and that the main table of the service exists.
   * @return null if all OK else return an Exception
   */
  public Exception checkDB()
  {
    return dao.checkDB();
  }

  // -----------------------------------------------------------------------
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------

  /**
   * Create a new system object given a TSystem and the raw data used to create the TSystem.
   * Secrets in the text should be masked.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Pre-populated TSystem object (including tenantId and systemId)
   * @param skipCredCheck - Indicates if cred check should happen (for LINUX, S3)
   * @param rawData - Json used to create the TSystem object - secrets should be scrubbed. Saved in update record.
   * @return TSystem with defaults set and validated credentials filled in as needed
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public TSystem createSystem(ResourceRequestUser rUser, TSystem system, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    SystemOperation op = SystemOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (system == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, rawData));

    // Extract some attributes for convenience and clarity.
    // NOTE: do not do this for effectiveUserId since it may be ${owner} and get resolved below.
    String tenant = system.getTenant();
    String systemId = system.getId();
    SystemType systemType = system.getSystemType();

    // ---------------------------- Check inputs ------------------------------------
    // Required system attributes: tenant, id, type, host, defaultAuthnMethod
    if (StringUtils.isBlank(tenant) || StringUtils.isBlank(systemId) || system.getSystemType() == null ||
        StringUtils.isBlank(system.getHost()) || system.getDefaultAuthnMethod() == null ||
        StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // Check if system already exists
    if (dao.checkForSystem(tenant, systemId, true))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, systemId));
    }

    // ==========================================================================================================
    // WARNING: Be very careful of ordering of steps from here on.
    //          Ordering of setting defaults, resolving variables and validating attributes can be critical.
    // ==========================================================================================================

    // Make sure owner, effectiveUserId, notes and tags are all set
    // Note that this is done before auth so owner can get resolved and used during auth check.
    system.setDefaults();

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    // NOTE: This also handles case where effectiveUserId is ${owner},
    //       so after this effUser is either a resolved static string or ${apiUserId}
    //       and the only variable of interest in rootDir should be HOST_EVAL($var)
    system.resolveVariablesAtCreate(rUser.getOboUserId());

    // Determine if effectiveUserId is static
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, system, true);

    // Set flag indicating if we will deal with credentials.
    // We only do that when credentials provided and effectiveUser is static
    Credential cred = system.getAuthnCredential();
    boolean manageCredentials = (cred != null && isStaticEffectiveUser);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, system.getOwner());

    // ---------------- Check for reserved names ------------------------
    checkReservedIds(rUser, systemId);

    // If credentials provided validate constraints and verify credentials
    if (cred != null)
    {
      // Skip check if not LINUX or S3
      if (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)) skipCredCheck = true;

      // static effectiveUser case. Credential must not contain loginUser
      // NOTE: If effectiveUserId is dynamic then request has already been rejected above during
      //       call to validateTSystem(). See method TSystem.checkAttrMisc().
      //       But we include isStaticEffectiveUser here anyway in case that ever changes.
      if (isStaticEffectiveUser && !StringUtils.isBlank(cred.getLoginUser()))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_LOGINUSER", rUser, systemId);
        throw new IllegalArgumentException(msg);
      }

      // ---------------- Verify credentials if not skipped
      if (!skipCredCheck && manageCredentials)
      {
        Credential c = verifyCredentials(rUser, system, cred, cred.getLoginUser(), system.getDefaultAuthnMethod());
        system.setAuthnCredential(c);
        // If credential validation failed we do not create the system. Return now.
        if (Boolean.FALSE.equals(c.getValidationResult())) return system;
      }
    }

    // Evaluate HOST_EVAL macro if necessary. ssh connection to the host will be required.
    // Due to constraints on use of HOST_EVAL in rootDir, we should have already checked the credentials above,
    // so they should be OK (unless caller has specified skipCredentialCheck=true)
    if (system.getRootDir().startsWith(HOST_EVAL_PREFIX1) || system.getRootDir().startsWith(HOST_EVAL_PREFIX2))
    {
      String resolvedRootDir = resolveRootDirHostEval(rUser, system);
      system.setRootDir(resolvedRootDir);
    }

    // For LINUX and IRODS, normalize the rootDir.
    if (SystemType.LINUX.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      String normalizedRootDir = PathUtils.getAbsolutePath("/", system.getRootDir()).toString();
      system.setRootDir(normalizedRootDir);
    }

    // Construct Json string representing the TSystem (without credentials) about to be created
    TSystem scrubbedSystem = new TSystem(system);
    scrubbedSystem.setAuthnCredential(nullCredential);
    String updateJsonStr = TapisGsonUtils.getGson().toJson(scrubbedSystem);

    // ----------------- Create all artifacts --------------------
    // Creation of system, perms and creds not in single DB transaction.
    // Use try/catch to roll back any writes in case of failure.
    boolean itemCreated = false;
    String systemsPermSpecALL = getPermSpecAllStr(tenant, systemId);
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + tenant + ":*:" + systemId;

    // Get SK client now. If we cannot get this rollback not needed.
    // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
    getSKClient(rUser);
    try
    {
      // ------------------- Make Dao call to persist the system -----------------------------------
      itemCreated = dao.createSystem(rUser, system, updateJsonStr, rawData);

      // ------------------- Add permissions -----------------------------
      // Consider using a notification instead (jira cic-3071)
      // Give owner files service related permission for root directory
      getSKClient(rUser).grantUserPermission(tenant, system.getOwner(), filesPermSpec);

      // ------------------- Store credentials -----------------------------------
      // Store credentials in Security Kernel if cred provided and effectiveUser is static
      if (manageCredentials)
      {
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Create credential
        // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
        createCredential(rUser, cred, systemId, system.getEffectiveUserId(), isStaticEffectiveUser);
      }
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      // Log error
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ROLLBACK", rUser, systemId, e0.getMessage());
      log.error(msg);

      // Rollback
      // Remove system from DB
      if (itemCreated) try {dao.hardDeleteSystem(tenant, systemId); }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "hardDelete", e.getMessage()));}
      // Remove perms
      // Consider using a notification instead (jira cic-3071)
      try { getSKClient(rUser).revokeUserPermission(tenant, system.getOwner(), filesPermSpec);  }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));}
      // Remove creds
      if (manageCredentials)
      {
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
        try { deleteCredential(rUser, systemId, system.getEffectiveUserId(), isStaticEffectiveUser); }
        catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "deleteCred", e.getMessage()));}
      }
      throw e0;
    }
    // Update dynamically computed info.
    SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), systemId);
    system.setIsPublic(systemShare.isPublic());
    system.setSharedWithUsers(systemShare.getUserList());
    system.setIsDynamicEffectiveUser(!isStaticEffectiveUser);
    return system;
  }

  /**
   * Create a new child system object given parent system id and attributes for the child system.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - parent system id
   * @param childId - desired id for child
   * @param childEffectiveUserId - child attribute
   * @param childRootDir - child attribute
   * @param childOwner - child attribute
   * @param childEnabled - child attribute
   * @param rawData - Json used to create the TSystem object - secrets should be scrubbed. Saved in update record.
   * @return TSystem with defaults set and validated credentials filled in as needed
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public TSystem createChildSystem(ResourceRequestUser rUser, String systemId, String childId, String childEffectiveUserId,
                                   String childRootDir, String childOwner, boolean childEnabled, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException {
    String resourceTenant = null;

    TSystem system = getSystem(rUser, systemId, null, false, false, nullImpersonationId,
                               nullSharedAppCtx, resourceTenant, false, false);
    if(!system.isAllowChildren()) {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_CHILDREN_NOT_PERMITTED", rUser, systemId));
    }

    if(StringUtils.isBlank(childId)) {
      childId = system.getId() + "-" + rUser.getOboUserId();
    }

    // Check if system already exists
    if (dao.checkForSystem(system.getTenant(), childId, true))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, childId));
    }

    if(StringUtils.isBlank(childOwner)) {
      childOwner = rUser.getOboUserId();
    }

    TSystem childSystem = new TSystem(system, childId, childEffectiveUserId, childRootDir, childOwner, childEnabled);

    return createSystem(rUser, childSystem, true, rawData);
  }

  /**
   * Update a system object given a PatchSystem and the text used to create the PatchSystem.
   * Secrets in the text should be masked.
   * Attributes that can be updated:
   *   description, host, effectiveUserId, defaultAuthnMethod,
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId,
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUser, canRunBatch, mpiCmd,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * Attributes that cannot be updated:
   *   tenant, id, systemType, owner, authnCredential, bucketName, rootDir, canExec
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param patchSystem - Pre-populated PatchSystem object (including tenantId and systemId)
   * @param rawData - Text used to create the PatchSystem object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void patchSystem(ResourceRequestUser rUser, String systemId, PatchSystem patchSystem, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (patchSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract various names for convenience
    String oboTenant = rUser.getOboTenantId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(rawData)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // if the patch system contains a request to set allowChildren to false, only allow
    // the change if there are no children.
    Boolean changeAllowChildren = patchSystem.getAllowChildren();
    if (BooleanUtils.isFalse(changeAllowChildren)) {
      if (dao.hasChildren(rUser.getOboTenantId(), systemId)) {
        String msg = LibUtils.getMsgAuth("SYSLIB_ERROR_HAS_CHILDREN", rUser);
        throw new IllegalStateException(msg);
      }
    }

    // If needed, create list of job env variables with proper defaults.
    // Note that because this is a patch DO NOT fill in with non-null unless it is in the request.
    // We rely on null to indicate it was not in the call to patch, method createPatchedTSystem
    if (patchSystem.getJobEnvVariables() != null)
    {
      patchSystem.setJobEnvVariables(TSystem.processJobEnvVariables(patchSystem.getJobEnvVariables()));
    }

    // Retrieve the system being patched and create fully populated TSystem with changes merged in
    TSystem origTSystem = dao.getSystem(oboTenant, systemId);
    TSystem patchedTSystem = createPatchedTSystem(origTSystem, patchSystem);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, origTSystem.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    patchedTSystem.setDefaults();
    validateTSystem(rUser, patchedTSystem, false);

    // This is a WIP and, in fact, probably not even a good idea to attempt.
    // We should instead generate the change history on demand from the raw data.
//    // Get a complete and succinct description of the update.
//    // If nothing has changed, then log a warning and return
//    String changeDescription = LibUtils.getChangeDescriptionSystemUpdate(origTSystem, patchedTSystem, patchSystem);
//    if (StringUtils.isBlank(changeDescription))
//    {
//      log.warn(LibUtils.getMsgAuth("SYSLIB_UPD_NO_CHANGE", rUser, "PATCH", systemId));
//      return;
//    }
//    dao.patchSystem(rUser, systemId, patchedTSystem, changeDescription, rawData);
    // Construct Json string representing the PatchApp about to be used to update the app
    String updateJsonStr = TapisGsonUtils.getGson().toJson(patchSystem);

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to persist the system -----------------------------------
    dao.patchSystem(rUser, systemId, patchedTSystem, updateJsonStr, rawData);
  }

  /**
   * Update all updatable attributes of a system object given a TSystem and the text used to create the TSystem.
   * Incoming TSystem must contain the tenantId and systemId.
   * Secrets in the text should be masked.
   * Attributes that cannot be updated and so will be looked up and filled in:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, canExec
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param putSystem - Pre-populated TSystem object (including tenantId and systemId)
   * @param skipCredCheck - Indicates if cred check should happen (for LINUX, S3)
   * @param rawData - Text used to create the System object - secrets should be scrubbed. Saved in update record.
   * @return TSystem with defaults set and validated credentials filled in as needed
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public TSystem putSystem(ResourceRequestUser rUser, TSystem putSystem, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (putSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract some attributes for convenience and clarity
    String oboTenant = rUser.getOboTenantId();
    String systemId = putSystem.getId();
    SystemType systemType = putSystem.getSystemType();
    String effectiveUserId = putSystem.getEffectiveUserId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(oboTenant) || StringUtils.isBlank(systemId) || StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Fill in defaults
    putSystem.setDefaults();

    // Set flag indicating if effectiveUserId is static
    boolean isStaticEffectiveUser = !effectiveUserId.equals(APIUSERID_VAR);

    // Set flag indicating if we will deal with credentials.
    // We only do that when credentials provided and effectiveUser is static
    Credential cred = putSystem.getAuthnCredential();
    boolean manageCredentials = (cred != null && isStaticEffectiveUser);

    // Retrieve the system being updated and create fully populated TSystem with updated attributes
    TSystem origTSystem = dao.getSystem(oboTenant, systemId);

    // Error if the system we are replacing had a parentId (i.e. - PUT not allowed for a child system) or if
    // the incoming request has a parentId set (i.e. trying to change the system to a child system)
    if(!StringUtils.isBlank(origTSystem.getParentId())) {
      String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_PUT_NOT_ALLOWED", rUser, systemId);
      throw new IllegalArgumentException(msg);
    }

    TSystem updatedTSystem = createUpdatedTSystem(origTSystem, putSystem);
    updatedTSystem.setAuthnCredential(cred);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, origTSystem.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, updatedTSystem, false);

    // If credentials provided validate constraints and verify credentials
    if (cred != null)
    {
      // Skip check if not LINUX or S3
      if (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)) skipCredCheck = true;

      // static effectiveUser case. Credential must not contain loginUser
      // NOTE: If effectiveUserId is dynamic then request has already been rejected above during
      //       call to validateTSystem(). See method TSystem.checkAttrMisc().
      //       But we include isStaticEffectiveUser here anyway in case that ever changes.
      if (isStaticEffectiveUser && !StringUtils.isBlank(cred.getLoginUser()))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_LOGINUSER", rUser, systemId);
        throw new IllegalArgumentException(msg);
      }

      // ---------------- Verify credentials if not skipped
      if (!skipCredCheck && manageCredentials)
      {
        Credential c = verifyCredentials(rUser, updatedTSystem, cred, cred.getLoginUser(), updatedTSystem.getDefaultAuthnMethod());
        updatedTSystem.setAuthnCredential(c);
        // If credential validation failed we do not create the system. Return now.
        if (Boolean.FALSE.equals(c.getValidationResult())) return updatedTSystem;
      }
    }

    // ------------------- Store credentials -----------------------------------
    // Store credentials in Security Kernel if cred provided and effectiveUser is static
    if (manageCredentials)
    {
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      // Create credential
      // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
      createCredential(rUser, cred, systemId, effectiveUserId, isStaticEffectiveUser);
    }

    // This is a WIP and, in fact, probably not even a good idea to attempt.
    // We should instead generate the change history on demand from the raw data.
//    // Get a complete and succinct description of the update.
//    // If nothing has changed, then log a warning and return
//    String changeDescription = LibUtils.getChangeDescriptionSystemUpdate(origTSystem, updatedTSystem, null);
//    if (StringUtils.isBlank(changeDescription))
//    {
//      log.warn(LibUtils.getMsgAuth("SYSLIB_UPD_NO_CHANGE", rUser, "PUT", systemId));
//      return updatedTSystem;
//    }
    // Construct Json string representing the PatchApp about to be used to update the app
    String updateJsonStr = TapisGsonUtils.getGson().toJson(putSystem);

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to update the system -----------------------------------
    dao.putSystem(rUser, updatedTSystem, updateJsonStr, rawData);

    // Update dynamically computed info.
    SystemShare systemShare = getSystemShareInfo(rUser, putSystem.getTenant(), systemId);
    putSystem.setIsPublic(systemShare.isPublic());
    putSystem.setSharedWithUsers(systemShare.getUserList());
    putSystem.setIsDynamicEffectiveUser(!isStaticEffectiveUser);
    return updatedTSystem;
  }

  /**
   * Update enabled to true for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int enableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    return updateEnabled(rUser, systemId, SystemOperation.enable);
  }

  /**
   * Update enabled to false for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int disableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    return updateEnabled(rUser, systemId, SystemOperation.disable);
  }

  /**
   * Soft delete a system
   *   - Remove effectiveUser credentials associated with the system.
   *   - Remove permissions associated with the system.
   *   - Update deleted to true for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int deleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    SystemOperation op = SystemOperation.delete;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // System must exist
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, true);
    if (system == null)
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // cant delete a system if it has children
    if(dao.hasChildren(rUser.getOboTenantId(), systemId)) {
      String msg = LibUtils.getMsg("SYSLIB_ERROR_HAS_CHILDREN", rUser);
      throw new IllegalStateException(msg);
    }
    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    // Remove effectiveUser credentials associated with the system
    // Remove permissions associated with the system
    removeSKArtifacts(rUser, system);

    // Update deleted attribute
    return updateDeleted(rUser, systemId, op);
  }

  /**
   * Undelete a system
   *  - Add permissions for owner
   *  - Update deleted to false for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int undeleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalArgumentException, TapisClientException {
    SystemOperation op = SystemOperation.undelete;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // System must exist
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, true);
    if (system == null) {
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));
    }

    // if this is a child system, make sure that the parent hasn't been deleted, and that
    // the parent still allows children
    if(isChildSystem(system)) {
      boolean okToUndeleteChild = false;
      TSystem parentSystem = dao.getSystem(rUser.getOboTenantId(), system.getParentId(), false);
      if (parentSystem != null) {
        if(parentSystem.isAllowChildren()) {
          okToUndeleteChild = true;
        }
      }

      if(!okToUndeleteChild) {
        String msg = LibUtils.getMsgAuth("SYSLIB_ERROR_PARENT_CHILD_CONFLICT", rUser);
        throw new IllegalStateException(msg);
      }
    }

    // Get owner, if not found it is an error
    String owner = system.getOwner();
    if (StringUtils.isBlank(owner)) {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, systemId, op.name());
      log.error(msg);
      throw new TapisException(msg);
    }
    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, owner);

    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    // Consider using a notification instead (jira cic-3071)
    // Give owner files service related permission for root directory
    getSKClient(rUser).grantUserPermission(oboTenant, owner, filesPermSpec);

    // Update deleted attribute
    return updateDeleted(rUser, systemId, op);
  }

  /**
   * Change owner of a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param newOwnerName - Username of new owner
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int changeSystemOwner(ResourceRequestUser rUser, String systemId, String newOwnerName)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    SystemOperation op = SystemOperation.changeOwner;

    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(newOwnerName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    String oboTenant = rUser.getOboTenantId();

    // System must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
         throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Retrieve old owner
    String oldOwnerName = dao.getSystemOwner(oboTenant, systemId);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, oldOwnerName);

    // If new owner same as old owner then this is a no-op
    if (newOwnerName.equals(oldOwnerName)) return 0;

    // ----------------- Make all updates --------------------
    // Changes not in single DB transaction.
    // Use try/catch to roll back any changes in case of failure.
    // Get SK client now. If we cannot get this rollback not needed.
    // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
    getSKClient(rUser);
    String systemsPermSpec = getPermSpecAllStr(oboTenant, systemId);
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    try
    {
      // ------------------- Make Dao call to update the system owner -----------------------------------
      dao.updateSystemOwner(rUser, systemId, oldOwnerName, newOwnerName);
      // Consider using a notification instead (jira cic-3071)
      // Give new owner files service related permission for root directory
      getSKClient(rUser).grantUserPermission(oboTenant, newOwnerName, filesPermSpec);

      // Remove permissions from old owner
      getSKClient(rUser).revokeUserPermission(oboTenant, oldOwnerName, filesPermSpec);

      // Get a complete and succinct description of the update.
      String changeDescription = LibUtils.getChangeDescriptionUpdateOwner(systemId, oldOwnerName, newOwnerName);
      // Create a record of the update
      dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      try { dao.updateSystemOwner(rUser, systemId, newOwnerName, oldOwnerName); } catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "updateOwner", e.getMessage()));}
      // Consider using a notification instead(jira cic-3071)
      try { getSKClient(rUser).revokeUserPermission(oboTenant, newOwnerName, filesPermSpec); }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));}
      try { getSKClient(rUser).grantUserPermission(oboTenant, oldOwnerName, filesPermSpec); }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPermF1", e.getMessage()));}
      throw e0;
    }
    return 1;
  }

  @Override
  public int unlinkFromParent(ResourceRequestUser rUser, String childSystemId) throws TapisException, TapisClientException {
    SystemOperation op = SystemOperation.modify;

    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    }

    if (StringUtils.isBlank(childSystemId)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));
    }

    String oboTenant = rUser.getOboTenantId();

    // System must already exist and not be deleted
    TSystem childSystem = dao.getSystem(oboTenant, childSystemId, false);
    if (childSystem == null) {
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, childSystemId));
    }

    // Get parent's Id
    String parentSystemId = childSystem.getParentId();
    if (parentSystemId == null) {
      // if there is no parent id, we are done.  This is already not a child system.
      return 1;
    }

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, childSystemId, childSystem.getOwner());

    // ------------------- Make Dao call to unlink the system -----------------------------------
    dao.removeParentId(rUser, oboTenant, childSystemId);
    return 1;
  }

  @Override
  public int unlinkChildren(ResourceRequestUser rUser, String parentId, List<String> childIdsToUnlink) throws TapisException, TapisClientException {
    SystemOperation op = SystemOperation.modify;

    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    }

    if (childIdsToUnlink == null) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));
    }

    String oboTenant = rUser.getOboTenantId();

    // System must already exist and not be deleted
    for(String childSystemId : childIdsToUnlink) {
      TSystem childSystem = dao.getSystem(oboTenant, childSystemId, false);
      if (childSystem == null) {
        throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, childSystemId));
      }
      if(!parentId.equals(childSystem.getParentId())) {
        throw new NotFoundException(LibUtils.getMsgAuth("SYSLIB_PARENT_CHILD_NOT_FOUND", rUser, parentId, childSystemId));
      }
    }

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, parentId);

    // ------------------- Make Dao call to unlink the system -----------------------------------
    return dao.removeParentIdFromChildren(rUser, oboTenant, parentId, childIdsToUnlink);
  }

  @Override
  public int unlinkAllChildren(ResourceRequestUser rUser, String parentId) throws TapisException, TapisClientException {
    SystemOperation op = SystemOperation.modify;

    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    }

    if (StringUtils.isBlank(parentId)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));
    }

    String oboTenant = rUser.getOboTenantId();

    // System must already exist and not be deleted
    TSystem parentSystem = dao.getSystem(oboTenant, parentId, false);
    if (parentSystem == null) {
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, parentSystem));
    }

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, parentId, parentSystem.getOwner());

    // ------------------- Make Dao call to unlink the system -----------------------------------
    return dao.removeParentIdFromAllChildren(rUser, oboTenant, parentId);
  }

  /**
   * Hard delete a system record given the system name.
   * Also remove artifacts from the Security Kernel
   * NOTE: This is package-private. Only test code should ever use it.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param oboTenant - Tenant containing resources.
   * @param systemId - name of system
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis related exceptions
   */
  int hardDeleteSystem(ResourceRequestUser rUser, String oboTenant, String systemId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.hardDelete;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(oboTenant) ||  StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // If system does not exist then 0 changes
    TSystem system = dao.getSystem(oboTenant, systemId, true);
    if (system == null) return 0;

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    // Remove SK artifacts
    removeSKArtifacts(rUser, system);

    // Delete the system
    return dao.hardDeleteSystem(oboTenant, systemId);
  }

  /**
   * Hard delete all systems in the "test" tenant.
   * Also remove artifacts from the Security Kernel.
   * NOTE: This is package-private. Only test code should ever use it.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   */
  int hardDeleteAllTestTenantResources(ResourceRequestUser rUser)
          throws TapisException, TapisClientException
  {
    // For safety hard code the tenant name
    String oboTenant = "test";
    // Fetch all resource Ids including deleted items
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    var systemIdSet = dao.getSystemIDs(oboTenant, true);
    for (String id : systemIdSet)
    {
      hardDeleteSystem(rUser, oboTenant, id);
    }
    return systemIdSet.size();
  }

  /**
   * checkForSystem
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return true if system exists and has not been deleted, false otherwise
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException
  {
    return checkForSystem(rUser, systemId, false);
  }

  /**
   * checkForSystem
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return true if system exists and has not been deleted, false otherwise
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSystem(ResourceRequestUser rUser, String systemId, boolean includeDeleted)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // We need owner to check auth and if system not there cannot find owner, so cannot do auth check if no system
    if (dao.checkForSystem(rUser.getOboTenantId(), systemId, includeDeleted)) {
      // ------------------------- Check authorization -------------------------
      checkAuthOwnerUnkown(rUser, op, systemId);
      return true;
    }
    return false;
  }

  /**
   * isEnabled
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return true if enabled, false otherwise
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean isEnabled(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // Resource must exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);
    return dao.isEnabled(oboTenant, systemId);
  }

  /**
   * getSystem
   * Retrieve specified system.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @param returnCreds - flag indicating if credentials for effectiveUserId should be included
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, resolving effectiveUserId
   * @param sharedAppCtxGrantor - Share grantor for the case of a shared application context.
   * @param resourceTenant - use provided tenant instead of oboTenant when fetching resource
   * @param fetchShareInfo - indicates if share info should be included in result
   * @param checkHasCredentials - indicates if hasCredentials info should be included in the result
   * @return populated instance of a TSystem or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public TSystem getSystem(ResourceRequestUser rUser, String systemId, AuthnMethod authnMethod, boolean requireExecPerm,
                           boolean returnCreds, String impersonationId, String sharedAppCtxGrantor,
                           String resourceTenant, boolean fetchShareInfo, boolean checkHasCredentials)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // For clarity and convenience
    // Allow for option of impersonation. Auth checked below.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;

    // Determine the tenant for the resource. For user request always oboTenant, for svc request may be overridden
    String resTenant;
    if (!rUser.isServiceRequest()) resTenant = rUser.getOboTenantId();
    else resTenant = (StringUtils.isBlank(resourceTenant)) ? rUser.getOboTenantId() : resourceTenant;

    // If impersonationId set confirm that it is allowed.
    //  - allowed for certain Tapis services and for a tenant admin
    if (!StringUtils.isBlank(impersonationId)) checkImpersonateUserAllowed(rUser, op, systemId, impersonationId, resTenant);
    // If resourceTenant set confirm it is allowed. Only allowed for certain Tapis services.
    if (!StringUtils.isBlank(resourceTenant)) checkResourceTenantAllowed(rUser, op, systemId, resourceTenant);
    // If sharedAppCtx set confirm it is allowed. Only allowed for certain Tapis services.
    if (!StringUtils.isBlank(sharedAppCtxGrantor)) checkSharedAppCtxAllowed(rUser, op, systemId);

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(resTenant, systemId);
    // We need owner to check auth and if system not there cannot find owner, so return null if no system.
    if (system == null) return null;

    String rootDir = system.getRootDir();
    if (rootDir == null) rootDir = "";
    String owner = system.getOwner();
    boolean isOwner = oboOrImpersonatedUser.equals(owner);

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);
    // Determine the host login user. Not always needed, but at most 1 extra DB call for mapped loginUser
    // And getting it now makes some code below a little cleaner and clearer.
    String resolvedEffectiveUserId = resolveEffectiveUserId(system, oboOrImpersonatedUser);

    // ------------------------- Check authorization -------------------------
    // getSystem auth check:
    // Call checkAuth (this can throw ForbiddenException)
    //   - always allow a service calling as itself to read/execute a system.
    //   - if svc not calling as itself do the normal checks using oboUserOrImpersonationId.
    // If owner is making the request we can skip this check.
    if (!isOwner)
    {
      checkAuth(rUser, op, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);
    }

    // If caller asks for credentials, explicitly check auth now
    // That way we can call private getCredential and not have overhead of getUserCredential().
    if (returnCreds) checkAuth(rUser, SystemOperation.getCred, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);

    // If flag is set to also require EXECUTE perm then make explicit auth call to make sure user has exec perm
    if (requireExecPerm)
    {
      checkAuth(rUser, SystemOperation.execute, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);
    }

    // If flag is set to also require EXECUTE perm then system must support execute
    if (requireExecPerm && !system.getCanExec())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_NOTEXEC", rUser, systemId, op.name());
      throw new ForbiddenException(msg);
    }

    system.setEffectiveUserId(resolvedEffectiveUserId);

    // Fetch the credentials if credentials are requested
    //   or if we need to compute the dynamic attribute hasCredentials.
    // Note that resolved effectiveUserId not used to look up credentials.
    // If effUsr is static then secrets stored using the "static" path in SK and static string used to build the path.
    // If effUsr is dynamic then secrets stored using the "dynamic" path in SK and a Tapis user
    //    (oboUser or impersonationId) used to build the path.
    // NOTE: Having a separate method for checkHasCredentials would not help much. We still need the call to SK.
    if (returnCreds || checkHasCredentials)
    {
      // Determine targetUser for fetching/checking credential.
      // If static use effectiveUserId, else use oboOrImpersonatedUser
      String credTargetUser;
      if (isStaticEffectiveUser)
        credTargetUser = system.getEffectiveUserId();
      else
        credTargetUser = oboOrImpersonatedUser;
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      Credential cred = getCredential(rUser, system, credTargetUser, authnMethod, isStaticEffectiveUser,
                                      resourceTenant);
      if (returnCreds) system.setAuthnCredential(cred);
      system.setHasCredentials(cred != null);
    }

    // Update dynamically computed info.
    // Fetch share info only if requested by caller
    if (fetchShareInfo)
    {
      SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), systemId);
      system.setIsPublic(systemShare.isPublic());
      system.setSharedWithUsers(systemShare.getUserList());
    }
    system.setIsDynamicEffectiveUser(!isStaticEffectiveUser);
    return system;
  }

  /**
   * Get count of all systems matching certain criteria.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param startAfter - where to start when sorting, e.g. orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param includeDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth
   * @return Count of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int getSystemsTotalCount(ResourceRequestUser rUser, List<String> searchList, List<OrderBy> orderByList,
                               String startAfter, boolean includeDeleted, String listType, String impersonationId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    // For convenience and clarity
    String tenant = rUser.getOboTenantId();
    // Allow for option of impersonation.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    // If impersonationId set confirm that it is allowed
    //  - allowed for certain Tapis services and for a tenant admin
    if (!StringUtils.isBlank(impersonationId)) checkImpersonateUserAllowed(rUser, op, null, impersonationId, tenant);

    // Process listType. Figure out how we will filter based on authorization. OWNED, ALL, etc.
    // If no listType provided use the default
    if (StringUtils.isBlank(listType)) listType = DEFAULT_LIST_TYPE.name();
    // Validate the listType enum (case-insensitive).
    listType = listType.toUpperCase();
    if (!EnumUtils.isValidEnum(AuthListType.class, listType))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_LISTTYPE_ERROR", rUser, listType);
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    AuthListType listTypeEnum = AuthListType.valueOf(listType);

    // Set some flags for convenience and clarity
    boolean allItems = AuthListType.ALL.equals(listTypeEnum);
    boolean publicOnly = AuthListType.SHARED_PUBLIC.equals(listTypeEnum);

    // Build verified list of search conditions
    var verifiedSearchList = new ArrayList<String>();
    if (searchList != null && !searchList.isEmpty())
    {
      try
      {
        for (String cond : searchList)
        {
          // Use SearchUtils to validate condition
          String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
          verifiedSearchList.add(verifiedCondStr);
        }
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
        log.error(msg, e);
        throw new IllegalArgumentException(msg);
      }
    }

    // If needed, get IDs for items for which requester has READ or MODIFY permission
    Set<String> viewableIDs = new HashSet<>();
    if (allItems) viewableIDs = getViewableSystemIDs(rUser, oboOrImpersonatedUser);

    // If needed, get IDs for items shared with the requester or only shared publicly.
    Set<String> sharedIDs = new HashSet<>();
    if (allItems) sharedIDs = getSharedSystemIDs(rUser, oboOrImpersonatedUser, false);
    else if (publicOnly) sharedIDs = getSharedSystemIDs(rUser, oboOrImpersonatedUser, true);

    // Count all allowed systems matching the search conditions
    return dao.getSystemsCount(rUser, oboOrImpersonatedUser, verifiedSearchList, null, orderByList,
                               startAfter, includeDeleted, listTypeEnum, viewableIDs, sharedIDs);
  }

  /**
   * Get all systems matching certain criteria
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param limit - indicates maximum number of results to be included, -1 for unlimited
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param skip - number of results to skip (may not be used with startAfter)
   * @param startAfter - where to start when sorting, e.g. limit=10&orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param includeDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param fetchShareInfo - indicates if share info should be included in result
   * @param checkHasCredentials - indicates if hasCredentials info should be included in the result
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, resolving effectiveUserId
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList, int limit,
                                  List<OrderBy> orderByList, int skip, String startAfter, boolean includeDeleted,
                                  String listType, boolean fetchShareInfo, boolean checkHasCredentials, String impersonationId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    // For convenience and clarity
    String tenant = rUser.getOboTenantId();
    // Allow for option of impersonation.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    // If impersonationId set confirm that it is allowed
    //  - allowed for certain Tapis services and for a tenant admin
    if (!StringUtils.isBlank(impersonationId)) checkImpersonateUserAllowed(rUser, op, null, impersonationId, tenant);

    // Process listType. Figure out how we will filter based on authorization. OWNED, ALL, etc.
    // If no listType provided use the default
    if (StringUtils.isBlank(listType)) listType = DEFAULT_LIST_TYPE.name();
    // Validate the listType enum (case-insensitive).
    listType = listType.toUpperCase();
    if (!EnumUtils.isValidEnum(AuthListType.class, listType))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_LISTTYPE_ERROR", rUser, listType);
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    AuthListType listTypeEnum = AuthListType.valueOf(listType);

    // Set some flags for convenience and clarity
    boolean allItems = AuthListType.ALL.equals(listTypeEnum);
    boolean publicOnly = AuthListType.SHARED_PUBLIC.equals(listTypeEnum);

    // Build verified list of search conditions
    var verifiedSearchList = new ArrayList<String>();
    if (searchList != null && !searchList.isEmpty())
    {
      try
      {
        for (String cond : searchList)
        {
          // Use SearchUtils to validate condition
          String verifiedCondStr = SearchUtils.validateAndProcessSearchCondition(cond);
          verifiedSearchList.add(verifiedCondStr);
        }
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
        log.error(msg, e);
        throw new IllegalArgumentException(msg);
      }
    }

    // If needed, get IDs for items for which requester has READ or MODIFY permission
    Set<String> viewableIDs = new HashSet<>();
    if (allItems) viewableIDs = getViewableSystemIDs(rUser, oboOrImpersonatedUser);

    // If needed, get IDs for items shared with the requester or only shared publicly.
    Set<String> sharedIDs = new HashSet<>();
    if (allItems) sharedIDs = getSharedSystemIDs(rUser, oboOrImpersonatedUser, false);
    else if (publicOnly) sharedIDs = getSharedSystemIDs(rUser, oboOrImpersonatedUser, true);

    // Get all allowed systems matching the search conditions
    List<TSystem> systems = dao.getSystems(rUser, oboOrImpersonatedUser, verifiedSearchList,
                                      null,  limit, orderByList, skip, startAfter,
                                           includeDeleted, listTypeEnum, viewableIDs, sharedIDs);
    // Update dynamically computed info and resolve effUser as needed.
    for (TSystem system : systems)
    {
      // Fetch credentials if we need to compute the dynamic attribute hasCredentials.
      // NOTE: Having a separate method for checkHasCredentials would not help much. We still need the call to SK.
      if (checkHasCredentials)
      {
        // Determine targetUser for fetching/checking credential.
        // If static use effectiveUserId, else use oboOrImpersonatedUser
        boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);
        String credTargetUser;
        if (isStaticEffectiveUser)
          credTargetUser = system.getEffectiveUserId();
        else
          credTargetUser = oboOrImpersonatedUser;
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        Credential cred = getCredential(rUser, system, credTargetUser, system.getDefaultAuthnMethod(), isStaticEffectiveUser,
                                        system.getTenant());
        system.setHasCredentials(cred != null);
      }

      // Fetch share info only if requested by caller
      if (fetchShareInfo)
      {
        SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), system.getId());
        system.setIsPublic(systemShare.isPublic());
        system.setSharedWithUsers(systemShare.getUserList());
      }
      system.setIsDynamicEffectiveUser(system.getEffectiveUserId().equals(APIUSERID_VAR));
      system.setEffectiveUserId(resolveEffectiveUserId(system, oboOrImpersonatedUser));
    }
    return systems;
  }

  /**
   * Get all systems
   * Use provided string containing a valid SQL where clause for the search.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sqlSearchStr - string containing a valid SQL where clause
   * @param limit - indicates maximum number of results to be included, -1 for unlimited
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param skip - number of results to skip (may not be used with startAfter)
   * @param startAfter - where to start when sorting, e.g. limit=10&orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param includeDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param fetchShareInfo - indicates if share info should be included in result
   * @param checkHasCredentials - indicates if hasCredentials info should be included in the result
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser, String sqlSearchStr, int limit,
                                                   List<OrderBy> orderByList, int skip, String startAfter,
                                                   boolean includeDeleted, String listType, boolean fetchShareInfo,
                                                   boolean checkHasCredentials)
          throws TapisException, TapisClientException
  {
    // If search string is empty delegate to getSystems()
    if (StringUtils.isBlank(sqlSearchStr)) return getSystems(rUser, null, limit, orderByList, skip, startAfter,
                                                             includeDeleted, listType, fetchShareInfo, checkHasCredentials, nullImpersonationId);

    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

    // Process listType. Figure out how we will filter based on authorization. OWNED, ALL, etc.
    // If no listType provided use the default
    if (StringUtils.isBlank(listType)) listType = DEFAULT_LIST_TYPE.name();
    // Validate the listType enum (case-insensitive).
    listType = listType.toUpperCase();
    if (!EnumUtils.isValidEnum(AuthListType.class, listType))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_LISTTYPE_ERROR", rUser, listType);
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    AuthListType listTypeEnum = AuthListType.valueOf(listType);

    // Set some flags for convenience and clarity
    boolean allItems = AuthListType.ALL.equals(listTypeEnum);
    boolean publicOnly = AuthListType.SHARED_PUBLIC.equals(listTypeEnum);

    // Validate and parse the sql string into an abstract syntax tree (AST)
    // NOTE: The activemq parser validates and parses the string into an AST but there does not appear to be a way
    //          to use the resulting BooleanExpression to walk the tree. How to now create a usable AST?
    //   I believe we don't want to simply try to run the where clause for various reasons:
    //      - SQL injection
    //      - we want to verify the validity of each <attr>.<op>.<value>
    //        looks like activemq parser will ensure the leaf nodes all represent <attr>.<op>.<value> and in principle
    //        we should be able to check each one and generate of list of errors for reporting.
    //  Looks like jOOQ can parse an SQL string into a jooq Condition. Do this in the Dao? But still seems like no way
    //    to walk the AST and check each condition, so we can report on errors.
    ASTNode searchAST;
    try { searchAST = ASTParser.parse(sqlSearchStr); }
    catch (Exception e)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
      log.error(msg, e);
      throw new IllegalArgumentException(msg);
    }

    // If needed, get IDs for items for which requester has READ or MODIFY permission
    Set<String> viewableIDs = new HashSet<>();
    if (allItems) viewableIDs = getViewableSystemIDs(rUser, rUser.getOboUserId());

    // If needed, get IDs for items shared with the requester or only shared publicly.
    Set<String> sharedIDs = new HashSet<>();
    if (allItems) sharedIDs = getSharedSystemIDs(rUser, rUser.getOboUserId(), false);
    else if (publicOnly) sharedIDs = getSharedSystemIDs(rUser, rUser.getOboUserId(), true);

    // Get all allowed systems matching the search conditions
    List<TSystem> systems = dao.getSystems(rUser, rUser.getOboUserId(), null, searchAST, limit, orderByList,
                                           skip, startAfter, includeDeleted, listTypeEnum, viewableIDs, sharedIDs);
    // Update dynamically computed info and resolve effUser as needed.
    for (TSystem system : systems)
    {
      boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);
      // Fetch credentials if we need to compute the dynamic attribute hasCredentials.
      // NOTE: Having a separate method for checkHasCredentials would not help much. We still need the call to SK.
      if (checkHasCredentials)
      {
        // Determine targetUser for fetching/checking credential.
        // If static use effectiveUserId, else use oboUser
        String credTargetUser;
        if (isStaticEffectiveUser)
          credTargetUser = system.getEffectiveUserId();
        else
          credTargetUser = rUser.getOboUserId();
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        Credential cred = getCredential(rUser, system, credTargetUser, system.getDefaultAuthnMethod(), isStaticEffectiveUser,
                system.getTenant());
        system.setHasCredentials(cred != null);
      }

      // Fetch share info only if requested by caller
      if (fetchShareInfo)
      {
        SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), system.getId());
        system.setIsPublic(systemShare.isPublic());
        system.setSharedWithUsers(systemShare.getUserList());
      }
      system.setIsDynamicEffectiveUser(!isStaticEffectiveUser);
      system.setEffectiveUserId(resolveEffectiveUserId(system, rUser.getOboUserId()));
    }
    return systems;
  }

  /**
   * Get all systems for which user has READ permission and matching specified constraint conditions.
   * Use provided string containing a valid SQL where clause for the search.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param matchStr - string containing a valid SQL where clause
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystemsSatisfyingConstraints(ResourceRequestUser rUser, String matchStr, boolean fetchShareInfo)
          throws TapisException, TapisClientException
  {
    if (rUser == null)  throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

    // Get list of IDs of systems for which requester has READ permission.
    // This is either all systems (null) or a list of IDs.
    Set<String> allowedSysIDs = getViewableSystemIDs(rUser, rUser.getOboUserId());

    // Validate and parse the sql string into an abstract syntax tree (AST)
    ASTNode matchAST;
    try { matchAST = ASTParser.parse(matchStr); }
    catch (Exception e)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_MATCH_ERROR", rUser, e.getMessage());
      log.error(msg, e);
      throw new IllegalArgumentException(msg);
    }

    // Get all allowed systems matching the constraint conditions
    List<TSystem> systems = dao.getSystemsSatisfyingConstraints(rUser.getOboTenantId(), matchAST, allowedSysIDs);

    // Update dynamically computed info and resolve effUser as needed.
    for (TSystem system : systems)
    {
      // Fetch share info only if requested by caller
      if (fetchShareInfo)
      {
        SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), system.getId());
        system.setIsPublic(systemShare.isPublic());
        system.setSharedWithUsers(systemShare.getUserList());
      }
      system.setIsDynamicEffectiveUser(system.getEffectiveUserId().equals(APIUSERID_VAR));
      system.setEffectiveUserId(resolveEffectiveUserId(system, rUser.getOboUserId()));
    }
    return systems;
  }

  /**
   * Get system owner
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return - Owner or null if system not found or user not authorized
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public String getSystemOwner(ResourceRequestUser rUser,
                               String systemId) throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // We need owner to check auth and if system not there cannot find owner, so
    // if system does not exist then return null
    if (!dao.checkForSystem(rUser.getOboTenantId(), systemId, false)) return null;

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    return dao.getSystemOwner(rUser.getOboTenantId(), systemId);
  }

  // -----------------------------------------------------------------------
  // --------------------------- Permissions -------------------------------
  // -----------------------------------------------------------------------

  /**
   * Grant permissions to a user for a system.
   * Grant of MODIFY implies grant of READ
   * NOTE: Permissions only impact the default user role
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @param permissions - list of permissions to be granted
   * @param rawData - Client provided text used to create the permissions list. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public void grantUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                                   Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.grantPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    String oboTenant = rUser.getOboTenantId();

    // If system does not exist or has been deleted then throw an exception
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // NOTE: Previously we did a check here to see if owner is trying to update permissions for themselves.
    // If so we threw an exception because this would be confusing since owner always has full permissions.
    // Due to a request (github issue #47) to change the behavior of changeSystemOwner we now allow owner to
    // grant/revoke permissions for themselves.
    // See previous code versions for implementation of checkForOwnerPermUpdate()

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Grant of MODIFY implies grant of READ
    if (permissions.contains(Permission.MODIFY)) permissions.add(Permission.READ);

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(oboTenant, systemId, permissions);

    // Assign perms to user.
    // Start of updates. Will need to rollback on failure.
    try
    {
      // Assign perms to user. SK creates a default role for the user
      for (String permSpec : permSpecSet)
      {
        getSKClient(rUser).grantUserPermission(oboTenant, targetUser, permSpec);
      }
    }
    catch (TapisClientException tce)
    {
      // Rollback
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ERROR_ROLLBACK", rUser, systemId, tce.getMessage());
      log.error(msg);

      // Revoke permissions that may have been granted.
      for (String permSpec : permSpecSet)
      {
        try { getSKClient(rUser).revokeUserPermission(oboTenant, targetUser, permSpec); }
        catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePerm", e.getMessage()));}
      }

      // Convert to TapisException and re-throw
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_PERM_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionPermsUpdate(systemId, targetUser, permissions);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);
  }

  /**
   * Revoke permissions from a user for a system
   * Revoke of READ implies revoke of MODIFY
   * NOTE: Permissions only impact the default user role
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @param permissions - list of permissions to be revoked
   * @param rawData - Client provided text used to create the permissions list. Saved in update record.
   * @return Number of items revoked
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int revokeUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                                   Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.revokePerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    String oboTenant = rUser.getOboTenantId();

    // We need owner to check auth and if system not there cannot find owner, so
    // if system does not exist or has been deleted then return 0 changes
    if (!dao.checkForSystem(oboTenant, systemId, false)) return 0;


    // NOTE: Previously we did a check here to see if owner is trying to update permissions for themselves.
    // If so we threw an exception because this would be confusing since owner always has full permissions.
    // Due to a request (github issue #47) to change the behavior of changeSystemOwner we now allow owner to
    // grant/revoke permissions for themselves.
    // See previous code versions for implementation of checkForOwnerPermUpdate()

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, permissions);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Revoke of READ implies revoke of MODIFY
    if (permissions.contains(Permission.READ)) permissions.add(Permission.MODIFY);

    int changeCount;
    // Determine current set of user permissions
    var userPermSet = getUserPermSet(rUser, targetUser, oboTenant, systemId);

    try
    {
      // Revoke perms
      changeCount = revokePermissions(rUser, oboTenant, systemId, targetUser, permissions);
    }
    catch (TapisClientException tce)
    {
      // Rollback
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ERROR_ROLLBACK", rUser, systemId, tce.getMessage());
      log.error(msg);

      // Grant permissions that may have been revoked and that the user previously held.
      for (Permission perm : permissions)
      {
        if (userPermSet.contains(perm))
        {
          String permSpec = getPermSpecStr(oboTenant, systemId, perm);
          try { getSKClient(rUser).grantUserPermission(oboTenant, targetUser, permSpec); }
          catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPerm", e.getMessage()));}
        }
      }

      // Convert to TapisException and re-throw
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_PERM_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionPermsUpdate(systemId, targetUser, permissions);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);
    return changeCount;
  }

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @return List of permissions
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public Set<Permission> getUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.getPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // If system does not exist or has been deleted then throw an exception
    if (!dao.checkForSystem(rUser.getOboTenantId(), systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use Security Kernel client to check for each permission in the enum list
    return getUserPermSet(rUser, targetUser, rUser.getOboTenantId(), systemId);
  }

  // -----------------------------------------------------------------------
  // ---------------------------- Credentials ------------------------------
  // -----------------------------------------------------------------------

  /**
   * Store or update credential for given system and target user.
   * Required: rUser, systemId, targetUser, credential.
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
  @Override
  public Credential createUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, Credential cred,
                                         boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException
  {
    SystemOperation op = SystemOperation.setCred;
    Credential retCred = null;

    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser) || cred == null)
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // Extract some attributes for convenience and clarity
    String oboTenant = rUser.getOboTenantId();
    String loginUser = cred.getLoginUser();

    // We will need some info from the system, so fetch it now.
    TSystem system = dao.getSystem(oboTenant, systemId);
    // If system does not exist or has been deleted then throw an exception
    if (system == null)
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If private SSH key is set check that we have a compatible key.
    if (!StringUtils.isBlank(cred.getPrivateKey()) && !cred.isValidPrivateSshKey())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2", rUser, systemId, targetUser);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }

    // Skip check if not LINUX or S3
    SystemType systemType = system.getSystemType();
    if (!SystemType.LINUX.equals(systemType) && !SystemType.S3.equals(systemType)) skipCredCheck = true;

    // ---------------- Verify credentials ------------------------
    // If not skipping credential validation then do it now
    if (!skipCredCheck)
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
      createCredential(rUser, cred, systemId, targetUser, isStaticEffectiveUser);
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
    String changeDescription = LibUtils.getChangeDescriptionCredCreate(systemId, targetUser, skipCredCheck, maskedCredential);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);
    return retCred;
  }

  /**
   * Delete credential for given system and user
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int deleteUserCredential(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.removeCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    int changeCount = 0;
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // If system does not exist or has been deleted then return 0 changes
    if (system == null) return changeCount;

    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Delete credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
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
   * Check user credential using given authnMethod or system default authnMethod.
   * Required: rUser, systemId, targetUser
   *
   * Secret path depends on whether effUser type is dynamic or static
   *
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is interpreted
   * as a Tapis user.
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the login user to be used when accessing the host.
   *
   * System must exist and not be deleted.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @param authnMethod - (optional) check credentials for specified authn method instead of default authn method
   * @return Checked credential with validation result set
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public Credential checkUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, IllegalStateException
  {
    SystemOperation op = SystemOperation.checkCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();

    // We will need some info from the system, so fetch it now.
    TSystem system = dao.getSystem(oboTenant, systemId);
    // If system does not exist or has been deleted then throw an exception
    if (system == null) throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

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
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }
    // ---------------- Verify credentials using defaultAuthnMethod --------------------
    return verifyCredentials(rUser, system, cred, cred.getLoginUser(), authnMethod);
  }

  /**
   * Get credential for given system, target user and authn method
   * Only certain services are authorized.
   *
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is
   * interpreted as a Tapis user. Note that their may me a mapping of the Tapis user to a host *loginUser*.
   *
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the host *loginUser* that is used when accessing the host.
   *
   * Another way to view static vs dynamic secrets in SK:
   *   If effUsr is static, then secrets stored using the "static" path in SK and static string used to build the path.
   *   If effUsr is dynamic, then secrets stored using the "dynamic" path in SK and a Tapis user
   *      (oboUser or impersonationId) used to build the path.
   *
   * Desired authentication method may be specified using query parameter authnMethod=<method>. If desired
   * authentication method not specified then credentials for the system's default authentication method are returned.
   *
   * The result includes the attribute *authnMethod* indicating the authentication method associated with
   * the returned credentials.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation. May be Tapis user or host user
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @return populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public Credential getUserCredential(ResourceRequestUser rUser, String systemId, String targetUser,
                                      AuthnMethod authnMethod)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.getCred;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    String oboTenant = rUser.getOboTenantId();

    // We will need some info from the system, so fetch it.
    TSystem system = dao.getSystem(oboTenant, systemId);
    // If system does not exist or has been deleted then return null
    if (system == null) return null;

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    // Set flag indicating if effectiveUserId is static
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // If authnMethod not passed in fill in with default from system
    if (authnMethod == null)
    {
      AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(oboTenant, systemId);
      if (defaultAuthnMethod == null)  throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));
      authnMethod = defaultAuthnMethod;
    }

    return getCredential(rUser, system, targetUser, authnMethod, isStaticEffectiveUser, null);
  }

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
  @Override
  public GlobusAuthInfo getGlobusAuthInfo(ResourceRequestUser rUser, String systemId)
          throws NotFoundException, TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.getGlobusAuthInfo;
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
    if (system == null) throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Call Tapis GlobusProxy service and create a GlobusAuthInfo from the client response;
    ResultGlobusAuthInfo r = getGlobusProxyClient(rUser).getAuthInfo(clientId, system.getHost());

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
  @Override
  public void generateAndSaveGlobusTokens(ResourceRequestUser rUser, String systemId, String userName,
                                          String authCode, String sessionId)
          throws NotFoundException, TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.setAccessRefreshTokens;
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
    if (system == null) throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, system.getOwner(), userName, null);

    // Call Tapis GlobuxProxy service to get tokens
    GlobusProxyClient globusClient = getGlobusProxyClient(rUser);
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
      createCredential(rUser, credential, systemId, userName, isStaticEffectiveUser);
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

  // -----------------------------------------------------------------------
  // ------------------- Scheduler Profiles---------------------------------
  // -----------------------------------------------------------------------

  /**
   * Create a scheduler profile.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param schedulerProfile - Pre-populated SchedulerProfile object (including tenant and name)
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - resource exists OR is in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (schedulerProfile == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    // Construct Json string representing the resource about to be created
    String createJsonStr = TapisGsonUtils.getGson().toJson(schedulerProfile);
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, createJsonStr));
    String oboTenant = schedulerProfile.getTenant();
    String schedProfileName = schedulerProfile.getName();

    // ---------------------------- Check inputs ------------------------------------
    // Required attributes: tenant, name, moduleLoadCommand
    if (StringUtils.isBlank(oboTenant) || StringUtils.isBlank(schedProfileName))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, schedProfileName));
    }

    // Check if schedulerProfile already exists
    if (dao.checkForSchedulerProfile(oboTenant, schedProfileName))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_PRF_EXISTS", rUser, schedProfileName));
    }

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    // For schedulerProfile this is only the owner which may be set to $apiUserId
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with oboUser.
    String owner = schedulerProfile.getOwner();
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) schedulerProfile.setOwner(rUser.getOboUserId());

    // Check authorization
    checkPrfAuth(rUser, op, schedulerProfile.getName(), schedulerProfile.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateSchedulerProfile(rUser, schedulerProfile);

    // No distributed transactions so no distributed rollback needed
    // Make Dao call to persist the resource
    dao.createSchedulerProfile(rUser, schedulerProfile);
  }

  /**
   * Get all scheduler profiles
   * NOTE: Anyone can read, no filtering based on auth.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @return List of scheduler profiles
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<SchedulerProfile> getSchedulerProfiles(ResourceRequestUser rUser) throws TapisException
  {
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    return dao.getSchedulerProfiles(rUser.getOboTenantId());
  }

  /**
   * getSchedulerProfile
   * NOTE: Anyone can read, no auth check
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - Name of the profile
   * @return schedulerProfile or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public SchedulerProfile getSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    // Use dao to get resource
    return dao.getSchedulerProfile(rUser.getOboTenantId(), name);
  }

  /**
   * Delete scheduler profile
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - name of profile
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int deleteSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, IllegalArgumentException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.delete;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));

    String oboTenant = rUser.getOboTenantId();

    // If profile does not exist or has been deleted then return 0 changes
    if (!dao.checkForSchedulerProfile(oboTenant, name)) return 0;

    // Check authorization
    checkPrfAuth(rUser, op, name, null);

    // Use dao to delete the resource
    return dao.deleteSchedulerProfile(oboTenant, name);
  }

  /**
   * checkForSchedulerProfile
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - Name of the profile
   * @return true if system exists and has not been deleted, false otherwise
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public boolean checkForSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));

    return dao.checkForSchedulerProfile(rUser.getOboTenantId(), name);
  }

  /**
   * Get System history records for the System ID specified
   */
  @Override
  public List<SystemHistoryItem> getSystemHistory(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);
    // ----------------- Retrieve system updates information (system history) --------------------
    List<SystemHistoryItem> systemHistory = dao.getSystemHistory(rUser.getOboTenantId(), systemId);
    return systemHistory;
  }
  
  /**
   * Get System share user IDs for the System ID specified
   */
  @Override
  public SystemShare getSystemShare(ResourceRequestUser rUser, String systemId)
      throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // We need owner to check auth and if system not there cannot find owner, so return null if no system.
    if (system == null) return null;

    checkAuth(rUser, op, systemId, system.getOwner(), nullTargetUser, nullPermSet);

    // Get the SystemShare object
    SystemShare systemShare = getSystemShareInfo(rUser, system.getTenant(), systemId);
    return systemShare;
  }
  
  /**
   * Create or update share of a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param systemShare - User names
   */
  @Override
  public void shareSystem(ResourceRequestUser rUser, String systemId, SystemShare systemShare)
      throws TapisException, TapisClientException
  {
    updateUserShares(rUser, OP_SHARE, systemId, systemShare, false);
  }
  
  /**
   * Unshare of a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param systemShare - User names
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis client related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void unshareSystem(ResourceRequestUser rUser, String systemId, SystemShare systemShare)
      throws TapisException, TapisClientException
  {
    updateUserShares(rUser, OP_UNSHARE, systemId, systemShare, false);
  }
  
  
  
  /**
   * Share a system publicly
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis client related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void shareSystemPublicly(ResourceRequestUser rUser, String systemId) 
      throws TapisException, TapisClientException
  {
    updateUserShares(rUser, OP_SHARE, systemId, nullSystemShare, true);
  }
  
  /**
   * Unshare a system publicly
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis client related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void unshareSystemPublicly(ResourceRequestUser rUser, String systemId) 
       throws TapisException, TapisClientException
  {
    updateUserShares(rUser, OP_UNSHARE, systemId, nullSystemShare, true);
  }
  

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /*
   * Given a child system id get the parent system id
   */
  public String getParentId(ResourceRequestUser rUser, String systemId) throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    }

    String oboTenant = rUser.getOboTenantId();

    // Resource must exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false)) {
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));
    }

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    return dao.getParent(oboTenant, systemId);
  }

  /*
   * Determine if a system is a child system
   */
  private static boolean isChildSystem(TSystem system)
  {
    return !StringUtils.isBlank(system.getParentId());
  }

  /**
   * Update enabled attribute for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param sysOp - operation, enable or disable
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  private int updateEnabled(ResourceRequestUser rUser, String systemId, SystemOperation sysOp)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // resource must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, sysOp, systemId);

    // ----------------- Make update --------------------
    if (sysOp == SystemOperation.enable)
      dao.updateEnabled(rUser, oboTenant, systemId, true);
    else
      dao.updateEnabled(rUser, oboTenant, systemId, false);
    return 1;
  }

  /**
   * Update deleted attribute for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param sysOp - operation, enable or disable
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   */
  private int updateDeleted(ResourceRequestUser rUser, String systemId, SystemOperation sysOp)
          throws TapisException
  {
    String oboTenant = rUser.getOboTenantId();
    // ----------------- Make update --------------------
    if (sysOp == SystemOperation.delete)
      dao.updateDeleted(rUser, oboTenant, systemId, true);
    else
      dao.updateDeleted(rUser, oboTenant, systemId, false);
    return 1;
  }

  /**
   * Get Security Kernel client with obo tenant and user set to the service tenant and user.
   * I.e. this is a client where the service calls SK as itself.
   * Note: Systems service always calls SK as itself.
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient(ResourceRequestUser rUser) throws TapisException
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

  /**
   * Check for reserved names.
   * Endpoints defined lead to certain names that are not valid.
   * Invalid names: healthcheck, readycheck, search
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param id - the id to check
   * @throws IllegalStateException - if attempt to create a resource with a reserved name
   */
  private void checkReservedIds(ResourceRequestUser rUser, String id) throws IllegalStateException
  {
    if (TSystem.RESERVED_ID_SET.contains(id.toUpperCase()))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_RESERVED", rUser, id);
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Check constraints on TSystem attributes.
   * If batchSchedulerProfile is set verify that the profile exists.
   * If DTN is used verify that dtnSystemId exists and has matching rootDir
   * Collect and report as many errors as possible, so they can all be fixed before next attempt
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param creating - indicates validation is part of a system create operation
   * @throws IllegalStateException - if any constraints are violated
   */
  private void validateTSystem(ResourceRequestUser rUser, TSystem tSystem1, boolean creating)
          throws TapisException, IllegalStateException
  {
    String msg;
    // Make checks that do not involve a dao or service call. This creates the initial list of err messages.
    List<String> errMessages = tSystem1.checkAttributeRestrictions();

    // Perform validations only done when a system is first created.
    if (creating)
    {
      // Validate use of HOST_EVAL in rootDir in the context of system creation
      // Note that rootDir can only be set at create, so we do not need to do this during updates.
      tSystem1.checkRootDirHostEvalDuringCreate(errMessages);
    }

    // If DTN is used (i.e. dtnSystemId is set) validate it
    // This checks that the DTN system exists, user has access to it, and the root directories match.
    if (!StringUtils.isBlank(tSystem1.getDtnSystemId()))
    {
      try
      {
        TSystem dtnSystem = getSystem(rUser, tSystem1.getDtnSystemId(), null, false, false,
                         null, null, null, false, false);
        LibUtils.validateDtnConfig(tSystem1, dtnSystem, errMessages);
      }
      catch (NotAuthorizedException e)
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_401", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
      catch (ForbiddenException e)
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_403", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
      catch (Exception e)
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_CHECK_ERROR", tSystem1.getDtnSystemId(), e.getMessage());
        log.error(msg, e);
        errMessages.add(msg);
      }
    }

    // If batchSchedulerProfile is set verify that the profile exists.
    if (!StringUtils.isBlank(tSystem1.getBatchSchedulerProfile()))
    {
      if (!dao.checkForSchedulerProfile(tSystem1.getTenant(), tSystem1.getBatchSchedulerProfile()))
      {
        msg = LibUtils.getMsg("SYSLIB_PRF_NO_PROFILE", tSystem1.getBatchSchedulerProfile());
        errMessages.add(msg);
      }
    }

    // If validation failed throw an exception
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(rUser, tSystem1.getId(), errMessages);
      log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }

  /**
   * Check constraints on SchedulerProfile attributes.
   * Collect and report as many errors as possible so they can all be fixed before next attempt
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param profile1 - the profile to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private void validateSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile profile1) throws IllegalStateException
  {
    String msg;
    // Make api level checks, i.e. checks that do not involve a dao or service call.
    List<String> errMessages = profile1.checkAttributeRestrictions();

    // Now make checks that do require a dao or service call.
    // NOTE: Currently no such checks needed.

    // If validation failed throw an exception
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(rUser, profile1.getName(), errMessages);
      log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }

  /**
   * Determine the user to be used to access the system.
   * Determine effectiveUserId for static and dynamic (i.e. ${apiUserId}) cases.
   * If effectiveUserId is dynamic then resolve it
   * Take into account loginUser mapping.
   * @param system - the system in question
   * @param tapisUser - tapis user associated with login, this is the oboUser or impersonationId
   * @return Resolved value for effective user.
   */
  private String resolveEffectiveUserId(TSystem system, String tapisUser)
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
   * Resolve HOST_EVAL in rootDir by connecting to the host
   * Much of this code copied from tapis-job repo, MacroResolver.replaceHostEval.
   *
   * @param system - the system
   * @return Resolved rootDir
   */
  private String resolveRootDirHostEval(ResourceRequestUser rUser, TSystem system) throws TapisException
  {
    String resolvedRootDir;
    String msg;
    String systemId = system.getId();

    // Parse full rootDir string to:
    //  - validate it
    //  - extract the argument provided to HOST_EVAL()
    //  - extract the remaining path following the initial HOST_EVAL
    // First trim any leading or trailing whitespace
    String rootDir = system.getRootDir().strip();
    Matcher m = HOST_EVAL_PATTERN.matcher(rootDir);
    // If no matches found then something went wrong. Most likely HOST_EVAL syntax problem
    if (!m.matches())
    {
      msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_NO_MATCHES", rUser, systemId, rootDir);
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }

    // There are always 2 groups, either of which might be the empty string.
    String hostEvalParm = m.group(1);
    String remainingPath = m.group(2);

    // Make sure we have non-empty env var name.
    if (StringUtils.isBlank(hostEvalParm))
    {
      msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_NO_ENV_VAR", rUser,rootDir);
      throw new IllegalArgumentException(msg);
    }

    // Parse the HOST_EVAL argument extracted from rootDir to:
    //  - validate it
    //  - extract env var name
    //  - extract optional default value
    // First trim any leading or trailing whitespace and strip off optional leading $
    hostEvalParm = StringUtils.removeStart(hostEvalParm.strip(), '$');
    m = ENV_VAR_NAME_PATTERN.matcher(hostEvalParm);
    if (!m.matches())
    {
      msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_INVALID_ENV_VAR", rUser, systemId, rootDir, hostEvalParm);
      throw new IllegalArgumentException(msg);
    }

    // Extract the variable and an optional default value, the latter of which can be null.
    String varName = m.group(1);
    String defaultValue = m.group(3);

    // We will need to make an ssh connection to the host.
    // Easiest way to do that is to use TapisRunCommand, which requires a client base TapisSystem object.
    TapisSystem tapisSystem = createTapisSystemFromTSystem(system);
    // Run the command on the host system.
    String cmd = String.format("echo $%s", varName);
    msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_RESOLVE_CMD", rUser, systemId, system.getHost(), cmd);
    log.trace(msg);
    var runCmd = new TapisRunCommand(tapisSystem);
    int exitStatus = runCmd.execute(cmd, true); // connection automatically closed
    runCmd.logNonZeroExitCode();
    String result = runCmd.getOutAsTrimmedString();
    // Trace the result
    msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_RESOLVE_EXIT", rUser, systemId, system.getHost(), cmd, exitStatus, result);
    log.trace(msg);
    if (StringUtils.isBlank(result))
    {
      if (!StringUtils.isBlank(defaultValue))
      {
        result = defaultValue;
      }
      else
      {
        msg = LibUtils.getMsgAuth("SYSLIB_HOST_EVAL_RESOLVE_EMPTY", rUser, systemId, rootDir, varName);
        throw new TapisException(msg);
      }
    }
    // Retain only the last line in multi-line value.
    // This removes any login banner message that the host might display.
    String resolvedVar = LibUtils.getLastLineFromResultString(result);

    // Replace HOST_EVAL() in rootDir with resolved env var and make sure there is a leading slash
    resolvedRootDir = resolvedVar + remainingPath;
    resolvedRootDir = StringUtils.prependIfMissing(resolvedRootDir, "/");
    return resolvedRootDir;
  }


  /**
   * Build a client based TapisSystem from a TSystem for use by MacroResolver and other shared code.
   * Need to fill in credentials and authn method if HOST_EVAL needs evaluation
   * NOTE: Following attributes are not needed and so are not set:
   *   created, updated, tags, notes, jobRuntimes, jobEnvVariables,
   *   batchScheduler, batchLogicalQueues, jobCapabilities
   * @param s - a TSystem
   * @return client-based TapisSystem built from a TSystem
   */
  private TapisSystem createTapisSystemFromTSystem(TSystem s)
  {
    Credential cred = s.getAuthnCredential();
    TapisSystem tapisSystem = new TapisSystem();
    tapisSystem.setTenant(s.getTenant());
    tapisSystem.setId(s.getId());
    tapisSystem.setDescription(s.getDescription());
    tapisSystem.setSystemType(EnumUtils.getEnum(SystemTypeEnum.class, s.getSystemType().name()));
    tapisSystem.setOwner(s.getOwner());
    tapisSystem.setHost(s.getHost());
    tapisSystem.setEnabled(s.isEnabled());
    tapisSystem.setEffectiveUserId(s.getEffectiveUserId());
    tapisSystem.setAuthnCredential(buildAuthnCred(cred, s.getDefaultAuthnMethod()));
    tapisSystem.setDefaultAuthnMethod(EnumUtils.getEnum(AuthnEnum.class, s.getDefaultAuthnMethod().name()));
    tapisSystem.setBucketName(s.getBucketName());
    tapisSystem.setRootDir(s.getRootDir());
    tapisSystem.setPort(s.getPort());
    tapisSystem.setUseProxy(s.isUseProxy());
    tapisSystem.setProxyHost(s.getProxyHost());
    tapisSystem.setProxyPort(s.getProxyPort());
    tapisSystem.setDtnSystemId(s.getDtnSystemId());
    tapisSystem.setIsPublic(s.isPublic());
    if (s.getSharedWithUsers() != null) tapisSystem.setSharedWithUsers(new ArrayList<>(s.getSharedWithUsers()));
    tapisSystem.setCanExec(s.getCanExec());
//    tapisSystem.setJobRuntimes(s.getJobRuntimes());
    tapisSystem.setJobWorkingDir(s.getJobWorkingDir());
//    tapisSystem.setJobEnvVariables(s.getJobEnvVariables());
    tapisSystem.setJobMaxJobs(s.getJobMaxJobs());
    tapisSystem.setCanRunBatch(s.getCanRunBatch());
    tapisSystem.setMpiCmd(s.getMpiCmd());
//    tapisSystem.setBatchScheduler(s.getBatchScheduler());
//    tapisSystem.setBatchLogicalQueues(s.getBatchLogicalQueues());
    tapisSystem.setBatchDefaultLogicalQueue(s.getBatchDefaultLogicalQueue());
    tapisSystem.setBatchSchedulerProfile(s.getBatchSchedulerProfile());
//    tapisSystem.setJobCapabilities(s.getJobCapabilities());
//    tapisSystem.setTags(s.getTags());
    tapisSystem.setNotes(s.getNotes());
    tapisSystem.setImportRefId(s.getImportRefId());
//    tapisSystem.setCreated(s.getCreated());
//    tapisSystem.setUpdated(s.getUpdated());
    tapisSystem.setUuid(s.getUuid());
    tapisSystem.setDeleted(s.isDeleted());
    return tapisSystem;
  }

  // Build a TapisSystem client credential based on the TSystem model credential
  private static edu.utexas.tacc.tapis.systems.client.gen.model.Credential buildAuthnCred(Credential cred,
                                                                                          AuthnMethod authnMethod)
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
    c.setCertificate(cred.getCertificate());
    c.setLoginUser(cred.getLoginUser());
    return c;
  }

  /**
   * Retrieve set of user permissions given sk client, user, tenant, id
   * @param userName - name of user
   * @param oboTenant - name of tenant associated with resource
   * @param systemId - Id of resource
   * @return - Set of Permissions for the user
   */
  private Set<Permission> getUserPermSet(ResourceRequestUser rUser, String userName, String oboTenant, String systemId)
          throws TapisClientException, TapisException
  {
    var userPerms = new HashSet<Permission>();
    for (Permission perm : Permission.values())
    {
      String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, perm.name(), systemId);
      if (getSKClient(rUser).isPermitted(oboTenant, userName, permSpec)) userPerms.add(perm);
    }
    return userPerms;
  }

  /**
   * Create a set of individual permSpec entries based on the list passed in
   * @param oboTenant - name of tenant associated with resource
   * @param systemId - resource Id
   * @param permList - list of individual permissions
   * @return - Set of permSpec entries based on permissions
   */
  private static Set<String> getPermSpecSet(String oboTenant, String systemId, Set<Permission> permList)
  {
    var permSet = new HashSet<String>();
    for (Permission perm : permList) { permSet.add(getPermSpecStr(oboTenant, systemId, perm)); }
    return permSet;
  }

  /**
   * Create a permSpec given a permission
   * @param perm - permission
   * @return - permSpec entry based on permission
   */
  private static String getPermSpecStr(String oboTenant, String systemId, Permission perm)
  {
    return String.format(PERM_SPEC_TEMPLATE, oboTenant, perm.name(), systemId);
  }

  /**
   * Create a permSpec for all permissions
   * @return - permSpec entry for all permissions
   */
  private static String getPermSpecAllStr(String oboTenant, String systemId)
  {
    return String.format(PERM_SPEC_TEMPLATE, oboTenant, "*", systemId);
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(ResourceRequestUser rUser, String systemId, List<String> msgList) {
    var sb = new StringBuilder(LibUtils.getMsgAuth("SYSLIB_CREATE_INVALID_ERRORLIST", rUser, systemId));
    sb.append(System.lineSeparator());
    if (msgList == null || msgList.isEmpty()) return sb.toString();
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  /**
   * Determine all systems for which the user has READ or MODIFY permission.
   */
  private Set<String> getViewableSystemIDs(ResourceRequestUser rUser, String oboUser)
          throws TapisException, TapisClientException
  {
    var systemIDs = new HashSet<String>();
    // Use implies to filter permissions returned. Without implies all permissions for apps, etc. are returned.
    String impliedBy = null;
    String implies = String.format("%s:%s:*:*", PERM_SPEC_PREFIX, rUser.getOboTenantId());
    var userPerms = getSKClient(rUser).getUserPerms(rUser.getOboTenantId(), oboUser, implies, impliedBy);

    // Check each perm to see if it allows user READ access.
    for (String userPerm : userPerms)
    {
      if (StringUtils.isBlank(userPerm)) continue;
      // Split based on :, permSpec has the format system:<tenant>:<perms>:<system_name>
      // NOTE: This assumes value in last field is always an id and never a wildcard.
      String[] permFields = COLON_SPLIT.split(userPerm);
      if (permFields.length < 4) continue;
      if (permFields[0].equals(PERM_SPEC_PREFIX) &&
          (permFields[2].contains(Permission.READ.name()) ||
           permFields[2].contains(Permission.MODIFY.name()) ||
           permFields[2].contains(TSystem.PERMISSION_WILDCARD)))
      {
        // If system exists add ID to the list
        // else resource no longer exists or has been deleted so remove orphaned permissions
        if (dao.checkForSystem(rUser.getOboTenantId(), permFields[3], false))
        {
          systemIDs.add(permFields[3]);
        }
        else
        {
          // Log a warning and remove the permission
          String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ORPHAN", rUser, permFields[3]);
          log.warn(msg);
          removeOrphanedSKPerms(rUser, permFields[3], rUser.getOboTenantId());
        }
      }
    }
    return systemIDs;
  }

  /**
   * Determine all systems that are shared with a user.
   */
  private Set<String> getSharedSystemIDs(ResourceRequestUser rUser, String oboUser, boolean publicOnly)
          throws TapisException, TapisClientException
  {
    var systemIDs = new HashSet<String>();

    // ------------------- Make a call to retrieve share info -----------------------
    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(rUser.getOboTenantId());
    // Set grantee based on whether we want just public or not.
    if (publicOnly) skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    else skParms.setGrantee(oboUser);

    // Call SK to get all shared with oboUser and add them to the set
    var skShares = getSKClient(rUser).getShares(skParms);
    if (skShares != null && skShares.getShares() != null)
    {
      for (SkShare skShare : skShares.getShares())
      {
        systemIDs.add(skShare.getResourceId1());
      }
    }
    return systemIDs;
  }

  /**
   * Check to see if a user has the specified permission
   * By default use JWT tenant and user from authenticatedUser, allow for optional tenant or user.
   */
  private boolean isPermitted(ResourceRequestUser rUser, String tenantToCheck, String userToCheck,
                              String systemId, Permission perm)
          throws TapisException, TapisClientException
  {
    // Use tenant and user from authenticatedUsr or optional provided values
    String tenantName = (StringUtils.isBlank(tenantToCheck) ? rUser.getJwtTenantId() : tenantToCheck);
    String userName = (StringUtils.isBlank(userToCheck) ? rUser.getJwtUserId() : userToCheck);
    String permSpecStr = getPermSpecStr(tenantName, systemId, perm);
    return getSKClient(rUser).isPermitted(tenantName, userName, permSpecStr);
  }

  /**
   * Check to see if a user has any of the set of permissions
   * By default use JWT tenant and user from rUser, allow for optional tenant or user.
   */
  private boolean isPermittedAny(ResourceRequestUser rUser, String tenantToCheck, String userToCheck,
                                 String systemId, Set<Permission> perms)
          throws TapisException, TapisClientException
  {
    // Use tenant and user from authenticatedUsr or optional provided values
    String tenantName = (StringUtils.isBlank(tenantToCheck) ? rUser.getJwtTenantId() : tenantToCheck);
    String userName = (StringUtils.isBlank(userToCheck) ? rUser.getJwtUserId() : userToCheck);
    var permSpecs = new ArrayList<String>();
    for (Permission perm : perms) {
      permSpecs.add(getPermSpecStr(tenantName, systemId, perm));
    }
    return getSKClient(rUser).isPermittedAny(tenantName, userName, permSpecs.toArray(TSystem.EMPTY_STR_ARRAY));
  }

  /**
   * Get a credential given system, targetUser, isStatic and authnMethod
   * No checks are done for incoming arguments and the system must exist
   * resourceTenant used when a service is calling as itself and needs to specify the tenant for the resource
   * Return null if no registered credentials found for specified authnMethod
   */
  private Credential getCredential(ResourceRequestUser rUser, TSystem system, String targetUser,
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
     *   key_type is sshkey, password, accesskey, token or cert
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

      // NOTE: Next line is needed for the SK call. Not clear if it should be targetUser, serviceUserId, oboUser.
      //       If not set then the first getAuthnCred in SystemsServiceTest.testUserCredentials
      //          fails. But it appears the value does not matter. Even an invalid userId appears to be OK.
      sParms.setUser(oboUser);
      // Set key type based on authn method
      if (authnMethod.equals(AuthnMethod.PASSWORD))sParms.setKeyType(KeyType.password);
      else if (authnMethod.equals(AuthnMethod.PKI_KEYS))sParms.setKeyType(KeyType.sshkey);
      else if (authnMethod.equals(AuthnMethod.ACCESS_KEY))sParms.setKeyType(KeyType.accesskey);
      else if (authnMethod.equals(AuthnMethod.TOKEN))sParms.setKeyType(KeyType.token);
      else if (authnMethod.equals(AuthnMethod.CERT))sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      SkSecret skSecret = getSKClient(rUser).readSecret(sParms);
      if (skSecret == null) return null;
      var dataMap = skSecret.getSecretMap();
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

  /*
   * Create or update a credential
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
   *   key_type is sshkey, password, accesskey, token or cert
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
  private void createCredential(ResourceRequestUser rUser, Credential credential,
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
    // Store password if present
    if (!StringUtils.isBlank(credential.getPassword()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.password);
      dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
      sParms.setData(dataMap);
      // First 2 parameters correspond to tenant and user from request payload
      // Tenant is used in constructing full path for secret, user is not used.
      getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store PKI keys if both present
    if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.sshkey);
      dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
      dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
      sParms.setData(dataMap);
      getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store Access key and secret if both present
    if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.accesskey);
      dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
      dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
      sParms.setData(dataMap);
      getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // Store Access token and Refresh token if both present
    if (!StringUtils.isBlank(credential.getAccessToken()) && !StringUtils.isBlank(credential.getRefreshToken()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.token);
      dataMap.put(SK_KEY_ACCESS_TOKEN, credential.getAccessToken());
      dataMap.put(SK_KEY_REFRESH_TOKEN, credential.getRefreshToken());
      sParms.setData(dataMap);
      getSKClient(rUser).writeSecret(oboTenant, oboUser, sParms);
    }
    // NOTE if necessary handle ssh certificate when supported
  }

  /**
   * Delete a credential
   * No checks are done for incoming arguments and the system must exist
   */
  private int deleteCredential(ResourceRequestUser rUser, String systemId, String targetUser, boolean isStatic)
          throws TapisClientException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();

    // Determine targetUserPath for the path to the secret.
    String targetUserPath = getTargetUserSecretPath(targetUser, isStatic);

    // Return 0 if credential does not exist
    var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    sMetaParms.setTenant(oboTenant).setUser(oboUser);
    sMetaParms.setSysId(systemId).setSysUser(targetUserPath);
    // NOTE: To be sure we know that the secret does not exist we need to check each key type
    //       By default keyType is sshkey which may not exist
    boolean secretNotFound = true;
    sMetaParms.setKeyType(KeyType.password);
    try { getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.token);
    try { getSKClient(rUser).readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { log.trace(e.getMessage()); }
    if (secretNotFound) return 0;

    // Construct basic SK secret parameters and attempt to destroy each type of secret.
    // If destroy attempt throws an exception then log a message and continue.
    sMetaParms.setKeyType(KeyType.password);
    try { getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.token);
    try { getSKClient(rUser).destroySecretMeta(sMetaParms); }
    catch (Exception e) { log.trace(e.getMessage()); }
    return 1;
  }

  /**
   * Remove SK artifacts associated with a System: user credentials, user permissions
   * No checks are done for incoming arguments and the system must exist
   */
  void removeSKArtifacts(ResourceRequestUser rUser, TSystem system)
          throws TapisException, TapisClientException
  {
    String systemId = system.getId();
    String oboTenant = system.getTenant();
    String effectiveUserId = system.getEffectiveUserId();

    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, "%", systemId);
    var userNames = getSKClient(rUser).getUsersWithPermission(oboTenant, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames)
    {
      revokePermissions(rUser, oboTenant, systemId, userName, ALL_PERMS);
      // Remove wildcard perm
      getSKClient(rUser).revokeUserPermission(oboTenant, userName, getPermSpecAllStr(oboTenant, systemId));
    }

    // Resolve effectiveUserId if necessary. This becomes the target user for perm and cred
    String resolvedEffectiveUserId = resolveEffectiveUserId(system, rUser.getOboUserId());

    // NOTE: Consider using a notification instead(jira cic-3071)
    // Remove files perm for owner and possibly effectiveUser
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    getSKClient(rUser).revokeUserPermission(oboTenant, system.getOwner(), filesPermSpec);
    if (!effectiveUserId.equals(APIUSERID_VAR))
      getSKClient(rUser).revokeUserPermission(oboTenant, resolvedEffectiveUserId, filesPermSpec);;

    // Remove credentials associated with the system if system has a static effectiveUserId
    if (!effectiveUserId.equals(APIUSERID_VAR)) {
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      deleteCredential(rUser, system.getId(), resolvedEffectiveUserId, true);
    }
  }

  /**
   * Remove all SK permissions associated with given system ID, tenant. System does not need to exist.
   * Used to clean up orphaned permissions.
   */
  private void removeOrphanedSKPerms(ResourceRequestUser rUser, String sysId, String tenant)
          throws TapisException, TapisClientException
  {
    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, tenant, "%", sysId);
    var userNames = getSKClient(rUser).getUsersWithPermission(tenant, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames)
    {
      revokePermissions(rUser, tenant, sysId, userName, ALL_PERMS);
      // Remove wildcard perm
      getSKClient(rUser).revokeUserPermission(tenant, userName, getPermSpecAllStr(tenant, sysId));
    }
  }

  /**
   * Revoke permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private int revokePermissions(ResourceRequestUser rUser, String oboTenant, String systemId, String userName,
                                       Set<Permission> permissions)
          throws TapisClientException, TapisException
  {
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(oboTenant, systemId, permissions);
    // Remove perms from default user role
    for (String permSpec : permSpecSet)
    {
      getSKClient(rUser).revokeUserPermission(oboTenant, userName, permSpec);
    }
    return permSpecSet.size();
  }

  /**
   * Create an updated TSystem based on the system created from a PUT request.
   * Attributes that cannot be updated and must be filled in from the original system:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, canExec
   */
  private TSystem createUpdatedTSystem(TSystem origSys, TSystem putSys)
  {
    // Rather than exposing otherwise unnecessary setters we use a special constructor.
    TSystem updatedSys = new TSystem(putSys, origSys.getTenant(), origSys.getId(), origSys.getSystemType(),
                                     origSys.getCanExec());
    updatedSys.setOwner(origSys.getOwner());
    updatedSys.setEnabled(origSys.isEnabled());
    updatedSys.setBucketName(origSys.getBucketName());
    updatedSys.setRootDir(origSys.getRootDir());
    return updatedSys;
  }

  /**
   * Merge a patch into an existing TSystem
   * Attributes that can be updated:
   *   description, host, effectiveUserId, defaultAuthnMethod,
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId,
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUers, canRunBatch, mpiCmd,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * The only attribute that can be reset to default is effectiveUserId. It is reset when
   *   a blank string is passed in.
   */
  private TSystem createPatchedTSystem(TSystem o, PatchSystem p)
  {
    // Start off with copy of original system
    TSystem p1 = new TSystem(o);
    // Override attributes if provided in the patch request.
    if (p.getDescription() != null) p1.setDescription(p.getDescription());
    if (p.getHost() != null) p1.setHost(p.getHost());
    // EffectiveUserId needs special handling. Empty string means reset to the default.
    if (p.getEffectiveUserId() != null)
    {
      if (StringUtils.isBlank(p.getEffectiveUserId()))
      {
        p1.setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
      }
      else
      {
        p1.setEffectiveUserId(p.getEffectiveUserId());
      }
    }
    if (p.getDefaultAuthnMethod() != null) p1.setDefaultAuthnMethod(p.getDefaultAuthnMethod());
    if (p.getPort() != null) p1.setPort(p.getPort());
    if (p.isUseProxy() != null) p1.setUseProxy(p.isUseProxy());
    if (p.getProxyHost() != null) p1.setProxyHost(p.getProxyHost());
    if (p.getProxyPort() != null) p1.setProxyPort(p.getProxyPort());
    if (p.getDtnSystemId() != null) p1.setDtnSystemId(p.getDtnSystemId());
    if (p.getJobRuntimes() != null) p1.setJobRuntimes(p.getJobRuntimes());
    if (p.getJobWorkingDir() != null) p1.setJobWorkingDir(p.getJobWorkingDir());
    if (p.getJobEnvVariables() != null) p1.setJobEnvVariables(p.getJobEnvVariables());
    if (p.getJobMaxJobs() != null) p1.setJobMaxJobs(p.getJobMaxJobs());
    if (p.getJobMaxJobsPerUser() != null) p1.setJobMaxJobsPerUser(p.getJobMaxJobsPerUser());
    if (p.getCanRunBatch() != null) p1.setCanRunBatch(p.getCanRunBatch());
    if (p.getEnableCmdPrefix() != null) p1.setEnableCmdPrefix(p.getEnableCmdPrefix());
    if (p.getMpiCmd() != null) p1.setMpiCmd(p.getMpiCmd());
    if (p.getBatchScheduler() != null) p1.setBatchScheduler(p.getBatchScheduler());
    if (p.getBatchLogicalQueues() != null) p1.setBatchLogicalQueues(p.getBatchLogicalQueues());
    if (p.getBatchDefaultLogicalQueue() != null) p1.setBatchDefaultLogicalQueue(p.getBatchDefaultLogicalQueue());
    if (p.getBatchSchedulerProfile() != null) p1.setBatchSchedulerProfile(p.getBatchSchedulerProfile());
    if (p.getJobCapabilities() != null) p1.setJobCapabilities(p.getJobCapabilities());
    if (p.getTags() != null) p1.setTags(p.getTags());
    if (p.getNotes() != null) p1.setNotes(p.getNotes());
    if (p.getImportRefId() != null) p1.setImportRefId(p.getImportRefId());
    if (p.getAllowChildren() != null) p1.setAllowChildren(p.getAllowChildren());
    return p1;
  }

  /**
   * Get GlobusProxy client associated with specified tenant
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @return GlobusProxy client
   * @throws TapisException - for Tapis related exceptions
   */
  private GlobusProxyClient getGlobusProxyClient(ResourceRequestUser rUser) throws TapisException
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

  // ************************************************************************
  // ************** Private Methods for Auth checking ***********************
  // ************************************************************************

  /*
   * Check for case when owner is not known and no need for impersonationId, targetUser or perms
   */
  private void checkAuthOwnerUnkown(ResourceRequestUser rUser, SystemOperation op, String systemId)
          throws TapisException, TapisClientException
  {
    checkAuth(rUser, op, systemId, nullOwner, nullTargetUser, nullPermSet, nullImpersonationId, nullSharedAppCtx);
  }

  /*
   * Check for case when owner is known and no need for impersonationId, targetUser or perms
   */
  private void checkAuthOwnerKnown(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner)
          throws TapisException, TapisClientException
  {
    checkAuth(rUser, op, systemId, owner, nullTargetUser, nullPermSet, nullImpersonationId, nullSharedAppCtx);
  }


  /**
   * Overloaded method for callers that do not support impersonation or sharing
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - app owner
   * @param targetUser - Target user for operation
   * @param perms - List of permissions for the revokePerm case
   */
  private void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                         String targetUser, Set<Permission> perms)
          throws TapisException, TapisClientException
  {
    checkAuth(rUser, op, systemId, owner, targetUser, perms, nullImpersonationId, nullSharedAppCtx);
  }

  /**
   * Standard authorization check using all arguments.
   * Check is different for service and user requests.
   *
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and authorization is denied.
   *
   * Auth check:
   *  - always allow read, execute, getPerms for a service calling as itself.
   *  - if svc not calling as itself do the normal checks using oboUserOrImpersonationId.
   *  - Note that if svc request and no special cases apply then final standard user request type check is done.
   *
   * Many callers do not support impersonation or sharing, so make them the final arguments and provide an overloaded
   *   method for simplicity.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - app owner
   * @param targetUser - Target user for operation
   * @param perms - List of permissions for the revokePerm case
   * @param impersonationId - for auth check use this user in place of oboUser
   */
  private void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                         String targetUser, Set<Permission> perms, String impersonationId, String sharedAppCtx)
          throws TapisException, TapisClientException
  {
    // Check service and user requests separately to avoid confusing a service name with a username
    if (rUser.isServiceRequest())
    {
      // NOTE: This call will do a final checkAuthOboUser() if no special cases apply.
      checkAuthSvc(rUser, op, systemId, owner, targetUser, perms, impersonationId, sharedAppCtx);
    }
    else
    {
      // This is an OboUser check
      checkAuthOboUser(rUser, op, systemId, owner, targetUser, perms, impersonationId, sharedAppCtx);
    }
  }

  /**
   * Service authorization check. Special auth exceptions and checks are made for service requests:
   *  - getCred is only allowed for certain services
   *  - Always allow read, execute, getPerms for a service calling as itself.
   *
   * If no special cases apply then final standard user request type auth check is made.
   *
   * ONLY CALL this method when it is a service request
   *
   * A check should be made for system existence before calling this method.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  private void checkAuthSvc(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                            String targetUser, Set<Permission> perms, String impersonationId, String sharedAppCtx)
          throws TapisException, TapisClientException
  {
    // If ever called and not a svc request then fall back to denied
    if (!rUser.isServiceRequest())
      throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()));

    // This is a service request. The username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    String svcTenant = rUser.getJwtTenantId();

    // For getCred, only certain services are allowed. Everyone else denied with a special message
    // Do this check first to reduce chance a request will be allowed that should not be allowed.
    if (op == SystemOperation.getCred)
    {
      if (SVCLIST_GETCRED.contains(svcName)) return;
      // Not authorized, throw an exception
      throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser, systemId, op.name()));
    }

    // Always allow read, execute, getPerms for a service calling as itself.
    if ((op == SystemOperation.read || op == SystemOperation.execute || op == SystemOperation.getPerms) &&
        (svcName.equals(rUser.getOboUserId()) && svcTenant.equals(rUser.getOboTenantId()))) return;

   // No more special cases. Do the standard auth check
   // Some services, such as Jobs, count on Systems to check auth for OboUserOrImpersonationId
   checkAuthOboUser(rUser, op, systemId, owner, targetUser, perms, impersonationId, sharedAppCtx);
  }

  /**
   * OboUser based authorization check.
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and authorization is denied.
   * Operations:
   *  Create -      must be owner or have admin role
   *  Delete -      must be owner or have admin role
   *  ChangeOwner - must be owner or have admin role
   *  GrantPerm -   must be owner or have admin role
   *  Read -     must be owner or have admin role or have READ or MODIFY permission or have share READ
   *  getPerms - must be owner or have admin role or have READ or MODIFY permission
   *  Modify - must be owner or have admin role or have MODIFY permission
   *  Execute - must be owner or have admin role or have EXECUTE permission or have share EXECUTE
   *  RevokePerm -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserRevokePerm)
   *  Set/RemoveCred -  must be owner or have admin role or (apiUserId=targetUser and READ access)
   *  RemoveCred -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserCredOp)
   *  GetCred -     Deny. Only authorized services may get credentials. Set specific message.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - system owner
   * @param targetUser Target user for operation
   * @param perms - List of permissions for the revokePerm case
   * @param impersonationId - for auth check use this Id in place of oboUser
   */
  private void checkAuthOboUser(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                                String targetUser, Set<Permission> perms, String impersonationId, String sharedAppCtx)
          throws TapisException, TapisClientException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;

    // Some checks do not require owner
    // Only an admin can hard delete
    switch(op) {
      case hardDelete:
        if (hasAdminRole(rUser)) return;
        break;
      case getCred:
        // Only some services allowed to get credentials. Never a user.
        throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser, systemId, op.name()));
    }

    // Remaining checks require owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSystemOwner(oboTenant, systemId);
    if (StringUtils.isBlank(owner))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, systemId, op.name());
      log.error(msg);
      throw new TapisException(msg);
    }
    switch(op) {
      case create:
      case enable:
      case disable:
      case delete:
      case undelete:
      case changeOwner:
      case grantPerms:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser)) return;
        break;
      case read:
        // Sharing support makes check complicated. Use separate method.
        if (checkAuthReadExecIncludeSharing(rUser, systemId, op, owner, oboOrImpersonatedUser, sharedAppCtx)) return;
        break;
      case getPerms:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
            isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, READMODIFY_PERMS)) return;
        break;
      case modify:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
            isPermitted(rUser, oboTenant, oboOrImpersonatedUser, systemId, Permission.MODIFY)) return;
        break;
      case execute:
        // Sharing support makes check complicated. Use separate method.
        if (checkAuthReadExecIncludeSharing(rUser, systemId, op, owner, oboOrImpersonatedUser, sharedAppCtx)) return;
        break;
      case revokePerms:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
            (oboOrImpersonatedUser.equals(targetUser) && allowUserRevokePerm(rUser, systemId, perms))) return;
        break;
      case setCred:
      case removeCred:
      case checkCred:
      case setAccessRefreshTokens:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
             (oboOrImpersonatedUser.equals(targetUser) && isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, READMODIFY_PERMS)) ||
             (oboOrImpersonatedUser.equals(targetUser) && isSystemSharedWithUser(rUser, systemId, oboOrImpersonatedUser, Permission.READ)))
          return;
        break;
    }
    // Not authorized, throw an exception
    throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()));
  }

  /*
   * Check for READ or EXEC auth for obo user including checks involving share grantor in the case of a shared app context.
   * Return true if allowed, false if not allowed
   */
  private boolean checkAuthReadExecIncludeSharing(ResourceRequestUser rUser, String systemId, SystemOperation op,
                                                  String owner, String oboOrImpersonatedUser, String sharedAppCtxGrantor)
          throws TapisException, TapisClientException
  {
    String oboTenant = rUser.getOboTenantId();
    boolean inSharedAppCtx = !StringUtils.isBlank(sharedAppCtxGrantor);
    // Start with owner checks. If owner then no need for calls to SK.
    // If obo user is owner or in shared context and share grantor is owner then allow.
    if (oboOrImpersonatedUser.equals(owner) || (inSharedAppCtx && sharedAppCtxGrantor.equals(owner))) return true;

    // Figure out which perms to check. Those for READ or those for EXECUTE
    Permission sharePerm = Permission.READ;
    Set<Permission> anyPerms = READMODIFY_PERMS;
    if (SystemOperation.execute.equals(op))
    {
      sharePerm = Permission.EXECUTE;
      anyPerms = EXECUTE_PERMS;
    }
    // If obo user is allowed for any of usual reasons then allow.
    // Allowed if:
    //    obo is admin, obo has fined-grained permissions, system is shared with obo
    if (hasAdminRole(rUser) || isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, anyPerms) ||
        isSystemSharedWithUser(rUser, systemId, oboOrImpersonatedUser, sharePerm)) return true;

    // If in shared app context and share grantor has access then allow.
    // Allowed if:
    //    share grantor has fine-grained permissions, system is shared with grantor
    // NOTE: share grantor is not given tenant admin authorizations
    if (inSharedAppCtx &&
        (isPermittedAny(rUser, oboTenant, sharedAppCtxGrantor, systemId, anyPerms) ||
         isSystemSharedWithUser(rUser, systemId, sharedAppCtxGrantor, sharePerm))) return true;
    // Not authorized, return false
    return false;
  }

  /**
   * 
   * Check if the system is shared with the user.
   * SK call hasPrivilege includes check for public sharing.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - system to check
   * @param targetUser - user to check
   * @param privilege - privilege to check
   * @return - Boolean value that indicates if app is shared
   */
  
  private boolean isSystemSharedWithUser(ResourceRequestUser rUser, String systemId, String targetUser, Permission privilege)
          throws TapisClientException, TapisException
  {
    String oboTenant = rUser.getOboTenantId();
    // Create SKShareGetSharesParms needed for SK calls.
    SKShareHasPrivilegeParms skParms = new SKShareHasPrivilegeParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(oboTenant);
    skParms.setResourceId1(systemId);
    skParms.setGrantee(targetUser);
    skParms.setPrivilege(privilege.name());
    return getSKClient(rUser).hasPrivilege(skParms);
  }
  
  /**
   * Confirm that caller is allowed to impersonate a Tapis user.
   * Must be a service request from a service allowed to impersonate
   * impersonationId and resourceTenant used for logging only.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  private void checkImpersonateUserAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId,
                                           String impersonationId, String resourceTenant)
          throws TapisException, TapisClientException
  {
    // If a user request and user is a tenant admin then log message and allow.
    if (!rUser.isServiceRequest() && hasAdminRole(rUser))
    {
      // A tenant admin is impersonating, log message and allow
      log.info(LibUtils.getMsgAuth("SYSLIB_AUTH_USR_IMPERSONATE", rUser, systemId, op.name(), impersonationId, resourceTenant));
      return;
    }
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    // If a service request and service is in the allowed list then log message and allow.
    if (rUser.isServiceRequest() && SVCLIST_IMPERSONATE.contains(svcName))
    {
      log.info(LibUtils.getMsgAuth("SYSLIB_AUTH_SVC_IMPERSONATE", rUser, systemId, op.name(), impersonationId, resourceTenant));
      return;
    }
    // Deny authorization
    throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_IMPERSONATE", rUser, systemId, op.name(), impersonationId, resourceTenant));
  }

  /**
   * Confirm that caller is allowed to set resourceTenant
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  private void checkResourceTenantAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId,
                                          String resourceTenant)
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    // If a service request and service is in the allowed list then log message and allow.
    if (rUser.isServiceRequest() && SVCLIST_RESOURCETENANT.contains(svcName))
    {
      log.trace(LibUtils.getMsgAuth("SYSLIB_AUTH_RESOURCETENANT", rUser, systemId, op.name(), resourceTenant));
      return;
    }
    // Deny authorization
    throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_RESOURCETENANT", rUser, systemId, op.name(), resourceTenant));
  }

  /**
   * Confirm that caller is allowed to set sharedAppCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  private void checkSharedAppCtxAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId)
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    // If a service request and service is in the allowed list then log message and allow.
    if (rUser.isServiceRequest() && SVCLIST_SHAREDAPPCTX.contains(svcName))
    {
      // An allowed service is setting shared context, log message and allow
      log.trace(LibUtils.getMsgAuth("SYSLIB_AUTH_SHAREDAPPCTX", rUser, systemId, op.name()));
      return;
    }
    throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_SHAREDAPPCTX", rUser, systemId, op.name()));
  }

  /**
   * Authorization check for Scheduler Profile operations.
   * A check should be made for existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and authorization is denied.
   * NOTE: SK only used to check for admin role. Anyone can read and only owner/admin can create/delete
   * Operations:
   *  Create -  must be owner or have admin role
   *  Delete -  must be owner or have admin role
   *  Read -    everyone is authorized
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param name - name of the profile
   * @param owner - owner
   */
  private void checkPrfAuth(ResourceRequestUser rUser, SchedulerProfileOperation op, String name, String owner)
          throws TapisException, TapisClientException
  {
    // Anyone can read, including all services
    if (op == SchedulerProfileOperation.read) return;

    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();

    // Check requires owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSchedulerProfileOwner(oboTenant, name);
    if (StringUtils.isBlank(owner))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, name, op.name());
      log.error(msg);
      throw new TapisException(msg);
    }

    // Owner and Admin can create, delete
    switch(op) {
      case create:
      case delete:
        if (owner.equals(oboUser) || hasAdminRole(rUser)) return;
        break;
    }
    // Not authorized, throw an exception
    throw new ForbiddenException(LibUtils.getMsgAuth("SYSLIB_PRF_UNAUTH", rUser, name, op.name()));
  }

  /**
   * Check to see if the oboUser has the admin role in the obo tenant
   */
  private boolean hasAdminRole(ResourceRequestUser rUser) throws TapisException, TapisClientException
  {
    return getSKClient(rUser).isAdmin(rUser.getOboTenantId(), rUser.getOboUserId());
  }

  /**
   * Check to see if a user who is not owner or admin is authorized to revoke permissions
   * If oboUser is revoking only READ then only need READ, otherwise also need MODIFY
   */
  private boolean allowUserRevokePerm(ResourceRequestUser rUser, String systemId, Set<Permission> perms)
          throws TapisException, TapisClientException
  {
    // Perms should never be null. Fall back to deny as best security practice.
    if (perms == null) return false;
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    if (perms.contains(Permission.MODIFY)) return isPermitted(rUser, oboTenant, oboUser, systemId, Permission.MODIFY);
    if (perms.contains(Permission.READ)) return isPermittedAny(rUser, oboTenant, oboUser, systemId, READMODIFY_PERMS);
    return false;
  }

  /*
   * Return segment of secret path for target user, including static or dynamic scope
   * Note that SK uses + rather than / to create sub-folders.
   */
  static private String getTargetUserSecretPath(String targetUser, boolean isStatic)
  {
    return String.format("%s+%s", isStatic ? "static" : "dynamic", targetUser);
  }
  
  // ************************************************************************
  // ****************** Private Methods for Sharing  ************************
  // ************************************************************************

  /*
   * Determine if a system is shared publicly
   */
  private boolean isSystemSharedPublic(ResourceRequestUser rUser, String tenant, String sysId)
          throws TapisException, TapisClientException
  {
    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(tenant);
    skParms.setResourceId1(sysId);
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    var skShares = getSKClient(rUser).getShares(skParms);
    return (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
  }

  /*
   * Get system share info
   */
  private SystemShare getSystemShareInfo(ResourceRequestUser rUser, String tenant, String sysId)
          throws TapisException, TapisClientException
  {
    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(tenant);
    skParms.setResourceId1(sysId);

    // First determine if system is publicly shared. Search for share to grantee ~public
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    var skShares = getSKClient(rUser).getShares(skParms);
    // Set isPublic based on result.
    boolean isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());

    // Now get all the users with whom the system has been shared
    var userSet = new HashSet<String>();
    skParms.setGrantee(null);
    skParms.setIncludePublicGrantees(false);
    skShares = getSKClient(rUser).getShares(skParms);
    if (skShares != null && skShares.getShares() != null)
    {
      for (SkShare skShare : skShares.getShares())
      {
        userSet.add(skShare.getGrantee());
      }
    }
    var shareInfo = new SystemShare(isPublic, userSet);
    return shareInfo;
  }

  /*
   * Common routine to update share/unshare for a list of users.
   * Can be used to mark a system publicly shared with all users in tenant including "~public" in the set of users.
   * Sharing and unsharing always involves privileges READ and EXECUTE.
   *
   * @param rUser - Resource request user
   * @param shareOpName - Operation type: share/unshare
   * @param systemId - System ID
   * @param  systemShare - System share object
   * @param isPublic - Indicates if the sharing operation is public
   * @throws TapisClientException - for Tapis client exception
   * @throws TapisException - for Tapis exception
   */
  private void updateUserShares(ResourceRequestUser rUser, String shareOpName, String systemId, SystemShare systemShare, boolean isPublic)
          throws TapisClientException, TapisException
  {
    SystemOperation op = SystemOperation.modify;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    Set<String> userList;
    if (!isPublic) {
      // if is not public update userList must have items
      if (systemShare == null || systemShare.getUserList() ==null || systemShare.getUserList().isEmpty())
        throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_USER_LIST", rUser));
      userList = systemShare.getUserList();
    } else {
      userList = publicUserSet; // "~public"
    }

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // We need owner to check auth and if system not there cannot find owner.
    if (system == null) throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    checkAuth(rUser, op, systemId, system.getOwner(), nullTargetUser, nullPermSet, nullImpersonationId, nullSharedAppCtx);

    switch (shareOpName)
    {
      case OP_SHARE ->
      {
        // Create request object needed for SK calls.
        var reqShareResource = new ReqShareResource();
        reqShareResource.setResourceType(SYS_SHR_TYPE);
        reqShareResource.setTenant(system.getTenant());
        reqShareResource.setResourceId1(systemId);
        reqShareResource.setGrantor(rUser.getOboUserId());

        for (String userName : userList)
        {
          reqShareResource.setGrantee(userName);
          reqShareResource.setPrivilege(Permission.READ.name());
          getSKClient(rUser).shareResource(reqShareResource);
          reqShareResource.setPrivilege(Permission.EXECUTE.name());
          getSKClient(rUser).shareResource(reqShareResource);
        }
      }
      case OP_UNSHARE ->
      {
        // Create object needed for SK calls.
        SKShareDeleteShareParms deleteShareParms = new SKShareDeleteShareParms();
        deleteShareParms.setResourceType(SYS_SHR_TYPE);
        deleteShareParms.setTenant(system.getTenant());
        deleteShareParms.setResourceId1(systemId);
        deleteShareParms.setGrantor(rUser.getOboUserId());

        for (String userName : userList)
        {
          deleteShareParms.setGrantee(userName);
          deleteShareParms.setPrivilege(Permission.READ.name());
          getSKClient(rUser).deleteShare(deleteShareParms);
          deleteShareParms.setPrivilege(Permission.EXECUTE.name());
          getSKClient(rUser).deleteShare(deleteShareParms);
        }
      }
    }
  }

  // *****************************************************************************************
  // ****************** Private Methods for Credential Validation and dir creation ***********
  // *****************************************************************************************

  /**
   * Verify that effectiveUserId can connect to the system using provided credentials and authnMethod
   * If loginUser is set then use it for connection,
   * else if effectiveUserId is ${apUserId} then use rUser.oboUser for connection
   * else use static effectiveUserId from TSystem for connection
   *
   * TSystem and Credential must be provided. If authnMethod not provided it is taken from the System.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param cred - credentials to check
   * @param loginUser - host login user from mapping
   * @param authnMethod - AuthnMethod to verify
   * @throws IllegalStateException - if credentials not verified
   */
  private Credential verifyCredentials(ResourceRequestUser rUser, TSystem tSystem1, Credential cred,
                                       String loginUser, AuthnMethod authnMethod)
          throws TapisException
  {
    String op = "verifyCredentials";
    // Create an initial cred as a fallback to return if there is an error.
    Credential retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
            cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
            cred.getAccessToken(), cred.getRefreshToken(), cred.getCertificate());
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
    else effectiveUser = resolveEffectiveUserId(tSystem1, rUser.getOboUserId());

    // Make sure it is supported for the system type
    if (SystemType.GLOBUS.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      // Not supported. Return now.
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, effectiveUser, authnMethod);
      return new Credential(AuthnMethod.PKI_KEYS, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                           cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(),
                           cred.getAccessToken(), cred.getRefreshToken(), cred.getCertificate(), Boolean.FALSE, msg);
    }
    return verifyConnection(rUser, op, tSystem1, authnMethod, cred, effectiveUser);
  }

  /*
   * Verify connection based on authentication method
   */
  private Credential verifyConnection(ResourceRequestUser rUser, String op, TSystem tSystem1, AuthnMethod authnMethod,
                                      Credential cred, String effectiveUser)
  {
    log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_START", rUser, tSystem1.getId(), tSystem1.getSystemType(),
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
    String msg;
    if ((doingLinux && !SystemType.LINUX.equals(systemType)) || (doingAccessKey && !SystemType.S3.equals(systemType)))
    {
      // System is not LINUX. Not supported.
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_SUPPORTED", rUser, systemId, systemType, effectiveUser, authnMethod);
      retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                               cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                               cred.getRefreshToken(), cred.getCertificate(), Boolean.FALSE, msg);
    }
    else if ((doingPki && (StringUtils.isBlank(cred.getPublicKey()) || StringUtils.isBlank(cred.getPrivateKey()))) ||
             (doingPassword && StringUtils.isBlank(cred.getPassword())) ||
             (doingAccessKey && (StringUtils.isBlank(cred.getAccessKey()) || StringUtils.isBlank(cred.getAccessSecret()))))
    {
      // We do not have the credentials we need
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_FOUND", rUser, op, systemId, systemType, effectiveUser, authnMethod);
      retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                               cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                               cred.getRefreshToken(), cred.getCertificate(), Boolean.FALSE, msg);
    }
    else
    {
      // Make the connection attempt
      // Try to handle as many exceptions as we can. For this reason, in each case there is a final catch of Exception
      //   which is re-thrown as a TapisException.
      log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_CONN", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
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
          return new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                                cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                                cred.getRefreshToken(), cred.getCertificate(), Boolean.FALSE, msg);
      }

      // We have made the connection attempt. Check the result.
      if (te == null)
      {
        // No problem with connection. Set result to TRUE
        retCred = new Credential(authnMethod, cred.getLoginUser(), cred.getPassword(), cred.getPrivateKey(),
                                 cred.getPublicKey(), cred.getAccessKey(), cred.getAccessSecret(), cred.getAccessToken(),
                                 cred.getRefreshToken(), cred.getCertificate(), Boolean.TRUE, null);
      }
      else
      {
        //
        // There was a problem. Try to figure out why. Set result to FALSE
        //
        Throwable cause = te.getCause();
        String eMsg = te.getMessage();
        if (te instanceof TapisSSHAuthException && cause != null && cause.getMessage().contains(NO_MORE_AUTH_METHODS))
        {
          // There was a special message in an SSH connection exception indicating credentials invalid.
          msg = LibUtils.getMsgAuth("SYSLIB_CRED_VALID_FAIL", rUser, tSystem1.getId(), tSystem1.getSystemType(), host,
                                    effectiveUser, authnMethod, cause.getMessage());
        }
        else if (cause instanceof S3Exception && Status.FORBIDDEN.getStatusCode() == ((S3Exception) cause).statusCode())
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
                                 cred.getRefreshToken(), cred.getCertificate(), Boolean.FALSE, msg);
      }
    }
    log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_END", rUser, tSystem1.getId(), tSystem1.getSystemType(),
                                   effectiveUser, authnMethod));
    return retCred;
  }
}
