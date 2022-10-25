package edu.utexas.tacc.tapis.systems.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
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
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.SchedulerProfileOperation;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.SystemShare;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SYSTEMS_SERVICE;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_SECRET;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PASSWORD;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_EFFECTIVEUSERID;

/*
 * Service level methods for Systems.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 * Annotate as an hk2 Service so that default scope for DI is singleton
 */
@Service
public class SystemsServiceImpl implements SystemsService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SystemsServiceImpl.class);

  // Permspec format for systems is "system:<tenant>:<perm_list>:<system_id>"
  public static final String PERM_SPEC_TEMPLATE = "system:%s:%s:%s";
  private static final String PERM_SPEC_PREFIX = "system";
  
  private static final String SYS_SHR_TYPE = "system";

  private static final Set<Permission> ALL_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY, Permission.EXECUTE));
  private static final Set<Permission> READMODIFY_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String FILES_SERVICE = TapisConstants.SERVICE_NAME_FILES;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;
  private static final Set<String> SVCLIST_GETCRED = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(FILES_SERVICE, APPS_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_SHAREDAPPCTX = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));

  // Message keys
  private static final String ERROR_ROLLBACK = "SYSLIB_ERROR_ROLLBACK";
  private static final String NOT_FOUND = "SYSLIB_NOT_FOUND";

  // NotAuthorizedException requires a Challenge, although it serves no purpose here.
  private static final String NO_CHALLENGE = "NoChallenge";

  // Compiled regex for splitting around ":"
  private static final Pattern COLON_SPLIT = Pattern.compile(":");

  // Named and typed null values to make it clear what is being passed in to a method
  private static final String nullOwner = null;
  private static final String nullImpersonationId = null;
  private static final String nullTargetUser = null;
  private static final Set<Permission> nullPermSet = null;
  private static final SystemShare nullSystemShare = null;
  
  // Sharing constants
  private static final String OP_SHARE = "share";
  private static final String OP_UNSHARE = "unShare";
  private static final Set<String> publicUserSet = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************

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
  public static String getSiteId() {return siteId;}
  private static String siteAdminTenantId;
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
   * @param skipCredCheck - Indicates if cred check for LINUX systems should happen
   * @param rawData - Json used to create the TSystem object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void createSystem(ResourceRequestUser rUser, TSystem system, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (system == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    _log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, rawData));

    // Extract some attributes for convenience and clarity.
    // NOTE: do not do this for effectiveUserId since it may be ${owner} and get resolved below.
    String tenant = system.getTenant();
    String systemId = system.getId();
    Credential credential = system.getAuthnCredential();

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

    // Make sure owner, effectiveUserId, notes and tags are all set
    // Note that this is done before auth so owner can get resolved and used during auth check.
    system.setDefaults();

    // Set flag indicating if effectiveUserId is static
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // Set flag indicating if we will deal with credentials.
    // We only do that when credentials provided and effectiveUser is static
    boolean manageCredentials = (credential != null && isStaticEffectiveUser);

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    // NOTE: That this also handles case where effectiveUserId is ${owner},
    //       so after this effUser is either a resolved static string or ${apiUserId}
    system.resolveVariablesAtCreate(rUser.getOboUserId());

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, system.getOwner());

    // ---------------- Check for reserved names ------------------------
    checkReservedIds(rUser, systemId);

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, system);

    // If credentials provided validate constraints and verify credentials
    if (credential != null)
    {
      // static effectiveUser case. Credential must not contain loginUser
      // NOTE: If effectiveUserId is dynamic then request has already been rejected above during
      //       call to validateTSystem(). See method TSystem.checkAttrMisc().
      //       But we include isStaticEffectiveUser here anyway in case that ever changes.
      if (isStaticEffectiveUser && !StringUtils.isBlank(credential.getLoginUser()))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_LOGINUSER", rUser, systemId);
        throw new IllegalArgumentException(msg);
      }

      // ---------------- Verify credentials if not skipped
      if (!skipCredCheck) verifyCredentials(rUser, system, credential, credential.getLoginUser());
    }

    // Construct Json string representing the TSystem (without credentials) about to be created
    // This will be used as the description for the change history record
    TSystem scrubbedSystem = new TSystem(system);
    scrubbedSystem.setAuthnCredential(null);
    String changeDescription = TapisGsonUtils.getGson().toJson(scrubbedSystem);

    // ----------------- Create all artifacts --------------------
    // Creation of system, perms and creds not in single DB transaction.
    // Use try/catch to rollback any writes in case of failure.
    boolean itemCreated = false;
    String systemsPermSpecALL = getPermSpecAllStr(tenant, systemId);
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + tenant + ":*:" + systemId;

    // Get SK client now. If we cannot get this rollback not needed.
    // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
    getSKClient();
    try
    {
      // ------------------- Make Dao call to persist the system -----------------------------------
      itemCreated = dao.createSystem(rUser, system, changeDescription, rawData);

      // ------------------- Add permissions -----------------------------
      // Give owner full access to the system
      getSKClient().grantUserPermission(tenant, system.getOwner(), systemsPermSpecALL);
      // Consider using a notification instead (jira cic-3071)
      // Give owner files service related permission for root directory
      getSKClient().grantUserPermission(tenant, system.getOwner(), filesPermSpec);

      // ------------------- Store credentials -----------------------------------
      // Store credentials in Security Kernel if cred provided and effectiveUser is static
      if (manageCredentials)
      {
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Create credential
        // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
        createCredential(rUser, credential, systemId, system.getEffectiveUserId(), isStaticEffectiveUser);
      }
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      // Log error
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ROLLBACK", rUser, systemId, e0.getMessage());
      _log.error(msg);

      // Rollback
      // Remove system from DB
      if (itemCreated) try {dao.hardDeleteSystem(tenant, systemId); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "hardDelete", e.getMessage()));}
      // Remove perms
      try { getSKClient().revokeUserPermission(tenant, system.getOwner(), systemsPermSpecALL); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermOwner", e.getMessage()));}
      // Consider using a notification instead (jira cic-3071)
      try { getSKClient().revokeUserPermission(tenant, system.getOwner(), filesPermSpec);  }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));}
      // Remove creds
      if (manageCredentials)
      {
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
        try { deleteCredential(rUser, systemId, system.getEffectiveUserId(), isStaticEffectiveUser); }
        catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "deleteCred", e.getMessage()));}
      }
      throw e0;
    }
  }

  /**
   * Update a system object given a PatchSystem and the text used to create the PatchSystem.
   * Secrets in the text should be masked.
   * Attributes that can be updated:
   *   description, host, effectiveUserId, defaultAuthnMethod,
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId, dtnMountPoint, dtnMountSourcePath,
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUser, canRunBatch, mpiCmd,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * Attributes that cannot be updated:
   *   tenant, id, systemType, owner, authnCredential, bucketName, rootDir, canExec, isDtn
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param patchSystem - Pre-populated PatchSystem object (including tenantId and systemId)
   * @param rawData - Text used to create the PatchSystem object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void patchSystem(ResourceRequestUser rUser, String systemId, PatchSystem patchSystem, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (patchSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract various names for convenience
    String oboTenant = rUser.getOboTenantId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Retrieve the system being patched and create fully populated TSystem with changes merged in
    TSystem origTSystem = dao.getSystem(oboTenant, systemId);
    TSystem patchedTSystem = createPatchedTSystem(origTSystem, patchSystem);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, origTSystem.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    patchedTSystem.setDefaults();
    validateTSystem(rUser, patchedTSystem);

    // Get a complete and succinct description of the update.
    // If nothing has changed, then log a warning and return
    String changeDescription = LibUtils.getChangeDescriptionSystemUpdate(origTSystem, patchedTSystem, patchSystem);
    if (StringUtils.isBlank(changeDescription))
    {
      _log.warn(LibUtils.getMsgAuth("SYSLIB_UPD_NO_CHANGE", rUser, "PATCH", systemId));
      return;
    }

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to persist the system -----------------------------------
    dao.patchSystem(rUser, systemId, patchedTSystem, changeDescription, rawData);
  }

  /**
   * Update all updatable attributes of a system object given a TSystem and the text used to create the TSystem.
   * Incoming TSystem must contain the tenantId and systemId.
   * Secrets in the text should be masked.
   * Attributes that cannot be updated and so will be looked up and filled in:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, canExec, isDtn
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param putSystem - Pre-populated TSystem object (including tenantId and systemId)
   * @param skipCredCheck - Indicates if cred check for LINUX systems should happen
   * @param rawData - Text used to create the System object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void putSystem(ResourceRequestUser rUser, TSystem putSystem, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (putSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract some attributes for convenience and clarity
    String oboTenant = rUser.getOboTenantId();
    String systemId = putSystem.getId();
    String effectiveUserId = putSystem.getEffectiveUserId();
    Credential credential = putSystem.getAuthnCredential();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(oboTenant) || StringUtils.isBlank(systemId) || StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));


    // Set flag indicating if effectiveUserId is static
    boolean isStaticEffectiveUser = !effectiveUserId.equals(APIUSERID_VAR);


    // Set flag indicating if we will deal with credentials.
    // We only do that when credentials provided and effectiveUser is static
    boolean manageCredentials = (credential != null && isStaticEffectiveUser);

    // Retrieve the system being updated and create fully populated TSystem with updated attributes
    TSystem origTSystem = dao.getSystem(oboTenant, systemId);
    TSystem updatedTSystem = createUpdatedTSystem(origTSystem, putSystem);
    updatedTSystem.setAuthnCredential(credential);

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, origTSystem.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, updatedTSystem);

    // If credentials provided validate constraints and verify credentials
    if (credential != null)
    {
      // static effectiveUser case. Credential must not contain loginUser
      // NOTE: If effectiveUserId is dynamic then request has already been rejected above during
      //       call to validateTSystem(). See method TSystem.checkAttrMisc().
      //       But we include isStaticEffectiveUser here anyway in case that ever changes.
      if (isStaticEffectiveUser && !StringUtils.isBlank(credential.getLoginUser()))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_LOGINUSER", rUser, systemId);
        throw new IllegalArgumentException(msg);
      }

      // ---------------- Verify credentials if not skipped
      if (!skipCredCheck) verifyCredentials(rUser, putSystem, credential, credential.getLoginUser());
    }

    // ------------------- Store credentials -----------------------------------
    // Store credentials in Security Kernel if cred provided and effectiveUser is static
    if (manageCredentials)
    {
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      // Create credential
      // Note that we only manageCredentials for the static case and for the static case targetUser=effectiveUserId
      createCredential(rUser, credential, systemId, effectiveUserId, isStaticEffectiveUser);
    }

    // Get a complete and succinct description of the update.
    // If nothing has changed, then log a warning and return
    String changeDescription = LibUtils.getChangeDescriptionSystemUpdate(origTSystem, updatedTSystem, null);
    if (StringUtils.isBlank(changeDescription))
    {
      _log.warn(LibUtils.getMsgAuth("SYSLIB_UPD_NO_CHANGE", rUser, "PUT", systemId));
      return;
    }

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to update the system -----------------------------------
    dao.putSystem(rUser, updatedTSystem, changeDescription, rawData);
  }

  /**
   * Update enabled to true for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public int enableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
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
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - resource not found
   */
  @Override
  public int disableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
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
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - resource not found
   */
  @Override
  public int deleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
  {
    SystemOperation op = SystemOperation.delete;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // System must exist
    TSystem system = dao.getSystem(oboTenant, systemId, true);
    if (system == null)
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

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
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - resource not found
   */
  @Override
  public int undeleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
  {
    SystemOperation op = SystemOperation.undelete;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // System must exist
    if (!dao.checkForSystem(oboTenant, systemId, true))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Get owner, if not found it is an error
    String owner = dao.getSystemOwner(oboTenant, systemId);
    if (StringUtils.isBlank(owner)) {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, systemId, op.name());
      _log.error(msg);
      throw new IllegalStateException(msg);
    }
    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, owner);

    // Add permissions for owner
    String systemsPermSpecALL = getPermSpecAllStr(oboTenant, systemId);
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    // Give owner full access to the system
    getSKClient().grantUserPermission(oboTenant, owner, systemsPermSpecALL);
    // Consider using a notification instead (jira cic-3071)
    // Give owner files service related permission for root directory
    getSKClient().grantUserPermission(oboTenant, owner, filesPermSpec);

    // Update deleted attribute
    return updateDeleted(rUser, systemId, op);
  }

  /**
   * Change owner of a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param newOwnerName - User name of new owner
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public int changeSystemOwner(ResourceRequestUser rUser, String systemId, String newOwnerName)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
  {
    SystemOperation op = SystemOperation.changeOwner;

    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(newOwnerName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

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
    // Use try/catch to rollback any changes in case of failure.
    // Get SK client now. If we cannot get this rollback not needed.
    // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
    getSKClient();
    String systemsPermSpec = getPermSpecAllStr(oboTenant, systemId);
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    try {
      // ------------------- Make Dao call to update the system owner -----------------------------------
      dao.updateSystemOwner(rUser, systemId, oldOwnerName, newOwnerName);
      // Add permissions for new owner
      getSKClient().grantUserPermission(oboTenant, newOwnerName, systemsPermSpec);
      // Consider using a notification instead (jira cic-3071)
      // Give owner files service related permission for root directory
      getSKClient().grantUserPermission(oboTenant, newOwnerName, filesPermSpec);
      // Remove permissions from old owner
      getSKClient().revokeUserPermission(oboTenant, oldOwnerName, systemsPermSpec);
      // Consider using a notification instead (jira cic-3071)

      // Get a complete and succinct description of the update.
      String changeDescription = LibUtils.getChangeDescriptionUpdateOwner(systemId, oldOwnerName, newOwnerName);
      // Create a record of the update
      dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      try { dao.updateSystemOwner(rUser, systemId, newOwnerName, oldOwnerName); } catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "updateOwner", e.getMessage()));}
      // Consider using a notification instead(jira cic-3071)
      try { getSKClient().revokeUserPermission(oboTenant, newOwnerName, filesPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermNewOwner", e.getMessage()));}
      try { getSKClient().revokeUserPermission(oboTenant, newOwnerName, filesPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));}
      try { getSKClient().grantUserPermission(oboTenant, oldOwnerName, systemsPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPermOldOwner", e.getMessage()));}
      try { getSKClient().grantUserPermission(oboTenant, oldOwnerName, filesPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPermF1", e.getMessage()));}
      throw e0;
    }
    return 1;
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
   * @throws NotAuthorizedException - unauthorized
   */
  int hardDeleteSystem(ResourceRequestUser rUser, String oboTenant, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.hardDelete;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(oboTenant) ||  StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

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
   * @throws NotAuthorizedException - unauthorized
   */
  int hardDeleteAllTestTenantResources(ResourceRequestUser rUser)
          throws TapisException, TapisClientException, NotAuthorizedException
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public boolean checkForSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    return checkForSystem(rUser, systemId, false);
  }

  /**
   * checkForSystem
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return true if system exists and has not been deleted, false otherwise
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public boolean checkForSystem(ResourceRequestUser rUser, String systemId, boolean includeDeleted)
          throws TapisException, NotAuthorizedException, TapisClientException
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
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public boolean isEnabled(ResourceRequestUser rUser, String systemId)
          throws TapisException, NotFoundException, NotAuthorizedException, TapisClientException
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
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @param accMethod - (optional) return credentials for specified authn method instead of default authn method
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @param getCreds - flag indicating if credentials for effectiveUserId should be included
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, resolving effectiveUserId
   * @param resolveEffUser - If effectiveUserId is set to ${apiUserId} then resolve it, else always return value
   *                         provided in system definition.
   * @param sharedAppCtx - Indicates that request is part of a shared app context. Tapis auth will be skipped.
   * @return populated instance of a TSystem or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public TSystem getSystem(ResourceRequestUser rUser, String systemId, AuthnMethod accMethod, boolean requireExecPerm,
                           boolean getCreds, String impersonationId, boolean resolveEffUser, boolean sharedAppCtx)
          throws TapisException, NotAuthorizedException, TapisClientException
  {

    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(oboTenant, systemId);
    // We need owner to check auth and if system not there cannot find owner, so return null if no system.
    if (system == null) return null;

    // Determine the effectiveUser type, either static or dynamic
    // Secrets get stored on different paths based on this
    boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

    // ------------------------- Check authorization -------------------------
    // If impersonationId supplied confirm that it is allowed
    if (!StringUtils.isBlank(impersonationId)) checkImpersonationAllowed(rUser, op, systemId, impersonationId);

    // If sharedAppCtx set confirm that it is allowed
    if (sharedAppCtx) checkSharedAppCtxAllowed(rUser, op, systemId);

    // getSystem auth check:
    //   - always allow a service calling as itself to read/execute a system.
    //   - if svc not calling as itself do the normal checks using oboUserOrImpersonationId.
    //   - as always make sure auth checks are skipped if svc passes in sharedAppCtx=true.
    // If not skipping auth then check auth
    if (!sharedAppCtx) checkAuth(rUser, op, systemId, nullOwner, nullTargetUser, nullPermSet, impersonationId);

    // If flag is set to also require EXECUTE perm then make explicit auth call to make sure user has exec perm
    if (!sharedAppCtx && requireExecPerm)
    {
      checkAuth(rUser, SystemOperation.execute, systemId, nullOwner, nullTargetUser, nullPermSet, impersonationId);
    }

    // If flag is set to also require EXECUTE perm then system must support execute
    if (requireExecPerm && !system.getCanExec())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_NOTEXEC", rUser, systemId, op.name());
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }

    // Resolve and optionally set effectiveUserId in result
    String resolvedEffectiveUserId = resolveEffectiveUserId(rUser, system, impersonationId);
    if (resolveEffUser) system.setEffectiveUserId(resolvedEffectiveUserId);

    // If requested retrieve credentials from Security Kernel
    if (getCreds)
    {
      AuthnMethod tmpAccMethod = system.getDefaultAuthnMethod();
      // If authnMethod specified then use it instead of default authn method defined for the system.
      if (accMethod != null) tmpAccMethod = accMethod;
      // Determine targetUser for fetching credential.
      //   If static use effectiveUserId, else use oboOrImpersonatedUser
      String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
      String credTargetUser;
      if (isStaticEffectiveUser)
        credTargetUser = system.getEffectiveUserId();
      else
        credTargetUser = oboOrImpersonatedUser;
      Credential cred = getUserCredential(rUser, systemId, credTargetUser, tmpAccMethod);
      system.setAuthnCredential(cred);
    }
    return system;
  }

  /**
   * Get count of all systems matching certain criteria and for which user has READ permission
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param startAfter - where to start when sorting, e.g. orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param showDeleted - whether to included resources that have been marked as deleted.
   * @return Count of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public int getSystemsTotalCount(ResourceRequestUser rUser, List<String> searchList,
                                  List<OrderBy> orderByList, String startAfter, boolean showDeleted)
          throws TapisException, TapisClientException
  {
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

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
        _log.error(msg, e);
        throw new IllegalArgumentException(msg);
      }
    }

    // Get list of IDs of systems for which requester has view permission.
    // This is either all systems (null) or a list of IDs.
    Set<String> allowedSysIDs = getAllowedSysIDs(rUser);

    // If none are allowed we know count is 0
    if (allowedSysIDs != null && allowedSysIDs.isEmpty()) return 0;

    // Count all allowed systems matching the search conditions
    return dao.getSystemsCount(rUser.getOboTenantId(), verifiedSearchList, null, allowedSysIDs, orderByList, startAfter,
                               showDeleted);
  }

  /**
   * Get all systems matching certain criteria and for which user has READ permission
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param limit - indicates maximum number of results to be included, -1 for unlimited
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param skip - number of results to skip (may not be used with startAfter)
   * @param startAfter - where to start when sorting, e.g. limit=10&orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param resolveEffUser - If effectiveUserId is set to ${apiUserId} then resolve it, else always return value
   *                         provided in system definition.
   * @param showDeleted - whether to included resources that have been marked as deleted.
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList,
                                  int limit, List<OrderBy> orderByList, int skip, String startAfter,
                                  boolean resolveEffUser, boolean showDeleted)
          throws TapisException, TapisClientException
  {
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

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
        _log.error(msg, e);
        throw new IllegalArgumentException(msg);
      }
    }

    // Get list of IDs of systems for which requester has READ permission.
    // This is either all systems (null) or a list of IDs.
    Set<String> allowedSysIDs = getAllowedSysIDs(rUser);

    // Get all allowed systems matching the search conditions
    List<TSystem> systems = dao.getSystems(rUser.getOboTenantId(), verifiedSearchList, null, allowedSysIDs,
                                            limit, orderByList, skip, startAfter, showDeleted);

    if (resolveEffUser)
    {
      for (TSystem system : systems)
      {
        system.setEffectiveUserId(resolveEffectiveUserId(rUser, system));
      }
    }
    return systems;
  }

  /**
   * Get all systems for which user has READ permission.
   * Use provided string containing a valid SQL where clause for the search.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sqlSearchStr - string containing a valid SQL where clause
   * @param limit - indicates maximum number of results to be included, -1 for unlimited
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param skip - number of results to skip (may not be used with startAfter)
   * @param startAfter - where to start when sorting, e.g. limit=10&orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param resolveEffUser - If effectiveUserId is set to ${apiUserId} then resolve it, else always return value
   *                         provided in system definition.
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser,
                                                   String sqlSearchStr, int limit, List<OrderBy> orderByList, int skip,
                                                   String startAfter, boolean resolveEffUser, boolean showDeleted)
          throws TapisException, TapisClientException
  {
    // If search string is empty delegate to getSystems()
    if (StringUtils.isBlank(sqlSearchStr)) return getSystems(rUser, null, limit, orderByList, skip,
                                                             startAfter, resolveEffUser, showDeleted);

    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

    // Validate and parse the sql string into an abstract syntax tree (AST)
    // NOTE: The activemq parser validates and parses the string into an AST but there does not appear to be a way
    //          to use the resulting BooleanExpression to walk the tree. How to now create a usable AST?
    //   I believe we don't want to simply try to run the where clause for various reasons:
    //      - SQL injection
    //      - we want to verify the validity of each <attr>.<op>.<value>
    //        looks like activemq parser will ensure the leaf nodes all represent <attr>.<op>.<value> and in principle
    //        we should be able to check each one and generate of list of errors for reporting.
    //  Looks like jOOQ can parse an SQL string into a jooq Condition. Do this in the Dao? But still seems like no way
    //    to walk the AST and check each condition so we can report on errors.
    ASTNode searchAST;
    try { searchAST = ASTParser.parse(sqlSearchStr); }
    catch (Exception e)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SEARCH_ERROR", rUser, e.getMessage());
      _log.error(msg, e);
      throw new IllegalArgumentException(msg);
    }

    // Get list of IDs of systems for which requester has READ permission.
    // This is either all systems (null) or a list of IDs.
    Set<String> allowedSysIDs = getAllowedSysIDs(rUser);

    // Get all allowed systems matching the search conditions
    List<TSystem> systems = dao.getSystems(rUser.getOboTenantId(), null, searchAST, allowedSysIDs,
                                           limit, orderByList, skip, startAfter, showDeleted);

    if (resolveEffUser)
    {
      for (TSystem system : systems)
      {
        system.setEffectiveUserId(resolveEffectiveUserId(rUser, system));
      }
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
  public List<TSystem> getSystemsSatisfyingConstraints(ResourceRequestUser rUser,
                                                       String matchStr)
          throws TapisException, TapisClientException
  {
    if (rUser == null)  throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));

    // Get list of IDs of systems for which requester has READ permission.
    // This is either all systems (null) or a list of IDs.
    Set<String> allowedSysIDs = getAllowedSysIDs(rUser);

    // Validate and parse the sql string into an abstract syntax tree (AST)
    ASTNode matchAST;
    try { matchAST = ASTParser.parse(matchStr); }
    catch (Exception e)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_MATCH_ERROR", rUser, e.getMessage());
      _log.error(msg, e);
      throw new IllegalArgumentException(msg);
    }

    // Get all allowed systems matching the constraint conditions
    List<TSystem> systems = dao.getSystemsSatisfyingConstraints(rUser.getOboTenantId(), matchAST, allowedSysIDs);

    for (TSystem system : systems)
    {
      system.setEffectiveUserId(resolveEffectiveUserId(rUser, system));
    }
    return systems;
  }

  /**
   * Get system owner
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @return - Owner or null if system not found or user not authorized
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public String getSystemOwner(ResourceRequestUser rUser,
                               String systemId) throws TapisException, NotAuthorizedException, TapisClientException
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void grantUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                                   Set<Permission> permissions, String rawData)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.grantPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // If system does not exist or has been deleted then throw an exception
    if (!dao.checkForSystem(oboTenant, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Check to see if owner is trying to update permissions for themselves.
    // If so throw an exception because this would be confusing since owner always has full permissions.
    // For an owner permissions are never checked directly.
    String owner = checkForOwnerPermUpdate(rUser, systemId, targetUser, op.name());

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerKnown(rUser, op, systemId, owner);

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
        getSKClient().grantUserPermission(oboTenant, targetUser, permSpec);
      }
    }
    catch (TapisClientException tce)
    {
      // Rollback
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ERROR_ROLLBACK", rUser, systemId, tce.getMessage());
      _log.error(msg);

      // Revoke permissions that may have been granted.
      for (String permSpec : permSpecSet)
      {
        try { getSKClient().revokeUserPermission(oboTenant, targetUser, permSpec); }
        catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePerm", e.getMessage()));}
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int revokeUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                                   Set<Permission> permissions, String rawData)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.revokePerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // We need owner to check auth and if system not there cannot find owner, so
    // if system does not exist or has been deleted then return 0 changes
    if (!dao.checkForSystem(oboTenant, systemId, false)) return 0;

    // Check to see if owner is trying to update permissions for themselves.
    // If so throw an exception because this would be confusing since owner always has full permissions.
    // For an owner permissions are never checked directly.
    String owner = checkForOwnerPermUpdate(rUser, systemId, targetUser, op.name());

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, owner, targetUser, permissions);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Revoke of READ implies revoke of MODIFY
    if (permissions.contains(Permission.READ)) permissions.add(Permission.MODIFY);

    int changeCount;
    // Determine current set of user permissions
    var userPermSet = getUserPermSet(targetUser, oboTenant, systemId);

    try
    {
      // Revoke perms
      changeCount = revokePermissions(oboTenant, systemId, targetUser, permissions);
    }
    catch (TapisClientException tce)
    {
      // Rollback
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ERROR_ROLLBACK", rUser, systemId, tce.getMessage());
      _log.error(msg);

      // Grant permissions that may have been revoked and that the user previously held.
      for (Permission perm : permissions)
      {
        if (userPermSet.contains(perm))
        {
          String permSpec = getPermSpecStr(oboTenant, systemId, perm);
          try { getSKClient().grantUserPermission(oboTenant, targetUser, permSpec); }
          catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPerm", e.getMessage()));}
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public Set<Permission> getUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.getPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // If system does not exist or has been deleted then return null
    if (!dao.checkForSystem(rUser.getOboTenantId(), systemId, false)) return null;

    // ------------------------- Check authorization -------------------------
    checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use Security Kernel client to check for each permission in the enum list
    return getUserPermSet(targetUser, rUser.getOboTenantId(), systemId);
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
   * @param credential - list of permissions to be granted
   * @param skipCredCheck - Indicates if cred check for LINUX systems should happen
   * @param rawData - Client provided text used to create the credential - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void createUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, Credential credential,
                                   boolean skipCredCheck, String rawData)
          throws TapisException, NotFoundException, NotAuthorizedException, IllegalStateException, TapisClientException
  {
    SystemOperation op = SystemOperation.setCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser) || credential == null)
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // Extract some attributes for convenience and clarity
    String oboTenant = rUser.getOboTenantId();
    String loginUser = credential.getLoginUser();

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
    if (!StringUtils.isBlank(credential.getPrivateKey()) && !credential.isValidPrivateSshKey())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2", rUser, systemId, targetUser);
      throw new IllegalArgumentException(msg);
    }

    // ---------------- Verify credentials ------------------------
    if (!skipCredCheck) { verifyCredentials(rUser, system, credential, loginUser); }

    // Create credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    try
    {
      createCredential(rUser, credential, systemId, targetUser, isStaticEffectiveUser);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      _log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // If dynamic and an alternate loginUser has been provided that is not the same as the Tapis user
    //   then record the mapping
    if (!isStaticEffectiveUser && !StringUtils.isBlank(loginUser) && !targetUser.equals(loginUser))
    {
      dao.createOrUpdateLoginUserMapping(rUser.getOboTenantId(), systemId, targetUser, loginUser);
    }

    // Construct Json string representing the update, with actual secrets masked out
    Credential maskedCredential = Credential.createMaskedCredential(credential);
    // Get a complete and succinct description of the update.
    String changeDescription = LibUtils.getChangeDescriptionCredCreate(systemId, targetUser, skipCredCheck, maskedCredential);
    // Create a record of the update
    dao.addUpdateRecord(rUser, systemId, op, changeDescription, rawData);
  }

  /**
   * Delete credential for given system and user
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int deleteUserCredential(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.removeCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    int changeCount = 0;
    TSystem system = dao.getSystem(oboTenant, systemId);
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
      _log.error(tce.toString());
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
   * Get credential for given system, target user and authn method
   * Only certain services are authorized.
   *
   * If the *effectiveUserId* for the system is dynamic (i.e. equal to *${apiUserId}*) then *targetUser* is
   * interpreted as a Tapis user. Note that their may me a mapping of the Tapis user to a host *loginUser*.
   *
   * If the *effectiveUserId* for the system is static (i.e. not *${apiUserId}*) then *targetUser* is interpreted
   * as the host *loginUser* that is used when accessing the host.
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
   * @return Credential - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public Credential getUserCredential(ResourceRequestUser rUser, String systemId, String targetUser,
                                      AuthnMethod authnMethod)
          throws TapisException, TapisClientException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.getCred;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUser))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

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
     *   key_type is sshkey, password, accesskey or cert
     *   and S1 is the reserved SecretName associated with the Systems.
     *
     * Hence the following code
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
      sParms.setTenant(rUser.getOboTenantId()).setSysId(systemId).setSysUser(targetUserPath);

      // NOTE: Next line is needed for the SK call. Not clear if it should be targetUser, serviceUserId, oboUser.
      //       If not set then the first getAuthnCred in SystemsServiceTest.testUserCredentials
      //          fails. But it appears the value does not matter. Even an invalid userId appears to be OK.
      sParms.setUser(rUser.getOboUserId());
      // Set key type based on authn method
      if (authnMethod.equals(AuthnMethod.PASSWORD))sParms.setKeyType(KeyType.password);
      else if (authnMethod.equals(AuthnMethod.PKI_KEYS))sParms.setKeyType(KeyType.sshkey);
      else if (authnMethod.equals(AuthnMethod.ACCESS_KEY))sParms.setKeyType(KeyType.accesskey);
      else if (authnMethod.equals(AuthnMethod.CERT))sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      SkSecret skSecret = getSKClient().readSecret(sParms);
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
        String mappedLoginUser = dao.getLoginUser(rUser.getOboTenantId(), systemId, targetUser);
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
                                 null); //dataMap.get(CERT) TODO: get ssh certificate when supported
    }
    catch (TapisClientException tce)
    {
      // If tapis client exception then log error but continue so null is returned.
      _log.warn(tce.toString());
    }
    return credential;
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (schedulerProfile == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    // Construct Json string representing the resource about to be created
    String createJsonStr = TapisGsonUtils.getGson().toJson(schedulerProfile);
    _log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, createJsonStr));
    String oboTenant = schedulerProfile.getTenant();
    String schedProfileName = schedulerProfile.getName();

    // ---------------------------- Check inputs ------------------------------------
    // Required attributes: tenant, id, moduleLoadCommand
    if (StringUtils.isBlank(oboTenant) || StringUtils.isBlank(schedProfileName) ||
            StringUtils.isBlank(schedulerProfile.getModuleLoadCommand()))
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public SchedulerProfile getSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, NotAuthorizedException
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int deleteSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalArgumentException
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
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public boolean checkForSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));

    return dao.checkForSchedulerProfile(rUser.getOboTenantId(), name);
  }

  /**
   * Get System Updates records for the System ID specified
   * @throws TapisException
   * @throws IllegalStateException
   * @throws TapisClientException
   * @throws NotAuthorizedException
   */
  @Override
  public List<SystemHistoryItem> getSystemHistory(ResourceRequestUser rUser, String systemId)
          throws TapisException, NotAuthorizedException, IllegalStateException, TapisClientException {

    SystemOperation op = SystemOperation.read;

    // ------------------------- Check authorization -------------------------
    checkAuthOwnerUnkown(rUser, op, systemId);

    // ----------------- Retrieve system updates information (system history) --------------------
    List<SystemHistoryItem> systemHistory = dao.getSystemHistory(rUser.getOboTenantId(), systemId);

    return systemHistory;
  }
  
  /**
   * Get System share user IDs for the System ID specified
   * @throws TapisException
   * @throws IllegalStateException
   * @throws TapisClientException
   * @throws NotAuthorizedException
   */
  @Override
  public SystemShare getSystemShare(ResourceRequestUser rUser, String systemId)
      throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(oboTenant, systemId);
    // We need owner to check auth and if system not there cannot find owner, so return null if no system.
    if (system == null) return null;

    checkAuth(rUser, op, systemId, system.getOwner(), nullTargetUser, nullPermSet);

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(system.getTenant());
    skParms.setResourceId1(systemId);

    var userSet = new HashSet<String>();
    
    // First determine if system is publicly shared. Search for share to grantee ~public
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    var skShares = getSKClient().getShares(skParms);
    // Set isPublic based on result.
    boolean isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
    // Now get all the users with whom the system has been shared
    skParms.setGrantee(null);
    skParms.setIncludePublicGrantees(false);
    skShares = getSKClient().getShares(skParms);
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
  
  /**
   * Create or update share of a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param systemShare - User names
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException 
   * @throws TapisClientException 
   * @throws NotAuthorizedException 
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public void shareSystem(ResourceRequestUser rUser, String systemId, SystemShare systemShare)
      throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException {
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
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void unshareSystem(ResourceRequestUser rUser, String systemId, SystemShare systemShare)
      throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException {
    updateUserShares(rUser, OP_UNSHARE, systemId, systemShare, false);
  }
  
  
  
  /**
   * Share a system publicly
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis client related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void shareSystemPublicly(ResourceRequestUser rUser, String systemId) 
      throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException {
    updateUserShares(rUser, OP_SHARE, systemId, nullSystemShare, true);
  }
  
  /**
   * Unshare a system publicly
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis client related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void unshareSystemPublicly(ResourceRequestUser rUser, String systemId) 
       throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException {
    updateUserShares(rUser, OP_UNSHARE, systemId, nullSystemShare, true);
  }
  

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /**
   * Update enabled attribute for a system
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param sysOp - operation, enable or disable
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - resource not found
   */
  private int updateEnabled(ResourceRequestUser rUser, String systemId, SystemOperation sysOp)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
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
   * @throws IllegalStateException - Resulting resource would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - resource not found
   */
  private int updateDeleted(ResourceRequestUser rUser, String systemId, SystemOperation sysOp)
          throws TapisException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException, TapisClientException
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
  private SKClient getSKClient() throws TapisException
  {
    String oboUser = getServiceUserId();
    String oboTenant = getServiceTenantId();
    
    try { return serviceClients.getClient(oboUser, oboTenant, SKClient.class); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, oboTenant, oboUser);
      throw new TapisException(msg, e);
    }
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
   * If DTN is used verify that dtnSystemId exists with isDtn = true
   * Collect and report as many errors as possible, so they can all be fixed before next attempt
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private void validateTSystem(ResourceRequestUser rUser, TSystem tSystem1) throws TapisException, IllegalStateException
  {
    String msg;
    // Make api level checks, i.e. checks that do not involve a dao or service call.
    List<String> errMessages = tSystem1.checkAttributeRestrictions();

    // Now make checks that do require a dao or service call.

    // If batchSchedulerProfile is set verify that the profile exists.
    if (!StringUtils.isBlank(tSystem1.getBatchSchedulerProfile()))
    {
      if (!dao.checkForSchedulerProfile(tSystem1.getTenant(), tSystem1.getBatchSchedulerProfile()))
      {
        msg = LibUtils.getMsg("SYSLIB_PRF_NO_PROFILE", tSystem1.getBatchSchedulerProfile());
        errMessages.add(msg);
      }
    }

    // If DTN is used (i.e. dtnSystemId is set) verify that dtnSystemId exists with isDtn = true
    if (!StringUtils.isBlank(tSystem1.getDtnSystemId()))
    {
      TSystem dtnSystem = null;
      try
      {
        dtnSystem = dao.getSystem(tSystem1.getTenant(), tSystem1.getDtnSystemId());
      }
      catch (TapisException e)
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_CHECK_ERROR", tSystem1.getDtnSystemId(), e.getMessage());
        _log.error(msg, e);
        errMessages.add(msg);
      }
      if (dtnSystem == null)
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_NO_SYSTEM", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
      else if (!dtnSystem.isDtn())
      {
        msg = LibUtils.getMsg("SYSLIB_DTN_NOT_DTN", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
    }

    // If validation failed throw an exception
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(rUser, tSystem1.getId(), errMessages);
      _log.error(allErrors);
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
      _log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }

  /**
   * Verify that effectiveUserId can connect to the system using provided credentials.
   * If loginUser is set then use it for connection,
   * else if effectiveUserId is ${apUserId} then use rUser.oboUser for connection
   * else use static effectiveUserId from TSystem for connection
   *
   * Skipped for non-LINUX systems
   * Skipped if no credentials provided, i.e. no password or ssh keys
   * Both types (password and ssh keys) are checked if they are provided
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param credential - credentials to check
   * @throws IllegalStateException - if credentials not verified
   */
  private void verifyCredentials(ResourceRequestUser rUser, TSystem tSystem1, Credential credential, String loginUser)
          throws TapisException, IllegalStateException
  {
    // We must have the system and a set of credentials to check.
    if (tSystem1 == null || credential == null) return;
    // Check is only done for LINUX systems
    if (!tSystem1.getSystemType().equals(TSystem.SystemType.LINUX)) return;

    // Determine user to check
    // None of the public methods that call this support impersonation so use null for impersonationId
    String effectiveUser = resolveEffectiveUserId(rUser, tSystem1);
    if (!StringUtils.isBlank(loginUser)) effectiveUser = loginUser;

    // Determine authnMethod to check, either password or ssh keys
    // if neither provided then skip check
    // if both provided check both
    boolean passwordSet = !StringUtils.isBlank(credential.getPassword());
    boolean sshKeysSet = (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()));
    if (!passwordSet && !sshKeysSet) return;

    String host = tSystem1.getHost();
    int port = tSystem1.getPort();
    _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_START", rUser, tSystem1.getId(), effectiveUser, host, port));

    // Attempt to connect to the system using each type of credential provided
    // Log error for each one that fails
    String msg = null;
    SSHConnection sshConnection = null;

    // If password set then try it
    if (passwordSet)
    {
      try
      {
        _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY", rUser, tSystem1.getId(), effectiveUser, host, port, AuthnMethod.PASSWORD.name()));
        sshConnection = new SSHConnection(host, port, effectiveUser, credential.getPassword());
      }
      catch(TapisException e)
      {
        msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_FAIL", rUser, tSystem1.getId(), host, effectiveUser, AuthnMethod.PASSWORD.name(),
                                  e.getMessage());
        _log.error(msg, e);
      }
      finally { if (sshConnection != null) sshConnection.close(); }
    }

    // If ssh keys set then try it
    if (sshKeysSet)
    {
      try
      {
        _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY", rUser, tSystem1.getId(), effectiveUser, host, port, AuthnMethod.PKI_KEYS.name()));
        sshConnection = new SSHConnection(host, port, effectiveUser, credential.getPublicKey(), credential.getPrivateKey());
      }
      catch(TapisException e)
      {
        msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_FAIL", rUser, tSystem1.getId(), host, effectiveUser, AuthnMethod.PKI_KEYS.name(),
                                  e.getMessage());
        _log.error(msg, e);
      }
      finally { if (sshConnection != null) sshConnection.close(); }
    }

    _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_END", rUser, tSystem1.getId(), effectiveUser, host, port));

    // If there was an error then throw IllegalStateException
    if (msg != null)
    {
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Determine the user to be used to access the system.
   * Determine effectiveUserId for static and dynamic (i.e. ${apiUserId}) cases.
   * If effectiveUserId is dynamic then resolve it
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - the system in question
   * @param impersonationId - use provided Tapis username instead of oboUser when resolving effectiveUserId
   * @return Resolved value for effective user.
   */
  private String resolveEffectiveUserId(ResourceRequestUser rUser, TSystem system, String impersonationId)
          throws TapisException
  {
    String systemId = system.getId();
    String effUser = system.getEffectiveUserId();
    // Incoming effectiveUserId should never be blank but for robustness handle that case.
    if (StringUtils.isBlank(effUser)) return effUser;

    // If a static string (i.e. not ${apiUserId} then simply return the string
    if (!effUser.equals(APIUSERID_VAR)) return effUser;

    // At this point we have a dynamic effectiveUserId. Figure it out.
    // Determine the loginUser associated with the credential
    // First determine whether to use oboUser or impersonationId
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    // Now see if there is a mapping from that that Tapis user to a different login user on the host
    String loginUser = dao.getLoginUser(rUser.getOboTenantId(), systemId, oboOrImpersonatedUser);

    // If a mapping then return it, else return oboUser or impersonationId
    return (!StringUtils.isBlank(loginUser)) ? loginUser : oboOrImpersonatedUser;
  }

  /**
   * Overloaded method for callers that do not support impersonation
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - the system in question
   * @return Resolved value for effective user.
   * @throws TapisException on error
   */
  private String resolveEffectiveUserId(ResourceRequestUser rUser, TSystem system) throws TapisException
  {
    return resolveEffectiveUserId(rUser, system, nullImpersonationId);
  }

  /**
   * Retrieve set of user permissions given sk client, user, tenant, id
   * @param userName - name of user
   * @param oboTenant - name of tenant associated with resource
   * @param systemId - Id of resource
   * @return - Set of Permissions for the user
   */
  private Set<Permission> getUserPermSet(String userName, String oboTenant, String systemId)
          throws TapisClientException, TapisException
  {
    var userPerms = new HashSet<Permission>();
    for (Permission perm : Permission.values())
    {
      String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, perm.name(), systemId);
      if (getSKClient().isPermitted(oboTenant, userName, permSpec)) userPerms.add(perm);
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
   * Check to see if owner is trying to update permissions for themselves.
   * If so throw an exception because this would be confusing since owner always has full permissions.
   * For an owner permissions are never checked directly.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId System id
   * @param targetOboUser user for whom perms are being updated
   * @param opStr Operation in progress, for logging
   * @return name of owner
   */
  private String checkForOwnerPermUpdate(ResourceRequestUser rUser, String systemId, String targetOboUser, String opStr)
          throws TapisException, NotAuthorizedException
  {
    // Look up owner. If not found then consider not authorized. Very unlikely at this point.
    String owner = dao.getSystemOwner(rUser.getOboTenantId(), systemId);
    if (StringUtils.isBlank(owner))
        throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, opStr), NO_CHALLENGE);
    // If owner making the request and owner is the target user for the perm update then reject.
    if (owner.equals(rUser.getOboUserId()) && owner.equals(targetOboUser))
    {
      // If it is a svc making request reject with no auth, if user making request reject with special message.
      // Need this check since svc not allowed to update perms but checkAuth happens after checkForOwnerPermUpdate.
      // Without this the op would be denied with a misleading message.
      // Unfortunately this means auth check for svc in 2 places but not clear how to avoid it.
      //   On the bright side it means at worst operation will be denied when maybe it should be allowed which is better
      //   than the other way around.
      if (rUser.isServiceRequest()) throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, opStr), NO_CHALLENGE);
      else throw new TapisException(LibUtils.getMsgAuth("SYSLIB_PERM_OWNER_UPDATE", rUser, systemId, opStr));
    }
    return owner;
  }

  /**
   * Determine all systems that a user is allowed to see.
   * If all systems return null else return list of system IDs
   * An empty list indicates no systems allowed.
   */
  private Set<String> getAllowedSysIDs(ResourceRequestUser rUser) throws TapisException, TapisClientException
  {
    // If requester is a service calling as itself or an admin then all systems allowed
    if ((rUser.isServiceRequest() && rUser.getJwtUserId().equals(rUser.getOboUserId())) || hasAdminRole(rUser))
    {
      return null;
    }
    var sysIDs = new HashSet<String>();
    var userPerms = getSKClient().getUserPerms(rUser.getOboTenantId(), rUser.getOboUserId());
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
        sysIDs.add(permFields[3]);
      }
    }
    return sysIDs;
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
    return getSKClient().isPermitted(tenantName, userName, permSpecStr);
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
    return getSKClient().isPermittedAny(tenantName, userName, permSpecs.toArray(TSystem.EMPTY_STR_ARRAY));
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
   *   key_type is sshkey, password, accesskey or cert
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
      getSKClient().writeSecret(oboTenant, oboUser, sParms);
    }
    // Store PKI keys if both present
    if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.sshkey);
      dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
      dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
      sParms.setData(dataMap);
      getSKClient().writeSecret(oboTenant, oboUser, sParms);
    }
    // Store Access key and secret if both present
    if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.accesskey);
      dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
      dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
      sParms.setData(dataMap);
      getSKClient().writeSecret(oboTenant, oboUser, sParms);
    }
    // TODO if necessary handle ssh certificate when supported
  }

  /**
   * Delete a credential
   * No checks are done for incoming arguments and the system must exist
   */
  private int deleteCredential(ResourceRequestUser rUser, String systemId,
                               String targetUser, boolean isStatic)
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
    try { getSKClient().readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { getSKClient().readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { getSKClient().readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    if (secretNotFound) return 0;

    // Construct basic SK secret parameters and attempt to destroy each type of secret.
    // If destroy attempt throws an exception then log a message and continue.
    sMetaParms.setKeyType(KeyType.password);
    try { getSKClient().destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { getSKClient().destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { getSKClient().destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    return 1;
  }

  /**
   * Remove all SK artifacts associated with a System: user credentials, user permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private void removeSKArtifacts(ResourceRequestUser rUser, TSystem system)
          throws TapisException, TapisClientException
  {
    String systemId = system.getId();
    String oboTenant = system.getTenant();
    String effectiveUserId = system.getEffectiveUserId();

    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, "%", systemId);
    var userNames = getSKClient().getUsersWithPermission(oboTenant, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames) {
      revokePermissions(oboTenant, systemId, userName, ALL_PERMS);
      // Remove wildcard perm
      getSKClient().revokeUserPermission(oboTenant, userName, getPermSpecAllStr(oboTenant, systemId));
    }

    // Resolve effectiveUserId if necessary. This becomes the target user for perm and cred
    String resolvedEffectiveUserId = resolveEffectiveUserId(rUser, system);

    // Consider using a notification instead(jira cic-3071)
    // Remove files perm for owner and possibly effectiveUser
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    getSKClient().revokeUserPermission(oboTenant, system.getOwner(), filesPermSpec);
    if (!effectiveUserId.equals(APIUSERID_VAR))
      getSKClient().revokeUserPermission(oboTenant, resolvedEffectiveUserId, filesPermSpec);;

    // Remove credentials associated with the system.
    // TODO: Have SK do this in one operation?
    // TODO: How to remove for users other than effectiveUserId?
    // Remove credentials in Security Kernel if effectiveUser is static
    if (!effectiveUserId.equals(APIUSERID_VAR)) {
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      deleteCredential(rUser, system.getId(), resolvedEffectiveUserId, true);
    }
  }

  /**
   * Revoke permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private int revokePermissions(String oboTenant, String systemId, String userName,
                                       Set<Permission> permissions)
          throws TapisClientException, TapisException
  {
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(oboTenant, systemId, permissions);
    // Remove perms from default user role
    for (String permSpec : permSpecSet)
    {
      getSKClient().revokeUserPermission(oboTenant, userName, permSpec);
    }
    return permSpecSet.size();
  }

  /**
   * Create an updated TSystem based on the system created from a PUT request.
   * Attributes that cannot be updated and must be filled in from the original system:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, isDtn, canExec
   */
  private TSystem createUpdatedTSystem(TSystem origSys, TSystem putSys)
  {
    // Rather than exposing otherwise unnecessary setters we use a special constructor.
    TSystem updatedSys = new TSystem(putSys, origSys.getTenant(), origSys.getId(), origSys.getSystemType(),
                                     origSys.isDtn(), origSys.getCanExec());
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
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId, dtnMountPoint, dtnMountSourcePath,
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
    if (p.getDtnMountPoint() != null) p1.setDtnMountPoint(p.getDtnMountPoint());
    if (p.getDtnMountSourcePath() != null) p1.setDtnMountSourcePath(p.getDtnMountSourcePath());
    if (p.getJobRuntimes() != null) p1.setJobRuntimes(p.getJobRuntimes());
    if (p.getJobWorkingDir() != null) p1.setJobWorkingDir(p.getJobWorkingDir());
    if (p.getJobEnvVariables() != null) p1.setJobEnvVariables(p.getJobEnvVariables());
    if (p.getJobMaxJobs() != null) p1.setJobMaxJobs(p.getJobMaxJobs());
    if (p.getJobMaxJobsPerUser() != null) p1.setJobMaxJobsPerUser(p.getJobMaxJobsPerUser());
    if (p.getCanRunBatch() != null) p1.setCanRunBatch(p.getCanRunBatch());
    if (p.getMpiCmd() != null) p1.setMpiCmd(p.getMpiCmd());
    if (p.getBatchScheduler() != null) p1.setBatchScheduler(p.getBatchScheduler());
    if (p.getBatchLogicalQueues() != null) p1.setBatchLogicalQueues(p.getBatchLogicalQueues());
    if (p.getBatchDefaultLogicalQueue() != null) p1.setBatchDefaultLogicalQueue(p.getBatchDefaultLogicalQueue());
    if (p.getBatchSchedulerProfile() != null) p1.setBatchSchedulerProfile(p.getBatchSchedulerProfile());
    if (p.getJobCapabilities() != null) p1.setJobCapabilities(p.getJobCapabilities());
    if (p.getTags() != null) p1.setTags(p.getTags());
    if (p.getNotes() != null) p1.setNotes(p.getNotes());
    if (p.getImportRefId() != null) p1.setImportRefId(p.getImportRefId());
    return p1;
  }

  // ************************************************************************
  // **************************  Auth checking ******************************
  // ************************************************************************

  /*
   * Check for case when owner is not known and no need for impersonationId, targetUser or perms
   */
  private void checkAuthOwnerUnkown(ResourceRequestUser rUser, SystemOperation op, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    checkAuth(rUser, op, systemId, nullOwner, nullTargetUser, nullPermSet, nullImpersonationId);
  }

  /*
   * Check for case when owner is known and no need for impersonationId, targetUser or perms
   */
  private void checkAuthOwnerKnown(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    checkAuth(rUser, op, systemId, owner, nullTargetUser, nullPermSet, nullImpersonationId);
  }


  /**
   * Overloaded method for callers that do not support impersonation
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - app owner
   * @param targetUser - Target user for operation
   * @param perms - List of permissions for the revokePerm case
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                         String targetUser, Set<Permission> perms)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    checkAuth(rUser, op, systemId, owner, targetUser, perms, nullImpersonationId);
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
   * Many callers do not support impersonation, so make impersonationId the final argument and provide an overloaded
   *   method for simplicity.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - app owner
   * @param targetUser - Target user for operation
   * @param perms - List of permissions for the revokePerm case
   * @param impersonationId - for auth check use this user in place of oboUser
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                         String targetUser, Set<Permission> perms, String impersonationId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    // Check service and user requests separately to avoid confusing a service name with a username
    if (rUser.isServiceRequest())
    {
      // NOTE: This call will do a final checkAuthOboUser() if no special cases apply.
      checkAuthSvc(rUser, op, systemId, owner, targetUser, perms, impersonationId);
    }
    else
    {
      // This is an OboUser check
      checkAuthOboUser(rUser, op, systemId, owner, targetUser, perms, impersonationId);
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
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkAuthSvc(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                            String targetUser, Set<Permission> perms, String impersonationId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    // If ever called and not a svc request then fall back to denied
    if (!rUser.isServiceRequest())
      throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()), NO_CHALLENGE);

    // This is a service request. The username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    String svcTenant = rUser.getJwtTenantId();

    // For getCred, only certain services are allowed. Everyone else denied with a special message
    // Do this check first to reduce chance a request will be allowed that should not be allowed.
    if (op == SystemOperation.getCred)
    {
      if (SVCLIST_GETCRED.contains(svcName)) return;
      // Not authorized, throw an exception
      throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser,
              systemId, op.name()), NO_CHALLENGE);
    }

    // Always allow read, execute, getPerms for a service calling as itself.
    if ((op == SystemOperation.read || op == SystemOperation.execute || op == SystemOperation.getPerms) &&
            (svcName.equals(rUser.getOboUserId()) && svcTenant.equals(rUser.getOboTenantId()))) return;

   // No more special cases. Do the standard auth check
   // Some services, such as Jobs, count on Systems to check auth for OboUserOrImpersonationId
   checkAuthOboUser(rUser, op, systemId, owner, targetUser, perms, impersonationId);
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
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkAuthOboUser(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                                String targetUser, Set<Permission> perms, String impersonationId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
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
        throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser, systemId, op.name()), NO_CHALLENGE);
    }

    // Remaining checks require owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSystemOwner(oboTenant, systemId);
    if (StringUtils.isBlank(owner))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_NO_OWNER", rUser, systemId, op.name());
      _log.error(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
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
    	   if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
                    isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, READMODIFY_PERMS) ||
                    isSystemSharedWithUser(rUser, systemId, oboOrImpersonatedUser, Permission.READ))
              return;
            break;

      case getPerms:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
                isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, READMODIFY_PERMS))
          return;
        break;
      case modify:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
                isPermitted(rUser, oboTenant, oboOrImpersonatedUser, systemId, Permission.MODIFY))
          return;
        break;
      case execute:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
                isPermitted(rUser, oboTenant, oboOrImpersonatedUser, systemId, Permission.EXECUTE) ||
                isSystemSharedWithUser(rUser, systemId, oboOrImpersonatedUser, Permission.EXECUTE))
          return;
        break;
      case revokePerms:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
                (oboOrImpersonatedUser.equals(targetUser) && allowUserRevokePerm(rUser, systemId, perms)))
          return;
        break;
      case setCred:
      case removeCred:
        if (owner.equals(oboOrImpersonatedUser) || hasAdminRole(rUser) ||
             (oboOrImpersonatedUser.equals(targetUser) && isPermittedAny(rUser, oboTenant, oboOrImpersonatedUser, systemId, READMODIFY_PERMS)) ||
             (oboOrImpersonatedUser.equals(targetUser) && isSystemSharedWithUser(rUser, systemId, oboOrImpersonatedUser, Permission.READ)))
          return;
        break;
    }
    // Not authorized, throw an exception
    throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()), NO_CHALLENGE);
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
    return getSKClient().hasPrivilege(skParms);
  }
  
  /**
   * Confirm that caller is allowed to impersonate a Tapis user.
   * Must be a service request from a service allowed to impersonate
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkImpersonationAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId, String impersonationId)
          throws NotAuthorizedException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
    {
      throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_IMPERSONATE", rUser, systemId, op.name(), impersonationId), NO_CHALLENGE);
    }
    // An allowed service is impersonating, log it
    _log.info(LibUtils.getMsgAuth("SYSLIB_AUTH_IMPERSONATE", rUser, systemId, op.name(), impersonationId));
  }

  /**
   * Confirm that caller is allowed to set sharedAppCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkSharedAppCtxAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId)
          throws NotAuthorizedException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_SHAREDAPPCTX.contains(svcName))
    {
      throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH_SHAREDAPPCTX", rUser, systemId, op.name()), NO_CHALLENGE);
    }
    // An allowed service is impersonating, log it
    _log.trace(LibUtils.getMsgAuth("SYSLIB_AUTH_SHAREDAPPCTX", rUser, systemId, op.name()));
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
   * @throws NotAuthorizedException - user not authorized to perform operation
   */
  private void checkPrfAuth(ResourceRequestUser rUser, SchedulerProfileOperation op, String name, String owner)
          throws TapisException, TapisClientException, NotAuthorizedException
  {
    // Anyone can read, including all services
    if (op == SchedulerProfileOperation.read) return;

    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();

    // Check requires owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSchedulerProfileOwner(oboTenant, name);
    if (StringUtils.isBlank(owner))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_PRF_AUTH_NO_OWNER", rUser, name, op.name());
      _log.error(msg);
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }

    // Owner and Admin can create, delete
    switch(op) {
      case create:
      case delete:
        if (owner.equals(oboUser) || hasAdminRole(rUser)) return;
        break;
    }
    // Not authorized, throw an exception
    throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_PRF_UNAUTH", rUser, name, op.name()), NO_CHALLENGE);
  }

  /**
   * Check to see if the oboUser has the admin role in the obo tenant
   */
  private boolean hasAdminRole(ResourceRequestUser rUser) throws TapisException, TapisClientException
  {
    return getSKClient().isAdmin(rUser.getOboTenantId(), rUser.getOboUserId());
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
  
  /*
   * Common routine to update share/unshare for a list of users.
   * Can be used to mark a system publicly shared with all users in tenant including "~public" in the set of users.
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

    String oboTenant = rUser.getOboTenantId();

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(oboTenant, systemId);
    // We need owner to check auth and if system not there cannot find owner.
    if (system == null) throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    checkAuth(rUser, op, systemId, system.getOwner(), nullTargetUser, nullPermSet, nullImpersonationId);
    
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
          getSKClient().shareResource(reqShareResource);
          reqShareResource.setPrivilege(Permission.EXECUTE.name());
          getSKClient().shareResource(reqShareResource);
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
          getSKClient().deleteShare(deleteShareParms);
          deleteShareParms.setPrivilege(Permission.EXECUTE.name());
          getSKClient().deleteShare(deleteShareParms);
        }
      }
    }
  }
}
