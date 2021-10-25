package edu.utexas.tacc.tapis.systems.service;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SYSTEMS_SERVICE;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_SECRET;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PASSWORD;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_EFFECTIVEUSERID;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_VAR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.SchedulerProfileOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

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

  private static final Set<Permission> ALL_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY, Permission.EXECUTE));
  private static final Set<Permission> READMODIFY_PERMS = new HashSet<>(Set.of(Permission.READ, Permission.MODIFY));
  // Permspec format for systems is "system:<tenant>:<perm_list>:<system_id>"
  private static final String PERM_SPEC_PREFIX = "system";
  private static final String PERM_SPEC_TEMPLATE = "system:%s:%s:%s";

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String FILES_SERVICE = TapisConstants.SERVICE_NAME_FILES;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;
  private static final Set<String> SVCLIST_GETCRED = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_READ = new HashSet<>(Set.of(FILES_SERVICE, APPS_SERVICE, JOBS_SERVICE));

  // Message keys
  private static final String ERROR_ROLLBACK = "SYSLIB_ERROR_ROLLBACK";
  private static final String NOT_FOUND = "SYSLIB_NOT_FOUND";

  // NotAuthorizedException requires a Challenge, although it serves no purpose here.
  private static final String NO_CHALLENGE = "NoChallenge";

  // Compiled regex for splitting around ":"
  private static final Pattern COLON_SPLIT = Pattern.compile(":");

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
   * Create a new system object given a TSystem and the text used to create the TSystem.
   * Secrets in the text should be masked.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Pre-populated TSystem object (including tenantId and systemId)
   * @param skipCredCheck - Indicates if cred check for LINUX systems should happen
   * @param scrubbedText - Text used to create the TSystem object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void createSystem(ResourceRequestUser rUser, TSystem system, boolean skipCredCheck, String scrubbedText)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (system == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    _log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, scrubbedText));
    String resourceTenantId = system.getTenant();
    String resourceId = system.getId();

    // ---------------------------- Check inputs ------------------------------------
    // Required system attributes: tenant, id, type, host, defaultAuthnMethod
    if (StringUtils.isBlank(resourceTenantId) || StringUtils.isBlank(resourceId) || system.getSystemType() == null ||
        StringUtils.isBlank(system.getHost()) || system.getDefaultAuthnMethod() == null ||
        StringUtils.isBlank(scrubbedText))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, resourceId));
    }

    // Check if system already exists
    if (dao.checkForSystem(resourceTenantId, resourceId, true))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, resourceId));
    }

    // Make sure owner, effectiveUserId, notes and tags are all set
    // Note that this is done before auth so owner can get resolved and used during auth check.
    system.setDefaults();
    String effectiveUserId = system.getEffectiveUserId();

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    system.resolveVariables(rUser.getOboUserId());

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, system.getId(), system.getOwner(), null, null);

    // ---------------- Check for reserved names ------------------------
    checkReservedIds(rUser, system.getId());

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, system);

    // ---------------- Verify credentials ------------------------
    if (!skipCredCheck && system.getAuthnCredential() != null)
    {
      verifyCredentials(rUser, system, system.getAuthnCredential());
    }

    // Construct Json string representing the TSystem (without credentials) about to be created
    TSystem scrubbedSystem = new TSystem(system);
    scrubbedSystem.setAuthnCredential(null);
    String createJsonStr = TapisGsonUtils.getGson().toJson(scrubbedSystem);

    // ----------------- Create all artifacts --------------------
    // Creation of system, perms and creds not in single DB transaction.
    // Use try/catch to rollback any writes in case of failure.
    boolean itemCreated = false;
    String systemsPermSpecALL = getPermSpecAllStr(resourceTenantId, resourceId);
    // TODO remove filesPermSpec related code (jira cic-3071)
    String filesPermSpec = "files:" + resourceTenantId + ":*:" + resourceId;

    // Get SK client now. If we cannot get this rollback not needed.
    var skClient = getSKClient();
    try {
      // ------------------- Make Dao call to persist the system -----------------------------------
      itemCreated = dao.createSystem(rUser, system, createJsonStr, scrubbedText);

      // ------------------- Add permissions -----------------------------
      // Give owner and possibly effectiveUser full access to the system
      skClient.grantUserPermission(resourceTenantId, system.getOwner(), systemsPermSpecALL);
      if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR)) {
        skClient.grantUserPermission(resourceTenantId, effectiveUserId, systemsPermSpecALL);
      }
      // TODO remove filesPermSpec related code (jira cic-3071)
      // Give owner/effectiveUser files service related permission for root directory
      skClient.grantUserPermission(resourceTenantId, system.getOwner(), filesPermSpec);
      if (!effectiveUserId.equals(APIUSERID_VAR) && !effectiveUserId.equals(OWNER_VAR))
        skClient.grantUserPermission(resourceTenantId, effectiveUserId, filesPermSpec);

      // ------------------- Store credentials -----------------------------------
      // Store credentials in Security Kernel if cred provided and effectiveUser is static
      if (system.getAuthnCredential() != null && !effectiveUserId.equals(APIUSERID_VAR)) {
        String accessUser = effectiveUserId;
        // If effectiveUser is owner resolve to static string.
        if (effectiveUserId.equals(OWNER_VAR)) accessUser = system.getOwner();
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Create credential
        createCredential(skClient, rUser, system.getAuthnCredential(), resourceId, accessUser);
      }
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      // Log error
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ROLLBACK", rUser, resourceId, e0.getMessage());
      _log.error(msg);

      // Rollback
      // Remove system from DB
      if (itemCreated) try {dao.hardDeleteSystem(resourceTenantId, resourceId); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "hardDelete", e.getMessage()));}
      // Remove perms
      try { skClient.revokeUserPermission(resourceTenantId, system.getOwner(), systemsPermSpecALL); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "revokePermOwner", e.getMessage()));}
      try { skClient.revokeUserPermission(resourceTenantId, effectiveUserId, systemsPermSpecALL); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "revokePermEffUsr", e.getMessage()));}
      // TODO remove filesPermSpec related code (jira cic-3071)
      try { skClient.revokeUserPermission(resourceTenantId, system.getOwner(), filesPermSpec);  }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "revokePermF1", e.getMessage()));}
      try { skClient.revokeUserPermission(resourceTenantId, effectiveUserId, filesPermSpec);  }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "revokePermF2", e.getMessage()));}
      // Remove creds
      if (system.getAuthnCredential() != null && !effectiveUserId.equals(APIUSERID_VAR)) {
        String accessUser = effectiveUserId;
        if (effectiveUserId.equals(OWNER_VAR)) accessUser = system.getOwner();
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        try { deleteCredential(skClient, rUser, resourceId, accessUser); }
        catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, resourceId, "deleteCred", e.getMessage()));}
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
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUser, jobIsBatch,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * Attributes that cannot be updated:
   *   tenant, id, systemType, owner, authnCredential, bucketName, rootDir, canExec, isDtn
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param patchSystem - Pre-populated PatchSystem object (including tenantId and systemId)
   * @param scrubbedText - Text used to create the PatchSystem object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void patchSystem(ResourceRequestUser rUser, String systemId, PatchSystem patchSystem, String scrubbedText)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (patchSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract various names for convenience
    String resourceTenantId = rUser.getOboTenantId();
    String resourceId = systemId;

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(resourceId) || StringUtils.isBlank(scrubbedText))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, resourceId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(resourceTenantId, resourceId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, resourceId));

    // Retrieve the system being patched and create fully populated TSystem with changes merged in
    TSystem origTSystem = dao.getSystem(resourceTenantId, resourceId);
    TSystem patchedTSystem = createPatchedTSystem(origTSystem, patchSystem);

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, resourceId, origTSystem.getOwner(), null, null);

    // ---------------- Check constraints on TSystem attributes ------------------------
    patchedTSystem.setDefaults();
    validateTSystem(rUser, patchedTSystem);

    // Construct Json string representing the PatchSystem about to be used to update the system
    String updateJsonStr = TapisGsonUtils.getGson().toJson(patchSystem);

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to persist the system -----------------------------------
    dao.patchSystem(rUser, resourceId, patchedTSystem, updateJsonStr, scrubbedText);
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
   * @param scrubbedText - Text used to create the System object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - Resulting TSystem would be in an invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void putSystem(ResourceRequestUser rUser, TSystem putSystem, boolean skipCredCheck, String scrubbedText)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.modify;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (putSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract various names for convenience
    String resourceTenantId = putSystem.getTenant();
    String resourceId = putSystem.getId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(resourceTenantId) || StringUtils.isBlank(resourceId) || StringUtils.isBlank(scrubbedText))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, resourceId));
    }

    // System must already exist and not be deleted
    if (!dao.checkForSystem(resourceTenantId, resourceId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, resourceId));

    // Retrieve the system being updated and create fully populated TSystem with updated attributes
    TSystem origTSystem = dao.getSystem(resourceTenantId, resourceId);
    TSystem updatedTSystem = createUpdatedTSystem(origTSystem, putSystem);

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, resourceId, origTSystem.getOwner(), null, null);

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, updatedTSystem);

    // ---------------- Verify credentials ------------------------
    if (!skipCredCheck && updatedTSystem.getAuthnCredential() != null)
    {
      verifyCredentials(rUser, updatedTSystem, updatedTSystem.getAuthnCredential());
    }

    // Construct Json string representing the PutSystem about to be used to update the system
    String updateJsonStr = TapisGsonUtils.getGson().toJson(putSystem);

    // ----------------- Create all artifacts --------------------
    // No distributed transactions so no distributed rollback needed
    // ------------------- Make Dao call to update the system -----------------------------------
    dao.putSystem(rUser, updatedTSystem, updateJsonStr, scrubbedText);
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

    String resourceTenantId = rUser.getOboTenantId();

    // System must exist
    if (!dao.checkForSystem(resourceTenantId, systemId, true))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);

    // Remove effectiveUser credentials associated with the system
    // Remove permissions associated with the system
    removeSKArtifacts(rUser, resourceTenantId, systemId);

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

    String resourceTenantId = rUser.getOboTenantId();

    // System must exist
    if (!dao.checkForSystem(resourceTenantId, systemId, true))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Get owner, if not found it is an error
    String owner = dao.getSystemOwner(resourceTenantId, systemId);
    if (StringUtils.isBlank(owner)) {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, systemId, op.name());
      _log.error(msg);
      throw new IllegalStateException(msg);
    }
    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);

    // Add permissions for owner
    String systemsPermSpecALL = getPermSpecAllStr(resourceTenantId, systemId);
    // TODO remove filesPermSpec related code (jira cic-3071)
    String filesPermSpec = "files:" + resourceTenantId + ":*:" + systemId;
    var skClient = getSKClient();
    // Give owner and possibly effectiveUser full access to the system
    skClient.grantUserPermission(resourceTenantId, owner, systemsPermSpecALL);
    // TODO remove filesPermSpec related code (jira cic-3071)
    // Give owner files service related permission for root directory
    skClient.grantUserPermission(resourceTenantId, owner, filesPermSpec);

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

    String resourceTenantId = rUser.getOboTenantId();

    // System must already exist and not be deleted
    if (!dao.checkForSystem(resourceTenantId, systemId, false))
         throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Retrieve old owner
    String oldOwnerName = dao.getSystemOwner(resourceTenantId, systemId);

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, oldOwnerName, null, null);

    // If new owner same as old owner then this is a no-op
    if (newOwnerName.equals(oldOwnerName)) return 0;

    // ----------------- Make all updates --------------------
    // Changes not in single DB transaction.
    // Use try/catch to rollback any changes in case of failure.
    // Get SK client now. If we cannot get this rollback not needed.
    var skClient = getSKClient();
    String systemsPermSpec = getPermSpecAllStr(resourceTenantId, systemId);
    // TODO remove addition of files related permSpec (jira cic-3071)
    String filesPermSpec = "files:" + resourceTenantId + ":*:" + systemId;
    try {
      // ------------------- Make Dao call to update the system owner -----------------------------------
      dao.updateSystemOwner(rUser, resourceTenantId, systemId, newOwnerName);
      // Add permissions for new owner
      skClient.grantUserPermission(resourceTenantId, newOwnerName, systemsPermSpec);
      // TODO remove addition of files related permSpec (jira cic-3071)
      // Give owner files service related permission for root directory
      skClient.grantUserPermission(resourceTenantId, newOwnerName, filesPermSpec);
      // Remove permissions from old owner
      skClient.revokeUserPermission(resourceTenantId, oldOwnerName, systemsPermSpec);
      // TODO: Notify files service of the change (jira cic-3071)
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      try { dao.updateSystemOwner(rUser, resourceTenantId, systemId, oldOwnerName); } catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "updateOwner", e.getMessage()));}
      // TODO remove filesPermSpec related code (jira cic-3071)
      try { skClient.revokeUserPermission(resourceTenantId, newOwnerName, filesPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermNewOwner", e.getMessage()));}
      try { skClient.revokeUserPermission(resourceTenantId, newOwnerName, filesPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));}
      try { skClient.grantUserPermission(resourceTenantId, oldOwnerName, systemsPermSpec); }
      catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPermOldOwner", e.getMessage()));}
      try { skClient.grantUserPermission(resourceTenantId, oldOwnerName, filesPermSpec); }
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
   * @param resourceTenantId - Tenant containing resources.
   * @param systemId - name of system
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  int hardDeleteSystem(ResourceRequestUser rUser, String resourceTenantId, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException
  {
    SystemOperation op = SystemOperation.hardDelete;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(resourceTenantId) ||  StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // If system does not exist then 0 changes
    if (!dao.checkForSystem(resourceTenantId, systemId, true)) return 0;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);

    // Remove SK artifacts
    removeSKArtifacts(rUser, resourceTenantId, systemId);

    // Delete the system
    return dao.hardDeleteSystem(resourceTenantId, systemId);
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
      // ------------------------- Check service level authorization -------------------------
      checkAuth(rUser, op, systemId, null, null, null);
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

    String resourceTenantId = rUser.getOboTenantId();

    // Resource must exist and not be deleted
    if (!dao.checkForSystem(resourceTenantId, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);
    return dao.isEnabled(resourceTenantId, systemId);
  }

  /**
   * getSystem
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Name of the system
   * @param getCreds - flag indicating if credentials for effectiveUserId should be included
   * @param accMethod - (optional) return credentials for specified authn method instead of default authn method
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @return populated instance of a TSystem or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public TSystem getSystem(ResourceRequestUser rUser, String systemId,
                           boolean getCreds, AuthnMethod accMethod, boolean requireExecPerm)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    // We need owner to check auth and if system not there cannot find owner, so return null if no system.
    if (!dao.checkForSystem(resourceTenantId, systemId, false)) return null;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);
    // If flag is set to also require EXECUTE perm then make a special auth call
    if (requireExecPerm)
    {
      checkAuthUser(rUser, SystemOperation.execute, resourceTenantId, rUser.getOboUserId(),
                    systemId, null, null, null);
    }

    TSystem result = dao.getSystem(resourceTenantId, systemId);
    if (result == null) return null;

    // If flag is set to also require EXECUTE perm then system must support execute
    if (requireExecPerm && !result.getCanExec())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_NOTEXEC", rUser, systemId, op.name());
      throw new NotAuthorizedException(msg, NO_CHALLENGE);
    }

    // Resolve effectiveUserId
    String resolvedEffectiveUserId = resolveEffectiveUserId(result.getEffectiveUserId(), result.getOwner(), rUser);
    result.setEffectiveUserId(resolvedEffectiveUserId);
    // If requested retrieve credentials from Security Kernel
    if (getCreds)
    {
      AuthnMethod tmpAccMethod = result.getDefaultAuthnMethod();
      // If authnMethod specified then use it instead of default authn method defined for the system.
      if (accMethod != null) tmpAccMethod = accMethod;
      Credential cred = getUserCredential(rUser, systemId, resolvedEffectiveUserId, tmpAccMethod);
      result.setAuthnCredential(cred);
    }
    return result;
  }

  /**
   * Get count of all systems matching certain criteria and for which user has READ permission
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param startAfter - where to start when sorting, e.g. orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
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
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList,
                                  int limit, List<OrderBy> orderByList, int skip, String startAfter, boolean showDeleted)
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

    for (TSystem system : systems)
    {
      system.setEffectiveUserId(resolveEffectiveUserId(system.getEffectiveUserId(), system.getOwner(), rUser));
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
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser,
                                                   String sqlSearchStr, int limit, List<OrderBy> orderByList, int skip,
                                                   String startAfter, boolean showDeleted)
          throws TapisException, TapisClientException
  {
    // If search string is empty delegate to getSystems()
    if (StringUtils.isBlank(sqlSearchStr)) return getSystems(rUser, null, limit, orderByList, skip,
                                                             startAfter, showDeleted);

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

    for (TSystem system : systems)
    {
      system.setEffectiveUserId(resolveEffectiveUserId(system.getEffectiveUserId(), system.getOwner(), rUser));
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
      system.setEffectiveUserId(resolveEffectiveUserId(system.getEffectiveUserId(), system.getOwner(), rUser));
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

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, null, null);

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
   * @param userName - Target user for operation
   * @param permissions - list of permissions to be granted
   * @param updateText - Client provided text used to create the permissions list. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void grantUserPermissions(ResourceRequestUser rUser, String systemId,
                                   String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.grantPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(userName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    // If system does not exist or has been deleted then throw an exception
    if (!dao.checkForSystem(resourceTenantId, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // Check to see if owner is trying to update permissions for themselves.
    // If so throw an exception because this would be confusing since owner always has full permissions.
    // For an owner permissions are never checked directly.
    String owner = checkForOwnerPermUpdate(rUser, systemId, userName, op.name());

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, owner, null, null);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Grant of MODIFY implies grant of READ
    if (permissions.contains(Permission.MODIFY)) permissions.add(Permission.READ);

    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(resourceTenantId, systemId, permissions);

    // Get the Security Kernel client
    var skClient = getSKClient();

    // Assign perms to user.
    // Start of updates. Will need to rollback on failure.
    try
    {
      // Assign perms to user. SK creates a default role for the user
      for (String permSpec : permSpecSet)
      {
        skClient.grantUserPermission(resourceTenantId, userName, permSpec);
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
        try { skClient.revokeUserPermission(resourceTenantId, userName, permSpec); }
        catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePerm", e.getMessage()));}
      }

      // Convert to TapisException and re-throw
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_PERM_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Construct Json string representing the update
    String updateJsonStr = TapisGsonUtils.getGson().toJson(permissions);
    // Create a record of the update
    dao.addUpdateRecord(rUser, resourceTenantId, systemId, op, updateJsonStr, updateText);
  }

  /**
   * Revoke permissions from a user for a system
   * Revoke of READ implies revoke of MODIFY
   * NOTE: Permissions only impact the default user role
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param userName - Target user for operation
   * @param permissions - list of permissions to be revoked
   * @param updateText - Client provided text used to create the permissions list. Saved in update record.
   * @return Number of items revoked
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int revokeUserPermissions(ResourceRequestUser rUser, String systemId,
                                   String userName, Set<Permission> permissions, String updateText)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.revokePerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(userName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    // We need owner to check auth and if system not there cannot find owner, so
    // if system does not exist or has been deleted then return 0 changes
    if (!dao.checkForSystem(resourceTenantId, systemId, false)) return 0;

    // Check to see if owner is trying to update permissions for themselves.
    // If so throw an exception because this would be confusing since owner always has full permissions.
    // For an owner permissions are never checked directly.
    String owner = checkForOwnerPermUpdate(rUser, systemId, userName, op.name());

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, owner, userName, permissions);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Revoke of READ implies revoke of MODIFY
    if (permissions.contains(Permission.READ)) permissions.add(Permission.MODIFY);

    var skClient = getSKClient();
    int changeCount;
    // Determine current set of user permissions
    var userPermSet = getUserPermSet(skClient, userName, resourceTenantId, systemId);

    try
    {
      // Revoke perms
      changeCount = revokePermissions(skClient, resourceTenantId, systemId, userName, permissions);
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
          String permSpec = getPermSpecStr(resourceTenantId, systemId, perm);
          try { skClient.grantUserPermission(resourceTenantId, userName, permSpec); }
          catch (Exception e) {_log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPerm", e.getMessage()));}
        }
      }

      // Convert to TapisException and re-throw
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_PERM_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Construct Json string representing the update
    String updateJsonStr = TapisGsonUtils.getGson().toJson(permissions);
    // Create a record of the update
    dao.addUpdateRecord(rUser, resourceTenantId, systemId, op, updateJsonStr, updateText);
    return changeCount;
  }

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param userName - Target user for operation
   * @return List of permissions
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public Set<Permission> getUserPermissions(ResourceRequestUser rUser,
                                            String systemId, String userName)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.getPerms;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(userName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    // If system does not exist or has been deleted then return null
    if (!dao.checkForSystem(rUser.getOboTenantId(), systemId, false)) return null;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, userName, null);

    // Use Security Kernel client to check for each permission in the enum list
    var skClient = getSKClient();
    return getUserPermSet(skClient, userName, rUser.getOboTenantId(), systemId);
  }

  // -----------------------------------------------------------------------
  // ---------------------------- Credentials ------------------------------
  // -----------------------------------------------------------------------

  /**
   * Store or update credential for given system and user.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param userName - Target user for operation
   * @param credential - list of permissions to be granted
   * @param skipCredCheck - Indicates if cred check for LINUX systems should happen
   * @param updateText - Client provided text used to create the credential - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public void createUserCredential(ResourceRequestUser rUser, String systemId,
                                   String userName, Credential credential, boolean skipCredCheck, String updateText)
          throws TapisException, NotFoundException, NotAuthorizedException, IllegalStateException, TapisClientException
  {
    SystemOperation op = SystemOperation.setCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(userName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    // If system does not exist or has been deleted then throw an exception
    if (!dao.checkForSystem(resourceTenantId, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, userName, null);

    // If private SSH key is set check that we have a compatible key.
    if (!StringUtils.isBlank(credential.getPrivateKey()) && !credential.isValidPrivateSshKey())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2", rUser, systemId, userName);
      throw new IllegalArgumentException(msg);
    }


    // ---------------- Verify credentials ------------------------
    if (!skipCredCheck)
    {
      TSystem system = dao.getSystem(resourceTenantId, systemId);
      verifyCredentials(rUser, system, credential);
    }

    // Create credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    var skClient = getSKClient();
    try
    {
      createCredential(skClient, rUser, credential, systemId, userName);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      _log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Construct Json string representing the update, with actual secrets masked out
    Credential maskedCredential = Credential.createMaskedCredential(credential);
    String updateJsonStr = TapisGsonUtils.getGson().toJson(maskedCredential);

    // Create a record of the update
    dao.addUpdateRecord(rUser, resourceTenantId, systemId, op, updateJsonStr, updateText);
  }

  /**
   * Delete credential for given system and user
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param userName - Target user for operation
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public int deleteUserCredential(ResourceRequestUser rUser, String systemId,
                                  String userName)
          throws TapisException, NotAuthorizedException, TapisClientException
  {
    SystemOperation op = SystemOperation.removeCred;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(userName))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    int changeCount = 0;
    // If system does not exist or has been deleted then return 0 changes
    if (!dao.checkForSystem(resourceTenantId, systemId, false)) return changeCount;

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, op, systemId, null, userName, null);

    // Get the Security Kernel client
    var skClient = getSKClient();

    // Delete credential
    // If this throws an exception we do not try to rollback. Attempting to track which secrets
    //   have been changed and reverting seems fraught with peril and not a good ROI.
    try {
      changeCount = deleteCredential(skClient, rUser, systemId, userName);
    }
    // If tapis client exception then log error and convert to TapisException
    catch (TapisClientException tce)
    {
      _log.error(tce.toString());
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_CRED_SK_ERROR", rUser, systemId, op.name()), tce);
    }

    // Construct Json string representing the update
    String updateJsonStr = TapisGsonUtils.getGson().toJson(userName);
    // Create a record of the update
    dao.addUpdateRecord(rUser, resourceTenantId, systemId, op, updateJsonStr, null);
    return changeCount;
  }

  /**
   * Get credential for given system, user and authn method
   * Only certain services are authorized.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUserId - Target user for operation
   * @param authnMethod - (optional) return credentials for specified authn method instead of default authn method
   * @return Credential - populated instance or null if not found.
   * @throws TapisException - for Tapis related exceptions
   * @throws NotAuthorizedException - unauthorized
   * @throws NotFoundException - Resource not found
   */
  @Override
  public Credential getUserCredential(ResourceRequestUser rUser, String systemId,
                                      String targetUserId, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, NotAuthorizedException, NotFoundException
  {
    SystemOperation op = SystemOperation.getCred;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(targetUserId))
         throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String resourceTenantId = rUser.getOboTenantId();

    // If system does not exist or has been deleted then return null
    if (!dao.checkForSystem(resourceTenantId, systemId, false)) return null;

    // ------------------------- Check service level authorization -------------------------
    // NOTE: No need to pass in an owner or userIdToCheck since only services are authorized.
    checkAuth(rUser, op, systemId, null, null, null);

    // If authnMethod not passed in fill in with default from system
    if (authnMethod == null)
    {
      AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(resourceTenantId, systemId);
      if (defaultAuthnMethod == null)  throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));
      authnMethod = defaultAuthnMethod;
    }

    /*
     * When the Systems services calls SK to read secrets it calls with a JWT as itself,
     *   jwtTenantId = admin tenant (Site Tenant Admin)
     *   jwtUserId = TapisConstants.SERVICE_NAME_SYSTEMS ("systems")
     *   and AccountType = TapisThreadContext.AccountType.service
     *
     * For Systems the secret needs to be scoped by the tenant associated with the system,
     *   the system id and the target user (i.e. the user associated with the secret).
     * Secrets for a system follow the format
     *   secret/tapis/tenant/tenant_id/system_id/user/user_id/key_type/S1
     * where tenant_id, system_id, user_id and key_type are filled in at runtime.
     *   key_type is sshkey, password, accesskey or cert and S1 is the reserved SecretName associated with the Systems.
     * Hence the following code
     *     new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME)
     *     sParms.setTenant(rUser.getOboTenantId()).setSysId(systemId).setSysUser(targetUserId);
     *
     */
    Credential credential = null;
    try
    {
      // Get the Security Kernel client
      var skClient = getSKClient();
      // Construct basic SK secret parameters
      // Establish secret type ("system") and secret name ("S1")
      var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);

      // Set tenant, system and user associated with the secret.
      // These values are used to build the vault path to the secret.
      sParms.setTenant(rUser.getOboTenantId()).setSysId(systemId).setSysUser(targetUserId);

      // NOTE: This is needed for the SK call. Not clear if it should be targetUserId, serviceUserId, apiUserId.
      //       If not set then the first getAuthnCred in SystemsServiceTest.testUserCredentials
      //          fails. But it appears the value does not matter. Even an invalid userId appears to be OK.
      sParms.setUser(rUser.getOboUserId());
      // Set key type based on authn method
      if (authnMethod.equals(AuthnMethod.PASSWORD))sParms.setKeyType(KeyType.password);
      else if (authnMethod.equals(AuthnMethod.PKI_KEYS))sParms.setKeyType(KeyType.sshkey);
      else if (authnMethod.equals(AuthnMethod.ACCESS_KEY))sParms.setKeyType(KeyType.accesskey);
      else if (authnMethod.equals(AuthnMethod.CERT))sParms.setKeyType(KeyType.cert);

      // Retrieve the secrets
      SkSecret skSecret = skClient.readSecret(sParms);
      if (skSecret == null) return null;
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) return null;

      // Create a credential
      credential = new Credential(authnMethod,
              dataMap.get(SK_KEY_PASSWORD),
              dataMap.get(SK_KEY_PRIVATE_KEY),
              dataMap.get(SK_KEY_PUBLIC_KEY),
              dataMap.get(SK_KEY_ACCESS_KEY),
              dataMap.get(SK_KEY_ACCESS_SECRET),
              null); //dataMap.get(CERT) TODO: get ssh certificate when supported
    }
    // If tapis client exception then log error but continue so null is returned.
    catch (TapisClientException tce)
    {
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
   * @param scrubbedText - Text used to create the profile object - secrets should be scrubbed. Saved in update record.
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - resource exists OR is in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   * @throws NotAuthorizedException - unauthorized
   */
  @Override
  public void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile, String scrubbedText)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException, NotAuthorizedException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (schedulerProfile == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    _log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, scrubbedText));
    String resourceTenantId = schedulerProfile.getTenant();
    String resourceId = schedulerProfile.getName();

    // ---------------------------- Check inputs ------------------------------------
    // Required attributes: tenant, id, moduleLoadCommand
    if (StringUtils.isBlank(resourceTenantId) || StringUtils.isBlank(resourceId) ||
            StringUtils.isBlank(schedulerProfile.getModuleLoadCommand()) || StringUtils.isBlank(scrubbedText))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, resourceId));
    }

    // Check if schedulerProfile already exists
    if (dao.checkForSchedulerProfile(resourceTenantId, resourceId))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_PRF_EXISTS", rUser, resourceId));
    }

    // Check authorization
    checkPrfAuth(rUser, op, schedulerProfile.getName(), schedulerProfile.getOwner());

    // Construct Json string representing the resource about to be created
    String createJsonStr = TapisGsonUtils.getGson().toJson(schedulerProfile);

    // No distributed transactions so no distributed rollback needed
    // Make Dao call to persist the resource
    dao.createSchedulerProfile(rUser, schedulerProfile, createJsonStr, scrubbedText);
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

    String resourceTenantId = rUser.getOboTenantId();

    // If profile does not exist or has been deleted then return 0 changes
    if (!dao.checkForSchedulerProfile(resourceTenantId, name)) return 0;

    // Check authorization
    checkPrfAuth(rUser, op, name, null);

    // Use dao to delete the resource
    return dao.deleteSchedulerProfile(resourceTenantId, name);
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

    String resourceTenantId = rUser.getOboTenantId();

    // resource must already exist and not be deleted
    if (!dao.checkForSystem(resourceTenantId, systemId, false))
      throw new NotFoundException(LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ------------------------- Check service level authorization -------------------------
    checkAuth(rUser, sysOp, systemId, null, null, null);

    // ----------------- Make update --------------------
    if (sysOp == SystemOperation.enable)
      dao.updateEnabled(rUser, resourceTenantId, systemId, true);
    else
      dao.updateEnabled(rUser, resourceTenantId, systemId, false);
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
    String resourceTenantId = rUser.getOboTenantId();
    // ----------------- Make update --------------------
    if (sysOp == SystemOperation.delete)
      dao.updateDeleted(rUser, resourceTenantId, systemId, true);
    else
      dao.updateDeleted(rUser, resourceTenantId, systemId, false);
    return 1;
  }

  /**
   * Get Security Kernel client
   * Note: The service always calls SK as itself.
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient() throws TapisException
  {
    SKClient skClient;
    String tenantId = getServiceTenantId();
    String userName = getServiceUserId();
    try
    {
      skClient = serviceClients.getClient(userName, tenantId, SKClient.class);
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, tenantId, userName);
      throw new TapisException(msg, e);
    }

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
   * If DTN is used verify that dtnSystemId exists with isDtn = true
   * Collect and report as many errors as possible so they can all be fixed before next attempt
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private void validateTSystem(ResourceRequestUser rUser, TSystem tSystem1) throws IllegalStateException
  {
    String msg;
    // Make api level checks, i.e. checks that do not involve a dao or service call.
    List<String> errMessages = tSystem1.checkAttributeRestrictions();

    // Now make checks that do require a dao or service call.

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
   * Verify that effectiveUserId can connect to the system using provided credentials.
   * Skipped if no credentials provided or effectiveUserId is not static.
   * NOTE: Skipped for non-LINUX systems
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tSystem1 - the TSystem to check
   * @param credToCheck - credentials to check
   * @throws IllegalStateException - if credentials not verified
   */
  private void verifyCredentials(ResourceRequestUser rUser, TSystem tSystem1, Credential credToCheck) throws IllegalStateException
  {
    // We must have the system and a set of credentials to check.
    if (tSystem1 == null || credToCheck == null) return;
    // Check is only done for LINUX systems
    if (!tSystem1.getSystemType().equals(TSystem.SystemType.LINUX)) return;
    // If effectiveUserId is not static then skip check.
    if (tSystem1.getEffectiveUserId().equals(APIUSERID_VAR)) return;

    String host = tSystem1.getHost();
    int port = tSystem1.getPort();
    String userName = tSystem1.getEffectiveUserId();
    AuthnMethod authnMethod = tSystem1.getDefaultAuthnMethod();
    _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_START", rUser, tSystem1.getId(), userName, host, port, authnMethod.name()));
    // Attempt to connect to the system
    String msg;
    SSHConnection sshConnection = null;
    try
    {
      if (AuthnMethod.PASSWORD.equals(authnMethod))
      {
        sshConnection = new SSHConnection(host, port, userName, credToCheck.getPassword());
      }
      else if (AuthnMethod.PKI_KEYS.equals(authnMethod))
      {
        sshConnection = new SSHConnection(host, port, userName, credToCheck.getPublicKey(), credToCheck.getPrivateKey());
      }
      else
      {
        msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_INVALID_AUTHN", rUser, tSystem1.getId(), userName, authnMethod.name());
        _log.error(msg);
      }
    }
    catch (TapisException e)
    {
      msg = LibUtils.getMsgAuth("SYSLIB_CRED_CONN_FAIL", rUser, tSystem1.getId(), host, userName, authnMethod.name(),
                                e.getMessage());
      _log.error(msg, e);
      throw new IllegalStateException(msg);
    }
    finally { if (sshConnection != null) sshConnection.close(); }
    _log.debug(LibUtils.getMsgAuth("SYSLIB_CRED_VERIFY_END", rUser, tSystem1.getId(), userName, host, port, authnMethod.name()));
  }

  /**
   * If effectiveUserId is dynamic then resolve it
   * @param userId - effectiveUserId string, static or dynamic
   * @return Resolved value for effective user.
   */
  private static String resolveEffectiveUserId(String userId, String owner, ResourceRequestUser rUser)
  {
    // NOTE: Use oboUser as the effective apiUserId because for a User request oboUser and apiUser are the same
    //       and for a Service request it is the oboUser and not the service user who is effectively the user
    //       making the request
    if (StringUtils.isBlank(userId)) return userId;
    else if (userId.equals(OWNER_VAR) && !StringUtils.isBlank(owner)) return owner;
    else if (userId.equals(APIUSERID_VAR) && rUser != null) return rUser.getOboUserId();
    else return userId;
  }


  /**
   * Retrieve set of user permissions given sk client, user, tenant, id
   * @param skClient - SK client
   * @param userName - name of user
   * @param resourceTenantId - name of tenant associated with resource
   * @param resourceId - Id of resource
   * @return - Set of Permissions for the user
   */
  private static Set<Permission> getUserPermSet(SKClient skClient, String userName, String resourceTenantId,
                                                String resourceId)
          throws TapisClientException
  {
    var userPerms = new HashSet<Permission>();
    for (Permission perm : Permission.values())
    {
      String permSpec = String.format(PERM_SPEC_TEMPLATE, resourceTenantId, perm.name(), resourceId);
      if (skClient.isPermitted(resourceTenantId, userName, permSpec)) userPerms.add(perm);
    }
    return userPerms;
  }

  /**
   * Create a set of individual permSpec entries based on the list passed in
   * @param resourceTenantId - name of tenant associated with resource
   * @param systemId - resource Id
   * @param permList - list of individual permissions
   * @return - Set of permSpec entries based on permissions
   */
  private static Set<String> getPermSpecSet(String resourceTenantId, String systemId, Set<Permission> permList)
  {
    var permSet = new HashSet<String>();
    for (Permission perm : permList) { permSet.add(getPermSpecStr(resourceTenantId, systemId, perm)); }
    return permSet;
  }

  /**
   * Create a permSpec given a permission
   * @param perm - permission
   * @return - permSpec entry based on permission
   */
  private static String getPermSpecStr(String resourceTenantId, String systemId, Permission perm)
  {
    return String.format(PERM_SPEC_TEMPLATE, resourceTenantId, perm.name(), systemId);
  }

  /**
   * Create a permSpec for all permissions
   * @return - permSpec entry for all permissions
   */
  private static String getPermSpecAllStr(String resourceTenantId, String systemId)
  {
    return String.format(PERM_SPEC_TEMPLATE, resourceTenantId, "*", systemId);
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
   * @param userName user for whom perms are being updated
   * @param opStr Operation in progress, for logging
   * @return name of owner
   */
  private String checkForOwnerPermUpdate(ResourceRequestUser rUser, String systemId,
                                         String userName, String opStr)
          throws TapisException, NotAuthorizedException
  {
    // Look up owner. If not found then consider not authorized. Very unlikely at this point.
    String owner = dao.getSystemOwner(rUser.getOboTenantId(), systemId);
    if (StringUtils.isBlank(owner))
        throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, opStr), NO_CHALLENGE);
    // If owner making the request and owner is the target user for the perm update then reject.
    if (owner.equals(rUser.getOboUserId()) && owner.equals(userName))
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
   * Standard service level authorization check. Check is different for service and user requests.
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and authorization is denied.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   * @param owner - system owner
   * @param userIdToCheck - optional name of the user to check. Default is to use rUser.getJwtUserId().
   * @param perms - List of permissions for the revokePerm case
   * @throws NotAuthorizedException - apiUserId not authorized to perform operation
   */
  private void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId,
                         String owner, String userIdToCheck, Set<Permission> perms)
      throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    // Check service and user requests separately to avoid confusing a service name with a user name
    if (rUser.isServiceRequest())
    {
      // This is a service request. The user name will be the service name. E.g. files, jobs, streams, etc
      // For read only certain services allowed.
      if (op == SystemOperation.read && SVCLIST_READ.contains(rUser.getJwtUserId())) return;
      // For getCred only certain services are allowed. Everyone else denied with a special message
      else if (op == SystemOperation.getCred)
      {
        if (SVCLIST_GETCRED.contains(rUser.getJwtUserId())) return;
        else
        {
          throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_AUTH_GETCRED", rUser,
                                                               systemId, op.name()), NO_CHALLENGE);
        }
      }
    }
    else
    {
      // This is a user check
      checkAuthUser(rUser, op, null, null, systemId, owner, userIdToCheck, perms);
      return;
    }
    // Not authorized, throw an exception
    throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()), NO_CHALLENGE);
  }

  /**
   * User based authorization check.
   * Can be used for OBOUser type checks.
   * By default use tenant and user from rUser, allow for optional tenant or user.
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and
   *   authorization is denied.
   * Operations:
   *  Create -      must be owner or have admin role
   *  Delete -      must be owner or have admin role
   *  ChangeOwner - must be owner or have admin role
   *  GrantPerm -   must be owner or have admin role
   *  Read -     must be owner or have admin role or have READ or MODIFY permission or be in list of allowed services
   *  getPerms - must be owner or have admin role or have READ or MODIFY permission or be in list of allowed services
   *  Modify - must be owner or have admin role or have MODIFY permission
   *  Execute - must be owner or have admin role or have EXECUTE permission
   *  RevokePerm -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserRevokePerm)
   *  SetCred -     must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserCredOp)
   *  RemoveCred -  must be owner or have admin role or apiUserId=targetUser and meet certain criteria (allowUserCredOp)
   *  GetCred -     Deny. Only authorized services may get credentials. Set specific message.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param tenantIdToCheck - optional name of the tenant to use. Default is to use rUser.getJwtTenantId().
   * @param userIdToCheck - optional name of the user to check. Default is to use rUser.getJwtUserId().
   * @param systemId - name of the system
   * @param owner - system owner
   * @param perms - List of permissions for the revokePerm case
   * @throws NotAuthorizedException - apiUserId not authorized to perform operation
   */
  private void checkAuthUser(ResourceRequestUser rUser, SystemOperation op,
                             String tenantIdToCheck, String userIdToCheck,
                             String systemId, String owner, String targetUser, Set<Permission> perms)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException
  {
    // Use JWT tenant and user from resourceRequestUser or optional provided values
    String tenantName = (StringUtils.isBlank(tenantIdToCheck) ? rUser.getJwtTenantId() : tenantIdToCheck);
    String userName = (StringUtils.isBlank(userIdToCheck) ? rUser.getJwtUserId() : userIdToCheck);

    // Some checks do not require owner
    switch(op) {
      case hardDelete:
        if (hasAdminRole(rUser, tenantName, userName))
          return;
        break;
      case getCred:
        // Only some services allowed to get credentials. Never a user.
        throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_AUTH_GETCRED", rUser,
                                                             systemId, op.name()), NO_CHALLENGE);
    }

    // Most checks require owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSystemOwner(tenantName, systemId);
    if (StringUtils.isBlank(owner)) {
      String msg = LibUtils.getMsgAuth("SYSLIB_AUTH_NO_OWNER", rUser, systemId, op.name());
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
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName))
          return;
        break;
      case read:
      case getPerms:
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName) ||
              isPermittedAny(rUser, tenantName, userName, systemId, READMODIFY_PERMS))
          return;
        break;
      case modify:
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName) ||
                isPermitted(rUser, tenantName, userName, systemId, Permission.MODIFY))
          return;
        break;
      case execute:
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName) ||
                isPermitted(rUser, tenantName, userName, systemId, Permission.EXECUTE))
          return;
        break;
      case revokePerms:
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName) ||
                (userName.equals(targetUser) &&
                        allowUserRevokePerm(rUser, tenantName, userName, systemId, perms)))
          return;
        break;
      case setCred:
      case removeCred:
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName) ||
                (userName.equals(targetUser) && allowUserCredOp(rUser, systemId, op)))
          return;
        break;
    }
    // Not authorized, throw an exception
    throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name()), NO_CHALLENGE);
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
   * @throws NotAuthorizedException - apiUserId not authorized to perform operation
   */
  private void checkPrfAuth(ResourceRequestUser rUser, SchedulerProfileOperation op, String name, String owner)
          throws TapisException, TapisClientException, NotAuthorizedException
  {
    // Anyone can read, including all services
    if (op == SchedulerProfileOperation.read) return;

    String tenantName = rUser.getOboTenantId();
    String userName = rUser.getOboUserId();

    // Check requires owner. If no owner specified and owner cannot be determined then log an error and deny.
    if (StringUtils.isBlank(owner)) owner = dao.getSchedulerProfileOwner(tenantName, name);
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
        if (owner.equals(userName) || hasAdminRole(rUser, tenantName, userName))
          return;
        break;
    }

    // Not authorized, throw an exception
    throw new NotAuthorizedException(LibUtils.getMsgAuth("SYSLIB_PRF_UNAUTH", rUser, name, op.name()), NO_CHALLENGE);
  }

  /**
   * Determine all systems that a user is allowed to see.
   * If all systems return null else return list of system IDs
   * An empty list indicates no systems allowed.
   */
  private Set<String> getAllowedSysIDs(ResourceRequestUser rUser)
          throws TapisException, TapisClientException
  {
    // If requester is a service calling as itself or an admin then all systems allowed
    if (rUser.isServiceRequest() && rUser.getJwtUserId().equals(rUser.getOboUserId()) ||
            hasAdminRole(rUser, null, null))
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
   * Check to see if a user has the service admin role
   * By default use JWT tenant and user from resourceRequestUser, allow for optional tenant or user.
   */
  private boolean hasAdminRole(ResourceRequestUser rUser, String tenantToCheck, String userToCheck)
          throws TapisException, TapisClientException
  {
    // Use tenant and user from authenticatedUsr or optional provided values
    String tenantName = (StringUtils.isBlank(tenantToCheck) ? rUser.getJwtTenantId() : tenantToCheck);
    String userName = (StringUtils.isBlank(userToCheck) ? rUser.getJwtUserId() : userToCheck);
    return getSKClient().isAdmin(tenantName, userName);
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
    var skClient = getSKClient();
    String permSpecStr = getPermSpecStr(tenantName, systemId, perm);
    return skClient.isPermitted(tenantName, userName, permSpecStr);
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
    var skClient = getSKClient();
    var permSpecs = new ArrayList<String>();
    for (Permission perm : perms) {
      permSpecs.add(getPermSpecStr(tenantName, systemId, perm));
    }
    return skClient.isPermittedAny(tenantName, userName, permSpecs.toArray(TSystem.EMPTY_STR_ARRAY));
  }

  /**
   * Check to see if a user who is not owner or admin is authorized to revoke permissions
   * By default use JWT tenant and user from rUser, allow for optional tenant or user.
   */
  private boolean allowUserRevokePerm(ResourceRequestUser rUser, String tenantToCheck, String userToCheck,
                                      String systemId, Set<Permission> perms)
          throws TapisException, TapisClientException
  {
    // Perms should never be null. Fall back to deny as best security practice.
    if (perms == null) return false;
    // Use tenant and user from authenticatedUsr or optional provided values
    String tenantName = (StringUtils.isBlank(tenantToCheck) ? rUser.getJwtTenantId() : tenantToCheck);
    String userName = (StringUtils.isBlank(userToCheck) ? rUser.getJwtUserId() : userToCheck);
    if (perms.contains(Permission.MODIFY)) return isPermitted(rUser, tenantName, userName, systemId, Permission.MODIFY);
    if (perms.contains(Permission.READ)) return isPermittedAny(rUser, tenantName, userName, systemId, READMODIFY_PERMS);
    return false;
  }

  /**
   * Check to see if apiUserId who is not owner or admin is authorized to operate on a credential
   * No checks are done for incoming arguments and the system must exist
   */
  private boolean allowUserCredOp(ResourceRequestUser rUser, String systemId, SystemOperation op)
          throws TapisException, IllegalStateException
  {
    // Get the effectiveUserId. If not ${apiUserId} then considered an error since credential would never be used.
    String effectiveUserId = dao.getSystemEffectiveUserId(rUser.getOboTenantId(), systemId);
    if (StringUtils.isBlank(effectiveUserId) || !effectiveUserId.equals(APIUSERID_VAR))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOTAPIUSER", rUser, systemId, op.name());
      _log.error(msg);
      throw new IllegalStateException(msg);

    }
    return true;
  }

  /*
   * Create or update a credential
   * No checks are done for incoming arguments and the system must exist
   *
   * When the Systems services calls SK to create secrets it calls with a JWT as itself,
   *   jwtTenantId = admin tenant (Site Tenant Admin)
   *   jwtUserId = TapisConstants.SERVICE_NAME_SYSTEMS ("systems")
   *   and AccountType = TapisThreadContext.AccountType.service
   *
   * For Systems the secret needs to be scoped by the tenant associated with the system,
   *   the system id and the target user (i.e. the user associated with the secret).
   * Secrets for a system follow the format
   *   secret/tapis/tenant/tenant_id/system_id/user/user_id/key_type/S1
   * where tenant_id, system_id, user_id and key_type are filled in at runtime.
   *   key_type is sshkey, password, accesskey or cert
   *   and S1 is the reserved SecretName associated with the Systems.
   * Hence the following code
   *     new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME)
   *     sParms.setSysId(systemId).setSysUser(userName)
   *     skClient.writeSecret(reqPayloadTenant, getServiceUserId(), sParms);
   * In the SKClient code the tenant value in SKSecretWriteParms is ignored.
   * See method writeSecret(String tenant, String user, SKSecretWriteParms parms) in SKClient.java
   * SK uses tenant from payload when constructing the full path for the secret. User from payload not used.
   */
  private static void createCredential(SKClient skClient, ResourceRequestUser rUser, Credential credential,
                                       String systemId, String userName)
          throws TapisClientException
  {
    // Construct basic SK secret parameters including tenant, system and user for credential
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    // Fill in systemId and userId for the path to the secret.
    sParms.setSysId(systemId).setSysUser(userName);
    Map<String, String> dataMap;
    // Check for each secret type and write values if they are present
    // Note that multiple secrets may be present.
    // Store password if present
    if (!StringUtils.isBlank(credential.getPassword())) {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.password);
      dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
      sParms.setData(dataMap);
      // First 2 parameters correspond to tenant and user from request payload
      // Tenant is used in constructing full path for secret, user is not used.
      skClient.writeSecret(rUser.getOboTenantId(), rUser.getOboUserId(), sParms);
    }
    // Store PKI keys if both present
    if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey())) {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.sshkey);
      dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
      dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
      sParms.setData(dataMap);
      skClient.writeSecret(rUser.getOboTenantId(), rUser.getOboUserId(), sParms);
    }
    // Store Access key and secret if both present
    if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret())) {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.accesskey);
      dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
      dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
      sParms.setData(dataMap);
      skClient.writeSecret(rUser.getOboTenantId(), rUser.getOboUserId(), sParms);
    }
    // TODO if necessary handle ssh certificate when supported
  }

  /**
   * Delete a credential
   * No checks are done for incoming arguments and the system must exist
   */
  private static int deleteCredential(SKClient skClient, ResourceRequestUser rUser, String systemId, String userName)
          throws TapisClientException
  {
    String apiTenantId = rUser.getOboTenantId();
    String apiUserId = rUser.getOboUserId();

    // Return 0 if credential does not exist
    var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    sMetaParms.setTenant(apiTenantId).setUser(apiUserId);
    sMetaParms.setSysId(systemId).setSysUser(userName);
    // NOTE: To be sure we know that the secret does not exist we need to check each key type
    //       By default keyType is sshkey which may not exist
    boolean secretNotFound = true;
    sMetaParms.setKeyType(KeyType.password);
    try { skClient.readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { skClient.readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { skClient.readSecretMeta(sMetaParms); secretNotFound = false; }
    catch (Exception e) { _log.trace(e.getMessage()); }
    if (secretNotFound) return 0;

    // Construct basic SK secret parameters and attempt to destroy each type of secret.
    // If destroy attempt throws an exception then log a message and continue.
    sMetaParms.setKeyType(KeyType.password);
    try { skClient.destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.sshkey);
    try { skClient.destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    sMetaParms.setKeyType(KeyType.accesskey);
    try { skClient.destroySecretMeta(sMetaParms); }
    catch (Exception e) { _log.trace(e.getMessage()); }
    return 1;
  }

  /**
   * Remove all SK artifacts associated with a System: user credentials, user permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private void removeSKArtifacts(ResourceRequestUser rUser, String resourceTenantId, String systemId)
          throws TapisException, TapisClientException
  {
    var skClient = getSKClient();

    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, resourceTenantId, "%", systemId);
    var userNames = skClient.getUsersWithPermission(resourceTenantId, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames) {
      revokePermissions(skClient, resourceTenantId, systemId, userName, ALL_PERMS);
      // Remove wildcard perm
      skClient.revokeUserPermission(resourceTenantId, userName, getPermSpecAllStr(resourceTenantId, systemId));
    }

    // Fetch the system. If system not found then return
    TSystem system = dao.getSystem(resourceTenantId, systemId, true);
    if (system == null) return;
    // Resolve effectiveUserId if necessary
    String effectiveUserId = system.getEffectiveUserId();
    String resolvedEffectiveUserId = resolveEffectiveUserId(effectiveUserId, system.getOwner(), rUser);

    // TODO remove filesPermSpec related code (jira cic-3071)
    // Remove files perm for owner and possibly effectiveUser
    String filesPermSpec = "files:" + resourceTenantId + ":*:" + systemId;
    skClient.revokeUserPermission(resourceTenantId, system.getOwner(), filesPermSpec);
    if (!effectiveUserId.equals(APIUSERID_VAR))
      skClient.revokeUserPermission(resourceTenantId, resolvedEffectiveUserId, filesPermSpec);;

    // Remove credentials associated with the system.
    // TODO: Have SK do this in one operation?
    // TODO: How to remove for users other than effectiveUserId?
    // Remove credentials in Security Kernel if effectiveUser is static
    if (!effectiveUserId.equals(APIUSERID_VAR)) {
      // Use private internal method instead of public API to skip auth and other checks not needed here.
      deleteCredential(skClient, rUser, system.getId(), resolvedEffectiveUserId);
    }
  }

  /**
   * Revoke permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private static int revokePermissions(SKClient skClient, String resourceTenantId, String systemId, String userName,
                                       Set<Permission> permissions)
          throws TapisClientException
  {
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(resourceTenantId, systemId, permissions);
    // Remove perms from default user role
    for (String permSpec : permSpecSet)
    {
      skClient.revokeUserPermission(resourceTenantId, userName, permSpec);
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
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUers, jobIsBatch,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * The only attribute that can be reset to default is effectiveUserId. It is reset when
   *   a blank string is passed in.
   */
  private TSystem createPatchedTSystem(TSystem o, PatchSystem p)
  {
    TSystem p1 = new TSystem(o);
    if (p.getDescription() != null) p1.setDescription(p.getDescription());
    if (p.getHost() != null) p1.setHost(p.getHost());
    if (p.getEffectiveUserId() != null) {
      if (StringUtils.isBlank(p.getEffectiveUserId())) {
        p1.setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
      } else {
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
    if (p.getJobIsBatch() != null) p1.setJobIsBatch(p.getJobIsBatch());
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
}
