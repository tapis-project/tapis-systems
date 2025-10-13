package edu.utexas.tacc.tapis.systems.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTParser;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisRunCommand;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.model.*;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

import static edu.utexas.tacc.tapis.shared.TapisConstants.SYSTEMS_SERVICE;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;
import static edu.utexas.tacc.tapis.systems.service.AuthUtils.*;

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

  public static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_SYSTEMS;
  public static final String FILES_SERVICE = TapisConstants.SERVICE_NAME_FILES;
  public static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  public static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;

  // Default interval in minutes for running the maintenance background task
  public static final int DEFAULT_SVC_MAINT_INTERVAL = 60;

  // Long names for certain operations
  static final String CREATE_SYS_OP = "createSystem";
  public static final String PUT_SYS_OP = "putSystem";
  public static final String GET_SYS_OP = "getSystem";
  public static final String GET_SYSF_OP = "getSystemsFinal";

  // Allow interrupt when shutting down executor services.
  private static final boolean mayInterruptIfRunning = true;

  // Message keys
  static final String NOT_FOUND = "SYSLIB_NOT_FOUND";
  static final String ERROR_ROLLBACK = "SYSLIB_ERROR_ROLLBACK";

  // SFTP client throws IOException containing this string if a path does not exist.
  private static final String NO_SUCH_FILE = "no such file";

  // Compiled regex for splitting around ":"
  private static final Pattern COLON_SPLIT = Pattern.compile(":");

  // Named and typed null values to make it clear what is being passed in to a method
  static final String nullTargetUser = null;
  static final String nullLoginUserMapping = null;
  private static final String nullOwner = null;
  private static final AuthnMethod nullAuthnMethod = null;
  private static final String nullImpersonationId = null;
  private static final String nullSharedAppCtx = null;
  private static final String nullResourceTenant = null;
  private static final Set<Permission> nullPermSet = null;
  private static final SystemShare nullSystemShare = null;
  private static final Credential nullCredential = null;
  
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
  private SysUtils sysUtils;
  @Inject
  private AuthUtils authUtils;
  @Inject
  private CredUtils credUtils;
  @Inject
  private ServiceContext serviceContext;

  private MaintenanceTask maintenanceTask; // Runnable maintenance task run via executor

  // ExecutorService and future for maintenance task
  private final ScheduledExecutorService maintenanceExecService = Executors.newSingleThreadScheduledExecutor();
  private Future<?> maintenanceTaskFuture;

  // We must be running on a specific site and this will never change
  // These are initialized in method initService()
  private static String siteId;
  private static String siteAdminTenantId;
  private static ResourceRequestUser rUserSvc;
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
  public void initService(String siteId1, String siteAdminTenantId1, RuntimeParameters runtimeParameters)
        throws TapisException, TapisClientException
  {
    // Initialize service context and site info
    siteId = siteId1;
    siteAdminTenantId = siteAdminTenantId1;
    serviceContext.initServiceJWT(siteId, SYSTEMS_SERVICE, runtimeParameters.getServicePassword());
    CredUtils.initTmsConfiguration();
    // Make sure DB is present and updated to latest version using flyway
    dao.migrateDB();

    // Create a ResourceRequest user representing the service. Used by some methods for logging.
    String svcName = getServiceUserId();
    String svcTenant = getServiceTenantId();
    var authUser = new AuthenticatedUser(svcName, svcTenant, TapisThreadContext.AccountType.service.name(), null,
                                         svcName, svcTenant, null, siteId, null);
    rUserSvc = new ResourceRequestUser(authUser);

    // Create the maintenanceTask runnable
    maintenanceTask = new MaintenanceTask(rUserSvc, dao, credUtils);

    // Check the systems_cred_info table and perform initial single-threaded synchronization steps.
    // - Mark IN_PROGRESS as FAILED
    // - Create PENDING records as needed for undeleted systems that have static effectiveUserId
    // - Mark FAILED as PENDING
    // - Process PENDING
    credUtils.initCredInfo(rUserSvc);
  }

  /**
   * Start the maintenance task thread
   * The maintenanceTask is a ScheduledExecutorService that runs periodically using the value passed
   * in as the period in minutes.
   *
   * @param intervalMinutes execution period in minutes
   */
  @Override
  public void startMaintenanceTask(long intervalMinutes)
  {
    log.info(LibUtils.getMsg("SYSLIB_MAINT_TASK_START"));
    maintenanceTaskFuture =
            maintenanceExecService.scheduleAtFixedRate(() -> MaintenanceTask.runMaintenance(maintenanceTask),
                  intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
  }

  /*
   * Stop the maintenance task thread
   */
  @Override
  public void stopMaintenanceTask()
  {
    log.info(LibUtils.getMsg("SYSLIB_MAINT_TASK_STOP"));
    if (maintenanceTaskFuture != null) maintenanceTaskFuture.cancel(mayInterruptIfRunning);
  }

  /**
   * Check that we can connect with DB and that the main table of the service exists.
   * @return null if all OK else return an Exception
   */
  @Override
  public Exception checkDB()
  {
    return dao.checkDB();
  }

  // -----------------------------------------------------------------------
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------

  /**
   * Create a new system object given a TSystem and the raw data used to create the TSystem.
   * Secrets in the rawData should be masked.
   *
   * NOTE that if credentials are provided and checked, and credentials are invalid,
   *    a system object is still returned. Caller must check Credential.getValidationResult()
   *
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
    TSystem retSystem = null; // The system object to return at the end
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (system == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, rawData));

    // Extract some attributes for convenience and clarity.
    // NOTE: do not do this here for effectiveUserId since it may be ${owner} and only get resolved below.
    String sysTenant = system.getTenant();
    String sysId = system.getId();
    SystemType sysType = system.getSystemType();
    AuthnMethod authnMethod = system.getDefaultAuthnMethod();

    // ---------------------------- Check inputs ------------------------------------
    // Required system attributes: tenant, id, type, host, defaultAuthnMethod
    if (StringUtils.isBlank(sysTenant) || StringUtils.isBlank(sysId) || system.getSystemType() == null ||
        StringUtils.isBlank(system.getHost()) || authnMethod == null || StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, sysId));
    }

    // Check if system already exists
    if (dao.checkForSystem(sysTenant, sysId, true))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, sysId);
      log.warn(msg);
      throw new IllegalStateException(msg);
    }

    // ==========================================================================================================
    // WARNING: Be very careful of ordering of steps from here on.
    //          Ordering of setting defaults, resolving variables and validating attributes can be critical.
    // ==========================================================================================================

    // Make sure owner, effectiveUserId, notes, tags, jobEnvVariables and batchDefaultLogincalQueue. are all set.
    // Note that this is done before auth so owner can get resolved and used during auth check.
    system.setDefaults();

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    // NOTE: This also handles case where effectiveUserId is ${owner},
    //       so after this effUser is either a resolved static string or ${apiUserId}
    //       and the only variable of interest in rootDir should be HOST_EVAL($var)
    system.resolveVariablesAtCreate(rUser.getOboUserId());

    // Now we can extract effUser and owner, for convenience and clarity. NOTE: Unresolved effUser.
    String sysEffUserId = system.getEffectiveUserId();
    String sysOwner = system.getOwner();

    // Determine if effectiveUserId is static
    boolean isStaticEffUser = !APIUSERID_VAR.equals(sysEffUserId);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, sysId, sysOwner);

    // ---------------- Check constraints on TSystem attributes. There are many. ------------------------
    validateTSystem(rUser, system, true);

    // ---------------- Check for reserved names ------------------------
    checkReservedIds(rUser, sysId);

    // We only do that when credentials provided and effectiveUser is static
    Credential cred = system.getAuthnCredential();

    // Set flag indicating if we will deal with credentials.
    boolean manageCredentials = (cred != null && isStaticEffUser);

    // Check that user is not trying to register credentials with a dynamic effUser.
    // NOTE: If effectiveUserId is dynamic then request has already been rejected above during
    //       call to validateTSystem(). See method TSystem.checkAttrMisc().
    //       But we include isStaticEffUser here anyway in case that ever changes.
    if (!isStaticEffUser && cred != null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CRED_NOT_ALLOWED", rUser, sysId);
      log.warn(msg);
      throw new IllegalArgumentException(msg);
    }

    // If credentials provided validate constraints and verify credentials
    Credential verifiedCred = cred;
    String hostLoginUser = null;
    String credTargetUser = null;
    if (manageCredentials)
    {
      // NOTE: At this point we know it is static effUser, so for hostLoginUser and credTargetUser we can use effUser.
      //       If that ever changes, and we make calls for dynamic effUser, we would need to do final resolve of effUser.
      hostLoginUser = sysEffUserId;
      credTargetUser = sysEffUserId;
      // Skip check if not LINUX or S3
      if (!SystemType.LINUX.equals(sysType) && !SystemType.S3.equals(sysType)) skipCredCheck = true;

      // static effectiveUser case. Credential must not contain loginUser
      credUtils.checkCredentialForInvalidLoginUser(rUser, system, cred, isStaticEffUser);

      // ---------------- Verify credentials if not skipped
      if (!skipCredCheck)
      {
        // During create, we only verify for static effectiveUser and system default authnMethod, so we pass in the
        //   effectiveUser from request as hostLoginUser and the authnMethod from the system.
        verifiedCred = credUtils.verifyCredentials(rUser, system, cred, hostLoginUser, authnMethod);
        system.setAuthnCredential(verifiedCred);
        // If credential validation failed we do not create the system. Return now.
        if (Boolean.FALSE.equals(verifiedCred.getValidationResult())) return system;
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
    if (SystemType.LINUX.equals(sysType) || SystemType.IRODS.equals(sysType))
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
    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + sysTenant + ":*:" + sysId;

    // We want to always have at least one CredInfo record once system is created.
    // We can then use this record to update hasCredentials for the TSystem before returning it.
    // The initial credInfo record will always be for tapisUser = system owner.
    CredentialInfo credInfo = null;

    // Get SK client now. If we cannot get this rollback not needed.
    // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
    sysUtils.getSKClient(rUser);
    try
    {
      // ------------------- Make Dao call to persist the system -----------------------------------
      itemCreated = dao.createSystem(rUser, system, updateJsonStr, rawData);
      // Now that it is in the DB, it will have a seq id and other attributes populated, so fetch it from db.
      retSystem = dao.getSystem(sysTenant, sysId);

      // ------------------- Add permissions -----------------------------
      // Consider using a notification instead (jira cic-3071)
      // Give owner files service related permission for root directory
      sysUtils.getSKClient(rUser).grantUserPermission(sysTenant, sysOwner, filesPermSpec);

      // ------------------- Store credentials -----------------------------------
      // Store credentials in Security Kernel if cred provided and effectiveUser is static
      if (manageCredentials)
      {
        // Use internal method instead of public API to skip auth and other checks not needed here.
        // This is createSystem, so isStatic is true so credTargetUser and hostLoginUser are the eff user id.
        // Note that a CredInfo record will be created.
        credInfo = credUtils.createCredential(rUser, cred, retSystem, credTargetUser, isStaticEffUser, hostLoginUser,
                                              skipCredCheck, false, op);
      }

      // If no credInfo record yet then create one
      if (credInfo == null)
      {
        // We did not create a credInfo as part of manageCredentials, so create one now to be sure we always have a
        //  CredInfo record associated with the owner for a newly created system.
        // Tapis user for this initial record is system owner, hostLoginUser is resolved effUser and
        //    userLoginMapping is null since no credential was provided.
        credInfo = credUtils.createCredInfoForOwnerAsNeeded(rUser, retSystem, isStaticEffUser, op.name());
      }
    }
    catch (Exception e0)
    {
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      // Log error
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ROLLBACK", rUser, sysId, e0.getMessage());
      log.error(msg);

      // Rollback
      // Remove system from DB
      if (itemCreated) try {dao.hardDeleteSystem(sysTenant, sysId); }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, sysId, "hardDelete", e.getMessage()));}
      // Remove perms
      // Consider using a notification instead (jira cic-3071)
      try { sysUtils.getSKClient(rUser).revokeUserPermission(sysTenant, sysOwner, filesPermSpec);  }
      catch (Exception e) {log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, sysId, "revokePermF1", e.getMessage()));}
      // Remove creds
      if (manageCredentials)
      {
        // Use private internal method instead of public API to skip auth and other checks not needed here.
        // Note that we only manageCredentials for the static case and for the static case credTargetUser=effectiveUserId
        try
        {
          // Remove SK records and CredInfo record. Use sys fetched from DB if possible
          TSystem tmpSys = (retSystem == null) ? system : retSystem;
          credUtils.deleteAllCredentialsForSystem(rUser, tmpSys, op);
        }
        catch (Exception e)
        {
          log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, sysId, "deleteCred", e.getMessage()));
        }
      }
      throw e0;
    }

    // Update the credential with the credential that (optionally) was verified.
    // So caller will know if validation succeeded.
    retSystem.setAuthnCredential(verifiedCred);

    // Determine hasCredentials. credInfo should always be set, but if not log an error and default to false.
    // Most likely reason for an error here is code above has been changed.
    boolean hasCredentials;
    if (credInfo != null)
    {
      hasCredentials = credInfo.hasCredentials();
    }
    else
    {
      hasCredentials = false;
      log.error(LibUtils.getMsgAuth("SYSLIB_CREDINFO_CREATE_ERR", rUser, sysTenant, sysId, sysOwner, isStaticEffUser));
    }

    // Update dynamically computed info and return the fully populated TSystem
    SystemShare systemShare = authUtils.getSystemShareInfo(rUser, sysTenant, sysId);
    retSystem.setIsPublic(systemShare.isPublic());
    retSystem.setSharedWithUsers(systemShare.getUserList());
    retSystem.setIsDynamicEffectiveUser(!isStaticEffUser);
    retSystem.setHasCredentials(hasCredentials);
    return retSystem;
  }

  /**
   * Create a new child system object given a parent systemId and properties for the child system.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param parentId - Parent system id
   * @param childId - Child system id
   * @param rawData - Json used to create the TSystem object - secrets should be scrubbed. Saved in update record.
   * @return Child TSystem
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - system exists OR TSystem in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public TSystem createChildSystem(ResourceRequestUser rUser, String parentId, String childId, String childEffectiveUserId,
                                   String childRootDir, String childOwner, boolean enabled, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    String opName = "createChildSystem";
    TSystem parentSystem = getSystem(rUser, rUser.getOboTenantId(), parentId);
    if (parentSystem == null)
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_PARENT_NOT_FOUND", rUser, opName, parentId, childId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

    if (!parentSystem.isAllowChildren())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_NOT_PERMITTED", rUser, parentId);
      log.warn(msg);
      throw new IllegalStateException(msg);
    }

    if(StringUtils.isBlank(childId)) {
      childId = parentSystem.getId() + "-" + rUser.getOboUserId();
    }

    // Check if system already exists
    if (dao.checkForSystem(parentSystem.getTenant(), childId, true))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, childId);
      log.warn(msg);
      throw new IllegalStateException(msg);
    }

    if (StringUtils.isBlank(childOwner)) { childOwner = rUser.getOboUserId(); }

    TSystem childSystem = new TSystem(parentSystem, childId, childEffectiveUserId, childRootDir, childOwner, enabled);

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
    String methodName = "patchSystem";
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (patchSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract various names for convenience
    String oboTenant = rUser.getOboTenantId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(systemId) || StringUtils.isBlank(rawData)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, systemId));
    }

    // System must already exist and not be deleted
    checkForSysWithThrow(rUser, oboTenant, systemId, false);

    // if the patch system contains a request to set allowChildren to false, only allow
    // the change if there are no children.
    Boolean changeAllowChildren = patchSystem.getAllowChildren();
    if (BooleanUtils.isFalse(changeAllowChildren)) {
      if (dao.hasChildren(rUser.getOboTenantId(), systemId)) {
        String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_HAS_CHILD_ERROR", rUser, systemId);
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
    String origEffUser = origTSystem.getEffectiveUserId();
    AuthnMethod origAuthnMethod = origTSystem.getDefaultAuthnMethod();

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, systemId, origTSystem.getOwner());

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

    // Update credInfo records if necessary, i.e. if defaultAuthnMethod or effUser have changed.
    credUtils.updateCredInfoRecordsForSystem(rUser, patchedTSystem, origAuthnMethod, origEffUser, methodName);
  }

  /**
   * Update all updatable attributes of a system object given a TSystem and the text used to create the TSystem.
   * <p>
   * NOTE that if credentials are provided and checked, and credentials are invalid,
   *    a system object is still returned. Caller must check Credential.getValidationResult()
   * <p>
   * Incoming TSystem must contain the tenantId and systemId.
   * Secrets in the text should be masked.
   * Attributes that cannot be updated and so will be looked up and filled in:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, canExec, effectiveUserId
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
    String methodName = "putSystem";
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (putSystem == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    // Extract some attributes for convenience and clarity
    String sysTenant = putSystem.getTenant();
    String sysId = putSystem.getId();

    // ---------------------------- Check inputs ------------------------------------
    if (StringUtils.isBlank(sysTenant) || StringUtils.isBlank(sysId) || StringUtils.isBlank(rawData))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, sysId));
    }

    // System must already exist and not be deleted
    checkForSysWithThrow(rUser, sysTenant, sysId, false);

    // Fill in defaults
    putSystem.setDefaults();

    // Retrieve the system being updated and create fully populated TSystem with updated attributes
    TSystem origTSystem = dao.getSystem(sysTenant, sysId);
    String origEffUserId = origTSystem.getEffectiveUserId();
    AuthnMethod origAuthnMethod = origTSystem.getDefaultAuthnMethod();

    // Set flag indicating if effectiveUserId is static
    boolean origIsStaticEffUser = !origEffUserId.equals(APIUSERID_VAR);

    // Note that effectiveUserId and authnCredential are ignored for PUT, so we do not need to
    // deal with updating credentials.

    // Error if the system we are replacing had a parentId (i.e. - PUT not allowed for a child system) or if
    // the incoming request has a parentId set (i.e. trying to change the system to a child system)
    if (!StringUtils.isBlank(origTSystem.getParentId()))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_PUT_NOT_ALLOWED", rUser, sysId);
      throw new IllegalArgumentException(msg);
    }

    TSystem updatedTSystem = createUpdatedTSystem(origTSystem, putSystem);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, sysId, origTSystem.getOwner());

    // ---------------- Check constraints on TSystem attributes ------------------------
    validateTSystem(rUser, updatedTSystem, false);

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

    // Update credInfo records if necessary, i.e. if defaultAuthnMethod has changed.
    // NOTE: Put does not allow for changing effUser, but this method will handle both, just in clase that ever changes.
    credUtils.updateCredInfoRecordsForSystem(rUser, updatedTSystem, origAuthnMethod, origEffUserId, methodName);

    // Update dynamically computed info.
    SystemShare systemShare = authUtils.getSystemShareInfo(rUser, sysTenant, sysId);
    putSystem.setIsPublic(systemShare.isPublic());
    putSystem.setSharedWithUsers(systemShare.getUserList());
    putSystem.setIsDynamicEffectiveUser(!origIsStaticEffUser);
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
   *   - Update deleted to true for the system
   * NOTE: No other actions taken. Credentials and SK permissions are not removed.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system to delete
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
    checkForSysWithThrow(rUser, rUser.getOboTenantId(), systemId, true);
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, true);
    // We just checked for system, so it should never be null. But just in case.
    if (system == null) return 0;

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    // Reject the request if the system has children
    if (dao.hasChildren(rUser.getOboTenantId(), systemId)) {
      String msg = LibUtils.getMsg("SYSLIB_CHILD_HAS_CHILD_ERROR", rUser, systemId);
      log.warn(msg);
      throw new IllegalStateException(msg);
    }

    // Update deleted attribute for the system
    return updateDeleted(rUser, systemId, op);
  }

  /**
   * Undelete a system
   *  - Add file permissions for owner
   *  - Update deleted to false for the system
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @return Number of items updated
   *
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  @Override
  public int undeleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, IllegalArgumentException, TapisClientException
  {
    SystemOperation op = SystemOperation.undelete;
    // ---------------------------- Check inputs ------------------------------------
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));

    String oboTenant = rUser.getOboTenantId();

    // System must exist
    checkForSysWithThrow(rUser, oboTenant, systemId, true);
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId, true);
    // We just checked for system, so it should never be null. But just in case.
    if (system == null) return 0;

    // Get owner, if not found it is an error
    String owner = system.getOwner();
    if (StringUtils.isBlank(owner))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_OP_NO_OWNER", rUser, systemId, op.name());
      log.error(msg);
      throw new TapisException(msg);
    }
    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, systemId, owner);

    // if this is a child system, make sure that the parent hasn't been deleted, and that
    // the parent still allows children
    if (isChildSystem(system)) {
      boolean okToUndeleteChild = false;
      TSystem parentSystem = dao.getSystem(rUser.getOboTenantId(), system.getParentId(), false);
      if (parentSystem != null) {
        if(parentSystem.isAllowChildren()) {
          okToUndeleteChild = true;
        }
      }

      if (!okToUndeleteChild) {
        String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_ALLOW_CONFLICT_ERROR", rUser, op.name(), systemId);
        log.warn(msg);
        throw new IllegalStateException(msg);
      }
    }

    // Consider using a notification instead (jira cic-3071)
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    // Consider using a notification instead (jira cic-3071)
    // Give owner files service related permission for root directory
    sysUtils.getSKClient(rUser).grantUserPermission(oboTenant, owner, filesPermSpec);

    // Update deleted attribute for system
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

    // We will be updating the CredInfo table, so make sure we are the only ones working on it
    synchronized (CredUtils.class)
    {
      // System must already exist and not be deleted
      checkForSysWithThrow(rUser, oboTenant, systemId, false);

      // Retrieve system. We will need it for a few things.
      TSystem sys = dao.getSystem(oboTenant, systemId);
      String oldOwnerName = sys.getOwner();
      boolean isStaticEffUser = !sys.isDynamicEffectiveUser();

      // ------------------------- Check authorization -------------------------
      authUtils.checkAuthOwnerKnown(rUser, op, systemId, oldOwnerName);

      // If new owner same as old owner then this is a no-op
      if (newOwnerName.equals(oldOwnerName)) return 0;

      // ----------------- Make all updates --------------------
      // Changes not in single DB transaction.
      // Use try/catch to roll back any changes in case of failure.
      // Get SK client now. If we cannot get this rollback not needed.
      // Note that we still need to call getSKClient each time because it refreshes the svc jwt as needed.
      sysUtils.getSKClient(rUser);
      String systemsPermSpec = getPermSpecAllStr(oboTenant, systemId);
      // Consider using a notification instead (jira cic-3071)
      String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
      try
      {
        // ------------------- Make Dao call to update the system owner -----------------------------------
        dao.updateSystemOwner(rUser, systemId, oldOwnerName, newOwnerName);
        sys.setOwner(newOwnerName);

        // Consider using a notification instead (jira cic-3071)
        // Give new owner files service related permission for root directory
        sysUtils.getSKClient(rUser).grantUserPermission(oboTenant, newOwnerName, filesPermSpec);
        // Remove permissions from old owner
        sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, oldOwnerName, filesPermSpec);

        // Create credInfo record for new owner and if static effUser remove old credential
        credUtils.createCredInfoForOwnerAsNeeded(rUser, sys, isStaticEffUser, op.name());
        if (isStaticEffUser) credUtils.deleteCredential(rUser, sys, sys.getEffectiveUserId(), isStaticEffUser, op);

        // Get a complete and succinct description of the update.
        String changeDescription = LibUtils.getChangeDescriptionUpdateOwner(systemId, oldOwnerName, newOwnerName);
        // Create a record of the update
        dao.addUpdateRecord(rUser, systemId, op, changeDescription, null);
      }
      catch (Exception e0)
      {
        // Something went wrong. Attempt to undo all changes and then re-throw the exception
        try {dao.updateSystemOwner(rUser, systemId, newOwnerName, oldOwnerName);} catch (Exception e)
        {
          log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "updateOwner", e.getMessage()));
        }
        // Consider using a notification instead(jira cic-3071)
        try {sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, newOwnerName, filesPermSpec);}
        catch (Exception e)
        {
          log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePermF1", e.getMessage()));
        }
        try {sysUtils.getSKClient(rUser).grantUserPermission(oboTenant, oldOwnerName, filesPermSpec);}
        catch (Exception e)
        {
          log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "grantPermF1", e.getMessage()));
        }
        throw e0;
      }
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
    checkForSysWithThrow(rUser, oboTenant, childSystemId, false);
    TSystem childSystem = dao.getSystem(oboTenant, childSystemId, false);
    // We just checked for system, so it should never be null. But just in case.
    if (childSystem == null) return 0;

    // Get parent's Id
    String parentSystemId = childSystem.getParentId();
    if (parentSystemId == null) {
      // if there is no parent id, we are done.  This is already not a child system.
      return 1;
    }

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, childSystemId, childSystem.getOwner());

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
      checkForSysWithThrow(rUser, oboTenant, childSystemId, false);
      TSystem childSystem = dao.getSystem(oboTenant, childSystemId, false);
      // We just checked for system, so it should never be null. But just in case.
      if (childSystem == null) return 0;
      if(!parentId.equals(childSystem.getParentId()))
      {
        String msg = LibUtils.getMsgAuth("SYSLIB_CHILD_CHILD_NOT_FOUND", rUser, parentId, childSystemId);
        log.info(msg);
        throw new NotFoundException(msg);
      }
    }

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, parentId);

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
    checkForSysWithThrow(rUser, oboTenant, parentId, false);
    TSystem parentSystem = dao.getSystem(oboTenant, parentId, false);
    // We just checked for system, so it should never be null. But just in case.
    if (parentSystem == null) return 0;

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerKnown(rUser, op, parentId, parentSystem.getOwner());

    // ------------------- Make Dao call to unlink the system -----------------------------------
    return dao.removeParentIdFromAllChildren(rUser, oboTenant, parentId);
  }

  /**
   * Hard delete a system given the system name.
   *   - remove permissions associated with the system.
   *   - remove shareInfo associated with the system.
   *   - remove all CredInfo records and SK secrets associated with the system
   *   - remove system record from data store
   * NOTE: This is package-private. Only test code should ever use it.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tenant - Tenant containing resources.
   * @param systemId - name of system
   * @return Number of items deleted
   * @throws TapisException - for Tapis related exceptions
   * @throws TapisClientException - for Tapis related exceptions
   */
  int hardDeleteSystem(ResourceRequestUser rUser, String tenant, String systemId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.hardDelete;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(tenant) ||  StringUtils.isBlank(systemId))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT", rUser));

    // If system does not exist then nothing to do, 0 changes
    TSystem system = dao.getSystem(tenant, systemId, true);
    if (system == null) return 0;

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    // Resolve effectiveUserId
    String resolvedEffectiveUserId = sysUtils.resolveEffectiveUserId(system, rUser.getOboUserId());
    // Remove permissions associated with the system
    authUtils.revokeAllSKPermissions(rUser, system, resolvedEffectiveUserId);
    // Remove shareInfo associated with the system
    authUtils.deleteAllShareInfo(rUser, system);
    // Delete all Credentials and CredInfo records associated with the system.
    credUtils.deleteAllCredentialsForSystem(rUser, system, op);

    // Delete the system from the DB
    return dao.hardDeleteSystem(tenant, systemId);
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
      authUtils.checkAuthOwnerUnkown(rUser, op, systemId);
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
    checkForSysWithThrow(rUser, oboTenant, systemId, false);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);
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
   * @return populated instance of a TSystem or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public TSystem getSystem(ResourceRequestUser rUser, String systemId, AuthnMethod authnMethod, boolean requireExecPerm,
                           boolean returnCreds, String impersonationId, String sharedAppCtxGrantor,
                           String resourceTenant, boolean fetchShareInfo)
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
    if (!StringUtils.isBlank(impersonationId)) authUtils.checkImpersonateUserAllowed(rUser, op, systemId, impersonationId, resTenant);
    // If resourceTenant set confirm it is allowed. Only allowed for certain Tapis services.
    if (!StringUtils.isBlank(resourceTenant)) AuthUtils.checkResourceTenantAllowed(rUser, op, systemId, resourceTenant);
    // If sharedAppCtx set confirm it is allowed. Only allowed for certain Tapis services.
    if (!StringUtils.isBlank(sharedAppCtxGrantor)) AuthUtils.checkSharedAppCtxAllowed(rUser, op, systemId);

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
    boolean isStaticEffUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);
    String resolvedEffectiveUserId = sysUtils.resolveEffectiveUserId(system, oboOrImpersonatedUser);

    // ------------------------- Check authorization -------------------------
    // getSystem auth check:
    // Call checkAuth (this can throw ForbiddenException)
    //   - always allow a service calling as itself to read/execute a system.
    //   - if svc not calling as itself do the normal checks using oboUserOrImpersonationId.
    // If owner is making the request we can skip this check.
    if (!isOwner)
    {
      authUtils.checkAuth(rUser, op, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);
    }

    // If caller asks for credentials, explicitly check auth now
    // That way we can call private getCredential and not have overhead of getUserCredential().
    if (returnCreds) authUtils.checkAuth(rUser, SystemOperation.getCred, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);

    // If flag is set to also require EXECUTE perm then make explicit auth call to make sure user has exec perm
    if (requireExecPerm)
    {
      authUtils.checkAuth(rUser, SystemOperation.execute, systemId, owner, nullTargetUser, nullPermSet, impersonationId, sharedAppCtxGrantor);
    }

    // If flag is set to also require EXECUTE perm then system must support execute
    if (requireExecPerm && !system.getCanExec())
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_NOTEXEC", rUser, systemId, op.name());
      log.warn(msg);
      throw new ForbiddenException(msg);
    }

    system.setEffectiveUserId(resolvedEffectiveUserId);

    // If credentials are requested, fetch them now.
    // Note that resolved effectiveUserId not used to look up credentials.
    // If effUsr is static then secrets stored using the "static" path in SK and static string used to build the path.
    // If effUsr is dynamic then secrets stored using the "dynamic" path in SK and a Tapis user
    //    (oboUser or impersonationId) used to build the path.
    if (returnCreds)
    {
      AuthnMethod tmpAuthnMethod = system.getDefaultAuthnMethod();
      // If authnMethod specified then use it instead of default authn method defined for the system.
      if (authnMethod != null) tmpAuthnMethod = authnMethod;
      // Determine credTargetUser for fetching credential.
      //   If static use effectiveUserId, else use oboOrImpersonatedUser
      String credTargetUser;
      if (isStaticEffUser)
        credTargetUser = system.getEffectiveUserId();
      else
        credTargetUser = oboOrImpersonatedUser;
      // Use internal method instead of public API to skip auth and other checks not needed here.
      Credential cred = credUtils.getCredential(rUser, system, credTargetUser, tmpAuthnMethod, isStaticEffUser,
                                                resourceTenant);
      system.setAuthnCredential(cred);
    }

    // Update dynamically computed info.
    // Fetch share info only if requested by caller
    if (fetchShareInfo)
    {
      SystemShare systemShare = authUtils.getSystemShareInfo(rUser, system.getTenant(), systemId);
      system.setIsPublic(systemShare.isPublic());
      system.setSharedWithUsers(systemShare.getUserList());
    }
    // Update isDynamic and hasCredentials
    system.setIsDynamicEffectiveUser(!isStaticEffUser);
    system.setHasCredentials(determineHasCredentials(rUser, system, oboOrImpersonatedUser, isStaticEffUser));
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
    if (!StringUtils.isBlank(impersonationId)) authUtils.checkImpersonateUserAllowed(rUser, op, null, impersonationId, tenant);

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
    if (allItems) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboOrImpersonatedUser, false);
    else if (publicOnly) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboOrImpersonatedUser, true);

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
   * @param filterByHasCredentials - whether to filter by hasCredentials = true, false or null
   * @param fetchShareInfo - indicates if share info should be included in result
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, resolving effectiveUserId
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList, int limit,
                                  List<OrderBy> orderByList, int skip, String startAfter, boolean includeDeleted,
                                  String listType, Boolean filterByHasCredentials, boolean fetchShareInfo,
                                  String impersonationId)
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
    if (!StringUtils.isBlank(impersonationId)) authUtils.checkImpersonateUserAllowed(rUser, op, null, impersonationId, tenant);

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
    if (allItems) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboOrImpersonatedUser, false);
    else if (publicOnly) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboOrImpersonatedUser, true);

    // Get all allowed systems matching the search conditions
    // If filtering by hasCredentials, turn off limit temporarily.
    //   Unfortunately cannot do it as part of SQL. We will need to get all of them and then limit later.
    int tmpLimit = limit;
    if (Boolean.TRUE.equals(filterByHasCredentials)) tmpLimit = -1;
    List<TSystem> systems = dao.getSystems(rUser, oboOrImpersonatedUser, verifiedSearchList,
                                           null,  tmpLimit, orderByList, skip, startAfter,
                                           includeDeleted, listTypeEnum, viewableIDs, sharedIDs);

    // Do final filtering and setting of any dynamic attributes
    // The return list will be either the full list returned by the dao call or new list containing only
    //   records filtered by hasCredentials.
    List<TSystem> retSystems = getSystemsFinal(rUser, systems, fetchShareInfo, filterByHasCredentials, limit, oboOrImpersonatedUser);
    return retSystems;
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
   * @param filterByHasCredentials - whether to filter by hasCredentials = true, false or null
   * @param fetchShareInfo - indicates if share info should be included in result
   * @return List of TSystem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser, String sqlSearchStr, int limit,
                                                   List<OrderBy> orderByList, int skip, String startAfter,
                                                   boolean includeDeleted, String listType,
                                                   Boolean filterByHasCredentials, boolean fetchShareInfo)
          throws TapisException, TapisClientException
  {
    String oboUser = rUser.getOboUserId();
    // If search string is empty delegate to getSystems()
    Boolean filterByHasCredentialsTmp=null;
    if (StringUtils.isBlank(sqlSearchStr)) return getSystems(rUser, null, limit, orderByList, skip, startAfter,
                                                             includeDeleted, listType, filterByHasCredentialsTmp,
                                                             fetchShareInfo, nullImpersonationId);

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
    if (allItems) viewableIDs = getViewableSystemIDs(rUser, oboUser);

    // If needed, get IDs for items shared with the requester or only shared publicly.
    Set<String> sharedIDs = new HashSet<>();
    if (allItems) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboUser, false);
    else if (publicOnly) sharedIDs = authUtils.getSharedSystemIDs(rUser, oboUser, true);

    // Get all allowed systems matching the search conditions
    // If filtering by hasCredentials, turn off limit temporarily.
    //   Unfortunately cannot do it as part of SQL. We will need to get all of them and then limit later.
    int tmpLimit = limit;
    if (Boolean.TRUE.equals(filterByHasCredentials)) tmpLimit = -1;
    List<TSystem> systems = dao.getSystems(rUser, oboUser, null, searchAST, tmpLimit, orderByList,
                                           skip, startAfter, includeDeleted, listTypeEnum, viewableIDs, sharedIDs);
    // Do final filtering and setting of any dynamic attributes
    // The return list will be either the full list returned by the dao call or new list containing only
    //   records filtered by hasCredentials.
    List<TSystem> retSystems = getSystemsFinal(rUser, systems, fetchShareInfo, filterByHasCredentials, limit, oboUser);
    return retSystems;
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
  public List<TSystem> getSystemsSatisfyingConstraints(ResourceRequestUser rUser, String matchStr,
                                                       Boolean filterByHasCredentials, boolean fetchShareInfo)
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

    // Do final filtering and setting of any dynamic attributes
    // The return list will be either the full list returned by the dao call or new list containing only
    //   records filtered by hasCredentials.
    List<TSystem> retSystems = getSystemsFinal(rUser, systems, fetchShareInfo, false, -1, rUser.getOboUserId());
    return retSystems;
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
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    return dao.getSystemOwner(rUser.getOboTenantId(), systemId);
  }

  /*
   * Given a child system id get the parent system id
   */
  @Override
  public String getParentId(ResourceRequestUser rUser, String systemId) throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(systemId)) {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_SYSTEM", rUser));
    }

    String oboTenant = rUser.getOboTenantId();

    // Resource must exist and not be deleted
    checkForSysWithThrow(rUser, oboTenant, systemId, false);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    return dao.getParent(oboTenant, systemId);
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

    // If system does not exist or has been deleted then throw an exception
    checkForSysWithThrow(rUser, rUser.getOboTenantId(), systemId, false);

    // NOTE: Previously we did a check here to see if owner is trying to update permissions for themselves.
    // If so we threw an exception because this would be confusing since owner always has full permissions.
    // Due to a request (github issue #47) to change the behavior of changeSystemOwner, we now allow owner to
    // grant/revoke permissions for themselves.
    // See previous code versions for implementation of checkForOwnerPermUpdate()

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Grant of MODIFY implies grant of READ
    if (permissions.contains(Permission.MODIFY)) permissions.add(Permission.READ);

    // Use utility method to do remaining work
    authUtils.grantPermissions(rUser, systemId, targetUser, permissions, op, rawData);
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
    authUtils.checkAuth(rUser, op, systemId, nullOwner, targetUser, permissions);

    // Check inputs. If anything null or empty throw an exception
    if (permissions == null || permissions.isEmpty())
    {
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    }

    // Revoke of READ implies revoke of MODIFY
    if (permissions.contains(Permission.READ)) permissions.add(Permission.MODIFY);

    // Use utility method to do remaining work
    return authUtils.revokePermissions(rUser, systemId, targetUser, permissions, op, rawData);
  }

  /**
   * Get list of system permissions for a user
   * NOTE: This retrieves permissions from all roles.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - name of system
   * @param targetUser - Target user for operation
   * @return Set of permissions
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
    checkForSysWithThrow(rUser, rUser.getOboTenantId(), systemId, false);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuth(rUser, op, systemId, nullOwner, targetUser, nullPermSet);

    // Use Security Kernel client to check for each permission in the enum list
    return authUtils.getUserPermSet(rUser, targetUser, rUser.getOboTenantId(), systemId);
  }

  // -----------------------------------------------------------------------
  // ---------------------------- Sharing ------------------------------
  // -----------------------------------------------------------------------

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

    authUtils.checkAuth(rUser, op, systemId, system.getOwner(), nullTargetUser, nullPermSet);

    // Get the SystemShare object
    return authUtils.getSystemShareInfo(rUser, system.getTenant(), systemId);
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
    // Use utility method to do the work
    authUtils.updateUserShares(rUser, OP_SHARE, systemId, systemShare, false);
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
    // Use utility method to do the work
    authUtils.updateUserShares(rUser, OP_UNSHARE, systemId, systemShare, false);
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
    // Use utility method to do the work
    authUtils.updateUserShares(rUser, OP_SHARE, systemId, nullSystemShare, true);
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
    // Use utility method to do the work
    authUtils.updateUserShares(rUser, OP_UNSHARE, systemId, nullSystemShare, true);
  }

  // -----------------------------------------------------------------------
  // ------------------------- Misc -------------------------------------
  // -----------------------------------------------------------------------

  /**
   * Get System history records for the System ID specified
   */
  @Override
  public List<SystemHistoryItem> getSystemHistory(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException
  {
    SystemOperation op = SystemOperation.read;
    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, op, systemId);
    // ----------------- Retrieve system updates information (system history) --------------------
    List<SystemHistoryItem> systemHistory = dao.getSystemHistory(rUser.getOboTenantId(), systemId);
    return systemHistory;
  }

  // ************************************************************************
  // **************************  Package-Private Methods  *******************
  // ************************************************************************

  /**
   * Use dao to see if system exists. If not throw NOT_FOUND exception.
   * @param rUser - user making the request
   * @param resourceTenantId - tenant
   * @param sysId - system id
   * @param includeDeleted - indicates if deleted records should be included
   */
  private void checkForSysWithThrow(ResourceRequestUser rUser, String resourceTenantId, String sysId,
                                    boolean includeDeleted)
          throws TapisException
  {
    if (!dao.checkForSystem(resourceTenantId, sysId, includeDeleted))
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, sysId);
      log.info(msg);
      throw new NotFoundException(msg);
    }
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

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /*
   * Determine hasCredentials attribute for a system.
   */
  private boolean determineHasCredentials(ResourceRequestUser rUser, TSystem sys, String oboOrImpersonatedUser,
                                          boolean isStaticEffUser)
  {
    CredentialInfo credInfo = credUtils.getCredInfo(rUser, sys, oboOrImpersonatedUser, isStaticEffUser);
    if (credInfo == null) return false;
    else return credInfo.hasCredentials();
  }

  /*
   * Do final filtering and setting of any dynamic attributes
   * We compute hasCredentials here for each system so if we are filtering by credentials we do it here
   *   and also check limit here if needed.
   */
  private List<TSystem> getSystemsFinal(ResourceRequestUser rUser, List<TSystem> systems, boolean fetchShareInfo,
                                        Boolean filterByHasCredentials, int limit, String oboOrImpersonatedUser)
        throws TapisException, TapisClientException
  {
    // Start a new list for final result. If filtering by credentials we need to do it here and possibly limit
    List<TSystem> retSystems = new ArrayList<>();

    // Loop over full list of systems computing dynamic attributes and possibly filtering and limiting as requested
    int counter = 0;
    for (TSystem sys : systems)
    {
      boolean isStaticEffUser = !sys.getEffectiveUserId().equals(APIUSERID_VAR);
      // NOTE: We could determine hasCredentials and fill in CredInfo more efficiently via
      //       using SQL to join with table systems_cred_info, but building the SQL query is already very complex.
      //       And we have to fetch share info anyway, so for now brute force it.
      // Determine hasCredentials
      sys.setHasCredentials(determineHasCredentials(rUser, sys, oboOrImpersonatedUser, isStaticEffUser));

      // If filtering by hasCredentials there is some special handling.
      if (filterByHasCredentials != null)
      {
        // If not including then simply continue now to skip the record.
        if (!filterByHasCredentials.equals(sys.hasCredentials())) continue;
        // If we have passed the limit then we are done, break out of loop
        counter++;
        if (limit >= 0 && counter > limit) break;
      }

      // Update other dynamically computed attributes and resolve effUser as needed.
      // Fetch share info only if requested by caller
      if (fetchShareInfo)
      {
        SystemShare systemShare = authUtils.getSystemShareInfo(rUser, sys.getTenant(), sys.getId());
        sys.setIsPublic(systemShare.isPublic());
        sys.setSharedWithUsers(systemShare.getUserList());
      }
      sys.setIsDynamicEffectiveUser(!isStaticEffUser);
      sys.setEffectiveUserId(sysUtils.resolveEffectiveUserId(sys, oboOrImpersonatedUser));

      // Include the system in final result
      retSystems.add(sys);
    }
    return retSystems;
  }

  /*
   * Basic getSystem with default options, share info and credentials are NOT fetched.
   * NOTE: dynamic properties and effectiveUserId are resolved, so this call is not always appropriate within
   *       service code.
   */
  private TSystem getSystem(ResourceRequestUser rUser, String tenant, String sysId)
        throws TapisException, TapisClientException
  {
    String resourceTenant = (rUser.getOboTenantId().equals(tenant)) ? nullResourceTenant : tenant;
    return getSystem(rUser, sysId, nullAuthnMethod, false, false, nullImpersonationId, nullSharedAppCtx, resourceTenant, false);
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
    checkForSysWithThrow(rUser, oboTenant, systemId, false);

    // ------------------------- Check authorization -------------------------
    authUtils.checkAuthOwnerUnkown(rUser, sysOp, systemId);

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
   * Check for reserved names.
   * Endpoints defined lead to certain names that are not valid.
   * Invalid names: healthcheck, readycheck, search
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param id - the id to check
   * @throws IllegalStateException - if attempt to create a resource with a reserved name
   */
  private static void checkReservedIds(ResourceRequestUser rUser, String id) throws IllegalStateException
  {
    if (TSystem.RESERVED_ID_SET.contains(id.toUpperCase()))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CREATE_RESERVED", rUser, id);
      log.warn(msg);
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
        TSystem dtnSystem = getSystem(rUser, rUser.getOboTenantId(), tSystem1.getDtnSystemId());
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
      String allErrors = SysUtils.getListOfErrors(rUser, tSystem1.getId(), errMessages);
      log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }

  /**
   * Resolve HOST_EVAL in rootDir by connecting to the host
   * Much of this code copied from tapis-job repo, MacroResolver.replaceHostEval.
   *
   * @param system - the system
   * @return Resolved rootDir
   */
  private static String resolveRootDirHostEval(ResourceRequestUser rUser, TSystem system) throws TapisException
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
      log.warn(msg);
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
      log.warn(msg);
      throw new IllegalArgumentException(msg);
    }

    // Extract the variable and an optional default value, the latter of which can be null.
    String varName = m.group(1);
    String defaultValue = m.group(3);

    // We will need to make an ssh connection to the host.
    // Easiest way to do that is to use TapisRunCommand, which requires a client base TapisSystem object.
    TapisSystem tapisSystem = createClientTapisSystemFromTSystem(system);
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
        log.warn(msg);
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
  private static TapisSystem createClientTapisSystemFromTSystem(TSystem s)
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
    tapisSystem.setAuthnCredential(CredUtils.buildAuthnCred(cred, s.getDefaultAuthnMethod()));
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
    tapisSystem.setHasCredentials(s.hasCredentials());
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
//    tapisSystem.setCreated(s.getCreated());
//    tapisSystem.setUpdated(s.getUpdated());
    tapisSystem.setUuid(s.getUuid());
    tapisSystem.setDeleted(s.isDeleted());
    return tapisSystem;
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
    var userPerms = sysUtils.getSKClient(rUser).getUserPerms(rUser.getOboTenantId(), oboUser, implies, impliedBy);

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
          authUtils.removeOrphanedSKPerms(rUser, permFields[3], rUser.getOboTenantId());
        }
      }
    }
    return systemIDs;
  }

  /**
   * Create an updated TSystem based on the system created from a PUT request.
   * Attributes that cannot be updated and must be filled in from the original system:
   *   tenant, id, systemType, owner, enabled, bucketName, rootDir, canExec, effectiveUserId
   */
  private static TSystem createUpdatedTSystem(TSystem origSys, TSystem putSys)
  {
    // Rather than exposing otherwise unnecessary setters we use a special constructor.
    TSystem updatedSys = new TSystem(putSys, origSys.getTenant(), origSys.getId(), origSys.getSystemType(),
                                     origSys.getCanExec());
    updatedSys.setOwner(origSys.getOwner());
    updatedSys.setEnabled(origSys.isEnabled());
    updatedSys.setBucketName(origSys.getBucketName());
    updatedSys.setRootDir(origSys.getRootDir());
    updatedSys.setEffectiveUserId(origSys.getEffectiveUserId());
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
  private static TSystem createPatchedTSystem(TSystem o, PatchSystem p)
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
}
