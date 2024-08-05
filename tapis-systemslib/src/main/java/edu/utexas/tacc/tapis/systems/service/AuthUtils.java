package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqShareResource;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;
import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareGetSharesParms;
import edu.utexas.tacc.tapis.security.client.model.SKShareHasPrivilegeParms;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemShare;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.model.TSystem.*;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.*;

/*
   Utility class containing Tapis authentication (permissions and sharing)
   related methods needed by the service implementation.
 */
public class AuthUtils
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(AuthUtils.class);

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

  // Lists of services allowed to perform certain restricted functionality:
  //     Retrieve credentials, impersonate user, set shared context, impersonate tenant
  private static final Set<String> SVCLIST_GETCRED = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(FILES_SERVICE, APPS_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_SHAREDAPPCTX = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));
  private static final Set<String> SVCLIST_RESOURCETENANT = new HashSet<>(Set.of(FILES_SERVICE, JOBS_SERVICE));

  // Named and typed null values to make it clear what is being passed in to a method
  private static final String nullOwner = null;
  private static final String nullImpersonationId = null;
  private static final String nullSharedAppCtx = null;
  private static final String nullTargetUser = null;
  private static final Set<Permission> nullPermSet = null;

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

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /* **************************************************************************** */
  /*                                Package-Private Methods                       */
  /* **************************************************************************** */

  /*
   * Check for case when owner is not known and no need for impersonationId, targetUser or perms
   */
  void checkAuthOwnerUnkown(ResourceRequestUser rUser, SystemOperation op, String systemId)
          throws TapisException, TapisClientException
  {
    checkAuth(rUser, op, systemId, nullOwner, nullTargetUser, nullPermSet, nullImpersonationId, nullSharedAppCtx);
  }

  /*
   * Check for case when owner is known and no need for impersonationId, targetUser or perms
   */
  void checkAuthOwnerKnown(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner)
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
  void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
                         String targetUser, Set<Permission> perms)
          throws TapisException, TapisClientException
  {
    checkAuth(rUser, op, systemId, owner, targetUser, perms, nullImpersonationId, nullSharedAppCtx);
  }

  /**
   * Standard authorization check using all arguments.
   * Check is different for service and user requests.
   * <p>
   * A check should be made for system existence before calling this method.
   * If no owner is passed in and one cannot be found then an error is logged and authorization is denied.
   * <p>
   * Auth check:
   *  - always allow read, execute, getPerms for a service calling as itself.
   *  - if svc not calling as itself do the normal checks using oboUserOrImpersonationId.
   *  - Note that if svc request and no special cases apply then final standard user request type check is done.
   * <p>
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
  void checkAuth(ResourceRequestUser rUser, SystemOperation op, String systemId, String owner,
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
   * Determine all systems that are shared with a user.
   */
  Set<String> getSharedSystemIDs(ResourceRequestUser rUser, String oboUser, boolean publicOnly)
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
    var skShares = sysUtils.getSKClient(rUser).getShares(skParms);
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
   * Confirm that caller is allowed to impersonate a Tapis user.
   * Must be a service request from a service allowed to impersonate
   * impersonationId and resourceTenant used for logging only.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  void checkImpersonateUserAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId,
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
    // Log warning and deny authorization
    String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_IMPERSONATE", rUser, systemId, op.name(), impersonationId, resourceTenant);
    log.warn(msg);
    throw new ForbiddenException(msg);
  }

  /**
   * Confirm that caller is allowed to set resourceTenant
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  static void checkResourceTenantAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId,
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
    // Log warning and deny authorization
    String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_RESOURCETENANT", rUser, systemId, op.name(), resourceTenant);
    log.warn(msg);
    throw new ForbiddenException(msg);
  }

  /**
   * Confirm that caller is allowed to set sharedAppCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param op - operation name
   * @param systemId - name of the system
   */
  static void checkSharedAppCtxAllowed(ResourceRequestUser rUser, SystemOperation op, String systemId)
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    // If a service request and service is in the allowed list then log message and allow.
    if (rUser.isServiceRequest() && SVCLIST_SHAREDAPPCTX.contains(svcName))
    {
      // An allowed service is setting shared context, log message and allow
      log.info(LibUtils.getMsgAuth("SYSLIB_AUTH_SHAREDAPPCTX", rUser, systemId, op.name()));
      return;
    }
    // Log warning and deny authorization
    String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_SHAREDAPPCTX", rUser, systemId, op.name());
    log.warn(msg);
    throw new ForbiddenException(msg);
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
  void checkAuthForProfile(ResourceRequestUser rUser, SchedulerProfile.SchedulerProfileOperation op, String name, String owner)
          throws TapisException, TapisClientException
  {
    // Anyone can read, including all services
    if (op == SchedulerProfile.SchedulerProfileOperation.read) return;

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
    String msg = LibUtils.getMsgAuth("SYSLIB_PRF_UNAUTH", rUser, name, op.name());
    log.info(msg);
    throw new ForbiddenException(msg);
  }

  /**
   * Check to see if the oboUser has the admin role in the obo tenant
   */
  boolean hasAdminRole(ResourceRequestUser rUser) throws TapisException, TapisClientException
  {
    return sysUtils.getSKClient(rUser).isAdmin(rUser.getOboTenantId(), rUser.getOboUserId());
  }

  /**
   * Retrieve set of user permissions given user, tenant, system id
   * @param userName - name of user
   * @param oboTenant - name of tenant associated with resource
   * @param systemId - Id of resource
   * @return - Set of Permissions for the user
   */
  Set<Permission> getUserPermSet(ResourceRequestUser rUser, String userName, String oboTenant, String systemId)
          throws TapisClientException, TapisException
  {
    var userPerms = new HashSet<Permission>();
    for (Permission perm : Permission.values())
    {
      String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, perm.name(), systemId);
      if (sysUtils.getSKClient(rUser).isPermitted(oboTenant, userName, permSpec)) userPerms.add(perm);
    }
    return userPerms;
  }

  /**
   * Create a permSpec for all permissions
   * @return - permSpec entry for all permissions
   */
  static String getPermSpecAllStr(String oboTenant, String systemId)
  {
    return String.format(PERM_SPEC_TEMPLATE, oboTenant, "*", systemId);
  }

  /**
   * Remove all SK permissions associated with given system ID, tenant. System does not need to exist.
   * Used to clean up orphaned permissions.
   */
  void removeOrphanedSKPerms(ResourceRequestUser rUser, String sysId, String tenant)
          throws TapisException, TapisClientException
  {
    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, tenant, "%", sysId);
    var userNames = sysUtils.getSKClient(rUser).getUsersWithPermission(tenant, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames)
    {
      revokeSKPermissions(rUser, tenant, sysId, userName, ALL_PERMS);
      // Remove wildcard perm
      sysUtils.getSKClient(rUser).revokeUserPermission(tenant, userName, AuthUtils.getPermSpecAllStr(tenant, sysId));
    }
  }

  /**
   * Create a set of individual permSpec entries based on the list passed in
   * @param oboTenant - name of tenant associated with resource
   * @param systemId - resource Id
   * @param permList - list of individual permissions
   * @return - Set of permSpec entries based on permissions
   */
  static Set<String> getPermSpecSet(String oboTenant, String systemId, Set<Permission> permList)
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
  static String getPermSpecStr(String oboTenant, String systemId, Permission perm)
  {
    return String.format(PERM_SPEC_TEMPLATE, oboTenant, perm.name(), systemId);
  }

  /*
   * Get system share info
   */
  SystemShare getSystemShareInfo(ResourceRequestUser rUser, String tenant, String sysId)
          throws TapisException, TapisClientException
  {
    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(SYS_SHR_TYPE);
    skParms.setTenant(tenant);
    skParms.setResourceId1(sysId);

    // First determine if system is publicly shared. Search for share to grantee ~public
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    var skShares = sysUtils.getSKClient(rUser).getShares(skParms);
    // Set isPublic based on result.
    boolean isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());

    // Now get all the users with whom the system has been shared
    var userSet = new HashSet<String>();
    skParms.setGrantee(null);
    skParms.setIncludePublicGrantees(false);
    skShares = sysUtils.getSKClient(rUser).getShares(skParms);
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
  void updateUserShares(ResourceRequestUser rUser, String shareOpName, String systemId, SystemShare systemShare, boolean isPublic)
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
      userList = PUBLIC_USER_SET; // "~public"
    }

    // We will need info from system, so fetch it now
    TSystem system = dao.getSystem(rUser.getOboTenantId(), systemId);
    // We need owner to check auth and if system not there cannot find owner.
    if (system == null)
    {
      String msg = LibUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      log.info(msg);
      throw new NotFoundException(msg);
    }

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
          sysUtils.getSKClient(rUser).shareResource(reqShareResource);
          reqShareResource.setPrivilege(Permission.EXECUTE.name());
          sysUtils.getSKClient(rUser).shareResource(reqShareResource);
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
          sysUtils.getSKClient(rUser).deleteShare(deleteShareParms);
          deleteShareParms.setPrivilege(Permission.EXECUTE.name());
          sysUtils.getSKClient(rUser).deleteShare(deleteShareParms);
        }
      }
    }
  }

  /**
   * Remove SK artifacts associated with a System: user credentials, user permissions
   * No checks are done for incoming arguments and the system must exist
   */
  void revokeAllSKPermissions(ResourceRequestUser rUser, TSystem system, String resolvedEffectiveUserId)
          throws TapisException, TapisClientException
  {
    String systemId = system.getId();
    String oboTenant = system.getTenant();
    String effectiveUserId = system.getEffectiveUserId();

    // Use Security Kernel client to find all users with perms associated with the system.
    String permSpec = String.format(PERM_SPEC_TEMPLATE, oboTenant, "%", systemId);
    var userNames = sysUtils.getSKClient(rUser).getUsersWithPermission(oboTenant, permSpec);
    // Revoke all perms for all users
    for (String userName : userNames)
    {
      revokeSKPermissions(rUser, oboTenant, systemId, userName, ALL_PERMS);
      // Remove wildcard perm
      sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, userName, getPermSpecAllStr(oboTenant, systemId));
    }

    // NOTE: Consider using a notification instead(jira cic-3071)
    // Remove files perm for owner and possibly effectiveUser
    String filesPermSpec = "files:" + oboTenant + ":*:" + systemId;
    sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, system.getOwner(), filesPermSpec);
    if (!effectiveUserId.equals(APIUSERID_VAR))
      sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, resolvedEffectiveUserId, filesPermSpec);;
  }

  /**
   * Use SKClient to Grant permissions. Attempt rollback on error.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param targetUser - user receiving permissions
   * @param permissions - permissions to grant
   * @param op - operation
   * @throws TapisException on error
   */
  void grantPermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                        Set<Permission> permissions, SystemOperation op, String rawData)
          throws TapisException
  {
    String oboTenant = rUser.getOboTenantId();
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(rUser.getOboTenantId(), systemId, permissions);

    // Assign perms to user.
    // Start of updates. Will need to rollback on failure.
    try {
      // Assign perms to user. SK creates a default role for the user
      for (String permSpec : permSpecSet) {
        sysUtils.getSKClient(rUser).grantUserPermission(oboTenant, targetUser, permSpec);
      }
    } catch (TapisClientException tce) {
      // Rollback
      // Something went wrong. Attempt to undo all changes and then re-throw the exception
      String msg = LibUtils.getMsgAuth("SYSLIB_PERM_ERROR_ROLLBACK", rUser, systemId, tce.getMessage());
      log.error(msg);
      // Revoke permissions that may have been granted.
      for (String permSpec : permSpecSet) {
        try {
          sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, targetUser, permSpec);
        } catch (Exception e) {
          log.warn(LibUtils.getMsgAuth(ERROR_ROLLBACK, rUser, systemId, "revokePerm", e.getMessage()));
        }
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
   * Use SKClient to revoke permissions. Attempt rollback on error.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param targetUser - user losing permissions
   * @param permissions - permissions to revoke
   * @param op - operation
   * @throws TapisException on error
   */
  int revokePermissions(ResourceRequestUser rUser, String systemId, String targetUser,
                        Set<Permission> permissions, SystemOperation op, String rawData)
          throws TapisException
  {
    int changeCount;
    String oboTenant = rUser.getOboTenantId();
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(rUser.getOboTenantId(), systemId, permissions);

    // Determine current set of user permissions
    Set<Permission> userPermSet = new HashSet<>();
    try
    {
      userPermSet = getUserPermSet(rUser, targetUser, oboTenant, systemId);
      // Revoke perms
      changeCount = revokeSKPermissions(rUser, oboTenant, systemId, targetUser, permissions);
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
          String permSpec = AuthUtils.getPermSpecStr(oboTenant, systemId, perm);
          try { sysUtils.getSKClient(rUser).grantUserPermission(oboTenant, targetUser, permSpec); }
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

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Service authorization check. Special auth exceptions and checks are made for service requests:
   *  - getCred is only allowed for certain services
   *  - Always allow read, execute, getPerms for a service calling as itself.
   * <p>
   * If no special cases apply then final standard user request type auth check is made.
   * <p>
   * ONLY CALL this method when it is a service request
   * <p>
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
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name());
      log.info(msg);
      throw new ForbiddenException(msg);
    }

    // This is a service request. The username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    String svcTenant = rUser.getJwtTenantId();

    // For getCred, only certain services are allowed. Everyone else denied with a special message
    // Do this check first to reduce chance a request will be allowed that should not be allowed.
    if (op == SystemOperation.getCred)
    {
      if (SVCLIST_GETCRED.contains(svcName)) return;
      // Not authorized, throw an exception
      String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser, systemId, op.name());
      log.info(msg);
      throw new ForbiddenException(msg);
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
        String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH_GETCRED", rUser, systemId, op.name());
        log.info(msg);
        throw new ForbiddenException(msg);
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
    String msg = LibUtils.getMsgAuth("SYSLIB_UNAUTH", rUser, systemId, op.name());
    log.info(msg);
    throw new ForbiddenException(msg);
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
    return sysUtils.getSKClient(rUser).hasPrivilege(skParms);
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
    return sysUtils.getSKClient(rUser).isPermitted(tenantName, userName, permSpecStr);
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
    return sysUtils.getSKClient(rUser).isPermittedAny(tenantName, userName, permSpecs.toArray(TSystem.EMPTY_STR_ARRAY));
  }
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
    var skShares = sysUtils.getSKClient(rUser).getShares(skParms);
    return (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
  }

  /**
   * Revoke permissions
   * No checks are done for incoming arguments and the system must exist
   */
  private int revokeSKPermissions(ResourceRequestUser rUser, String oboTenant, String systemId, String userName,
                                  Set<Permission> permissions)
          throws TapisClientException, TapisException
  {
    // Create a set of individual permSpec entries based on the list passed in
    Set<String> permSpecSet = getPermSpecSet(oboTenant, systemId, permissions);
    // Remove perms from default user role
    for (String permSpec : permSpecSet)
    {
      sysUtils.getSKClient(rUser).revokeUserPermission(oboTenant, userName, permSpec);
    }
    return permSpecSet.size();
  }
}
