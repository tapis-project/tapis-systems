package edu.utexas.tacc.tapis.systems.service;

import java.util.List;
import java.util.Set;
import org.jvnet.hk2.annotations.Contract;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.SystemShare;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;

/*
 * Interface for Systems Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface SystemsService
{
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------
  TSystem createSystem(ResourceRequestUser rUser, TSystem system, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  void patchSystem(ResourceRequestUser rUser, String systemId, PatchSystem patchSystem, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  TSystem putSystem(ResourceRequestUser rUser, TSystem putSystem, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  int enableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  int disableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  int deleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  int undeleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  int changeSystemOwner(ResourceRequestUser rUser, String systemId, String newOwnerName)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  boolean checkForSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException;

  boolean checkForSystem(ResourceRequestUser rUser, String systemId, boolean includeDeleted)
          throws TapisException, TapisClientException;

  boolean isEnabled(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException;

  TSystem getSystem(ResourceRequestUser rUser, String systemId, AuthnMethod authnMethod, boolean requireExecPerm,
                    boolean getCreds, String impersonationId, boolean resolveEffective, boolean sharedAppCtx)
          throws TapisException, TapisClientException;

  int getSystemsTotalCount(ResourceRequestUser rUser, List<String> searchList, List<OrderBy> orderByList,
                           String startAfter, boolean includeDeleted, String listType) throws TapisException, TapisClientException;

  List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList, int limit, List<OrderBy> orderByList,
                           int skip, String startAfter, boolean resolveEffective, boolean includeDeleted, String listType)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser, String searchStr, int limit,
                                        List<OrderBy> orderByList, int skip, String startAfter,
                                        boolean resolveEffective, boolean includeDeleted, String listType)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsSatisfyingConstraints(ResourceRequestUser rUser, String matchStr, boolean resolveEffective)
          throws TapisException, TapisClientException;

  String getSystemOwner(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException;

  // --------------------------- Permissions -------------------------------
  // -----------------------------------------------------------------------
  void grantUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser, Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException;

  int revokeUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser, Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException;

  Set<Permission> getUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException;

  // ---------------------------- Credentials ------------------------------
  // -----------------------------------------------------------------------
  Credential createUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, Credential credential,
                            boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, IllegalStateException;

  int deleteUserCredential(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException, IllegalStateException;

  Credential checkUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, IllegalStateException;

  Credential checkUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException;

  Credential getUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, AuthnMethod authnMethod)
          throws TapisException, TapisClientException;

  // ------------------- Scheduler Profiles---------------------------------
  // -----------------------------------------------------------------------
  void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException;

  SchedulerProfile getSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException;

  List<SchedulerProfile> getSchedulerProfiles(ResourceRequestUser rUser) throws TapisException;

  int deleteSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, IllegalArgumentException;

  boolean checkForSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException;

  List<SystemHistoryItem> getSystemHistory(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, IllegalStateException;
  
  SystemShare getSystemShare(ResourceRequestUser rUser, String systemId)
      throws TapisException, TapisClientException, IllegalStateException;
  
  //------------------- Share ---------------------------------
  // -----------------------------------------------------------------------
  void shareSystem(ResourceRequestUser rUser, String systemId, SystemShare postShare)
      throws TapisException, TapisClientException, IllegalStateException;
  
  void unshareSystem(ResourceRequestUser rUser, String systemId, SystemShare postShare)
      throws TapisException, TapisClientException, IllegalStateException;

  void shareSystemPublicly(ResourceRequestUser rUser, String systemId) 
      throws TapisException, TapisClientException;
  
  void unshareSystemPublicly(ResourceRequestUser rUser, String systemId) 
      throws TapisException, TapisClientException, IllegalStateException;
}
