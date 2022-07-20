package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import org.jvnet.hk2.annotations.Contract;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Set;

/*
 * Interface for Systems Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface SystemsService
{
  // ------------------------- Systems -------------------------------------
  // -----------------------------------------------------------------------
  void createSystem(ResourceRequestUser rUser, TSystem system, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException;

  void patchSystem(ResourceRequestUser rUser, String systemId, PatchSystem patchSystem, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  void putSystem(ResourceRequestUser rUser, TSystem putSystem, boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int enableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int disableSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int deleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int undeleteSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  int changeSystemOwner(ResourceRequestUser rUser, String systemId, String newOwnerName)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException, NotFoundException;

  boolean checkForSystem(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException;

  boolean checkForSystem(ResourceRequestUser rUser, String systemId, boolean includeDeleted)
          throws TapisException, TapisClientException, NotAuthorizedException;

  boolean isEnabled(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException;

  TSystem getSystem(ResourceRequestUser rUser, String systemId, AuthnMethod authnMethod, boolean requireExecPerm,
                    boolean getCreds, String impersonationId, boolean resolveEffectiveUser)
          throws TapisException, TapisClientException, NotAuthorizedException;

  int getSystemsTotalCount(ResourceRequestUser rUser, List<String> searchList, List<OrderBy> orderByList,
                           String startAfter, boolean showDeleted) throws TapisException, TapisClientException;

  List<TSystem> getSystems(ResourceRequestUser rUser, List<String> searchList, int limit, List<OrderBy> orderByList,
                           int skip, String startAfter, boolean resolveEffectiveUser, boolean showDeleted)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsUsingSqlSearchStr(ResourceRequestUser rUser, String searchStr, int limit,
                                        List<OrderBy> orderByList, int skip, String startAfter,
                                        boolean resolveEffectiveUser, boolean showDeleted)
          throws TapisException, TapisClientException;

  List<TSystem> getSystemsSatisfyingConstraints(ResourceRequestUser rUser, String matchStr)
          throws TapisException, TapisClientException;

  String getSystemOwner(ResourceRequestUser rUser, String systemId)
          throws TapisException, TapisClientException, NotAuthorizedException;

  // --------------------------- Permissions -------------------------------
  // -----------------------------------------------------------------------
  void grantUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser, Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException;

  int revokeUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser, Set<Permission> permissions, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException;

  Set<Permission> getUserPermissions(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException, NotAuthorizedException;

  // ---------------------------- Credentials ------------------------------
  // -----------------------------------------------------------------------
  void createUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, Credential credential,
                            boolean skipCredCheck, String rawData)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException;

  int deleteUserCredential(ResourceRequestUser rUser, String systemId, String targetUser)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException;

  Credential getUserCredential(ResourceRequestUser rUser, String systemId, String targetUser, AuthnMethod authnMethod)
          throws TapisException, TapisClientException, NotAuthorizedException;

  // ------------------- Scheduler Profiles---------------------------------
  // -----------------------------------------------------------------------
  void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalStateException, IllegalArgumentException;

  SchedulerProfile getSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, NotAuthorizedException;

  List<SchedulerProfile> getSchedulerProfiles(ResourceRequestUser rUser) throws TapisException;

  int deleteSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, NotAuthorizedException, IllegalArgumentException;

  boolean checkForSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, NotAuthorizedException;

List<SystemHistoryItem> getSystemHistory(ResourceRequestUser rUser, String systemId)
        throws TapisException, NotAuthorizedException, TapisClientException, IllegalStateException;
}
