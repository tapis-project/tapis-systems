package edu.utexas.tacc.tapis.systems.dao;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.AuthListType;

public interface SystemsDao
{

  Exception checkDB();

  void migrateDB();

  /* ********************************************************************** */
  /*                             Systems                                    */
  /* ********************************************************************** */

  boolean createSystem(ResourceRequestUser rUser, TSystem system, String changeDescription, String rawData)
          throws TapisException, IllegalStateException;

  void putSystem(ResourceRequestUser rUser, TSystem putSystem, String changeDescription, String rawData)
          throws TapisException, IllegalStateException;

  void patchSystem(ResourceRequestUser rUser, String systemId, TSystem patchedSystem, String changeDescription, String rawData)
          throws TapisException, IllegalStateException;

  void updateSystemOwner(ResourceRequestUser rUser, String id, String oldOwner, String newOwner) throws TapisException;

  void updateEnabled(ResourceRequestUser rUser, String tenantId, String id, boolean enabled) throws TapisException;

  void removeParentId(ResourceRequestUser rUser, String tenantId, String childSystemId) throws TapisException;

  int removeParentIdFromChildren(ResourceRequestUser rUser, String oboTenant, String parentId, List<String> childIdsToRemove) throws TapisException;
  int removeParentIdFromAllChildren(ResourceRequestUser rUser, String oboTenant, String parentId) throws TapisException;

  void updateDeleted(ResourceRequestUser rUser, String tenantId, String id, boolean deleted) throws TapisException;

  void addUpdateRecord(ResourceRequestUser rUser, String id, SystemOperation op, String changeDescription, String rawData);

  int hardDeleteSystem(String tenantId, String id) throws TapisException;

  boolean checkForSystem(String tenantId, String id, boolean includeDeleted) throws TapisException;
  boolean hasChildren(String tenantId, String id) throws TapisException;

  boolean isEnabled(String tenantId, String id);

  String getParent(String tenantId, String sysId);

  TSystem getSystem(String tenantId, String id);

  TSystem getSystem(String tenantId, String id, boolean includeDeleted);

  int getSystemsCount(ResourceRequestUser rUser, String oboUser, List<String> searchList, ASTNode searchAST,
                      List<OrderBy> orderByList, String startAfter, boolean includeDeleted, AuthListType listType,
                      Set<String> viewableIDs, Set<String> sharedIDs)
          throws TapisException;

  List<TSystem> getSystems(ResourceRequestUser rUser, String oboUser, List<String> searchList, ASTNode searchAST,
                           int limit, List<OrderBy> orderByList, int skip, String startAfter, boolean includeDeleted,
                           AuthListType listType, Set<String> viewableIDs, Set<String> sharedIDs)
          throws TapisException;

  Set<String> getSystemIDs(String tenant, boolean includeDeleted) throws TapisException;

  List<TSystem> getSystemsSatisfyingConstraints(String tenantId, ASTNode matchAST, Set<String> setOfIDs) throws TapisException;

  String getSystemOwner(String tenantId, String id) throws TapisException;

  AuthnMethod getSystemDefaultAuthnMethod(String tenantId, String id);

  /* ********************************************************************** */
  /*                        CredentialInfo Table                            */
  /* ********************************************************************** */

  int getCredInfoTotalCount();

  CredentialInfo getCredInfo(String tenantId, String systemId, String tapisUser, boolean isStatic);

  CredentialInfo getCredInfo(CredentialInfo credInfo);

  List<CredentialInfo> getCredInfoRecordsForSystem(String tenantId, String systemId);

  void deleteCredInfo(String tenantId, String systemId, String tapisUser, boolean isStatic);

  void deleteCredInfoRecord(CredentialInfo credInfo);

  void deleteAllCredInfoRecordsForSystem(String tenant, String systemId);

  CredentialInfo createCredInfo(ResourceRequestUser rUser, CredentialInfo credInfo);

  void updateCredInfoRecord(CredentialInfo credInfo, LocalDateTime updated);

  void updateCredInfoStatus(CredentialInfo credInfo, CredentialInfo.SyncStatus newSyncStatus, LocalDateTime updated);

  String getLoginUserMapping(String tenantId, String id, String tapisUser, boolean isStatic);

  int credInfoMarkAllInProgressAsFailed(ResourceRequestUser rUser, String failMsg);

  int credInfoMarkAllFailedAsPending(ResourceRequestUser rUser);

  void credInfoMarkAsComplete(CredentialInfo credInfo);

  List<CredentialInfo> credInfoGetRecordsInStatus(CredentialInfo.SyncStatus status);

  int credInfoCreatePendingForStaticSystems();

  /* ********************************************************************** */
  /*                             Scheduler Profiles                         */
  /* ********************************************************************** */

  void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile profile) throws TapisException, IllegalStateException;

  SchedulerProfile getSchedulerProfile(String tenantId, String name);

  List<SchedulerProfile> getSchedulerProfiles(String tenantId);

  int deleteSchedulerProfile(String tenantId, String name) throws TapisException;

  boolean checkForSchedulerProfile(String tenantId, String name) throws TapisException;

  String getSchedulerProfileOwner(String tenant, String name);

  List<SystemHistoryItem> getSystemHistory(String oboTenant, String systemId);
}
