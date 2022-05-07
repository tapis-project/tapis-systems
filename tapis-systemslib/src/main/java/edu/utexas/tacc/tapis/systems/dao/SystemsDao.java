package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.util.List;
import java.util.Set;

public interface SystemsDao
{

  Exception checkDB();

  void migrateDB() throws TapisException;

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

  void updateDeleted(ResourceRequestUser rUser, String tenantId, String id, boolean deleted) throws TapisException;

  void addUpdateRecord(ResourceRequestUser rUser, String id, SystemOperation op, String changeDescription, String rawData)
          throws TapisException;

  int hardDeleteSystem(String tenantId, String id) throws TapisException;

  boolean checkForSystem(String tenantId, String id, boolean includeDeleted) throws TapisException;

  boolean isEnabled(String tenantId, String id) throws TapisException;

  TSystem getSystem(String tenantId, String id) throws TapisException;

  TSystem getSystem(String tenantId, String id, boolean includeDeleted) throws TapisException;

  int getSystemsCount(String tenantId, List<String> searchList, ASTNode searchAST, Set<String> setOfIDs,
                      List<OrderBy> orderByList, String startAfter, boolean showDeleted) throws TapisException;

  List<TSystem> getSystems(String tenantId, List<String> searchList, ASTNode searchAST, Set<String> setOfIDs, int limit,
                           List<OrderBy> orderByList, int skip, String startAfter, boolean showDeleted)
          throws TapisException;

  Set<String> getSystemIDs(String tenant, boolean showDeleted) throws TapisException;

  List<TSystem> getSystemsSatisfyingConstraints(String tenantId, ASTNode matchAST, Set<String> setOfIDs) throws TapisException;

  String getSystemOwner(String tenantId, String id) throws TapisException;

  String getSystemEffectiveUserId(String tenantId, String id) throws TapisException;

  AuthnMethod getSystemDefaultAuthnMethod(String tenantId, String id) throws TapisException;


  /* ********************************************************************** */
  /*                             Scheduler Profiles                         */
  /* ********************************************************************** */

  void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile profile) throws TapisException, IllegalStateException;

  SchedulerProfile getSchedulerProfile(String tenantId, String name) throws TapisException;

  List<SchedulerProfile> getSchedulerProfiles(String tenantId) throws TapisException;

  int deleteSchedulerProfile(String tenantId, String name) throws TapisException;

  boolean checkForSchedulerProfile(String tenantId, String name) throws TapisException;

  String getSchedulerProfileOwner(String tenant, String name) throws TapisException;

  List<SystemHistoryItem> getSystemHistory(String oboTenant, String systemId) throws TapisException;

}
