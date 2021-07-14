package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.util.List;
import java.util.Set;

public interface SystemsDao
{
  boolean createSystem(ResourceRequestUser rUser, TSystem system, String createJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  void putSystem(ResourceRequestUser rUser, TSystem putSystem, String updateJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  void patchSystem(ResourceRequestUser rUser, TSystem patchedSystem, PatchSystem patchSystem,
                   String updateJsonStr, String scrubbedText)
          throws TapisException, IllegalStateException;

  void updateSystemOwner(ResourceRequestUser rUser, String tenantId, String id, String newOwnerName) throws TapisException;

  void updateEnabled(ResourceRequestUser rUser, String tenantId, String id, boolean enabled) throws TapisException;

  void updateDeleted(ResourceRequestUser rUser, String tenantId, String id, boolean deleted) throws TapisException;

  void addUpdateRecord(ResourceRequestUser rUser, String tenantId, String id, SystemOperation op,
                       String upd_json, String upd_text) throws TapisException;

  int hardDeleteSystem(String tenantId, String id) throws TapisException;

  Exception checkDB();

  void migrateDB() throws TapisException;

  boolean checkForSystem(String tenantId, String id, boolean includeDeleted) throws TapisException;

  boolean isEnabled(String tenantId, String id) throws TapisException;

  TSystem getSystem(String tenantId, String id) throws TapisException;

  TSystem getSystem(String tenantId, String id, boolean includeDeleted) throws TapisException;

  int getSystemsCount(String tenantId, List<String> searchList, ASTNode searchAST, Set<String> setOfIDs,
                      List<OrderBy> orderByList, String startAfter, boolean showDeleted) throws TapisException;

  List<TSystem> getSystems(String tenantId, List<String> searchList, ASTNode searchAST, Set<String> setOfIDs, int limit,
                           List<OrderBy> orderByList, int skip, String startAfter, boolean showDeleted)
          throws TapisException;

  List<TSystem> getSystemsSatisfyingConstraints(String tenantId, ASTNode matchAST, Set<String> setOfIDs) throws TapisException;

  String getSystemOwner(String tenantId, String id) throws TapisException;

  String getSystemEffectiveUserId(String tenantId, String id) throws TapisException;

  AuthnMethod getSystemDefaultAuthnMethod(String tenantId, String id) throws TapisException;
}
