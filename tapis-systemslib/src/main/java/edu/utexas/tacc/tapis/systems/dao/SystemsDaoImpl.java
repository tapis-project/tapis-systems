package edu.utexas.tacc.tapis.systems.dao;

import java.sql.Connection;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.flywaydb.core.Flyway;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.search.parser.ASTBinaryExpression;
import edu.utexas.tacc.tapis.search.parser.ASTLeaf;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.search.parser.ASTUnaryExpression;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy.OrderByDir;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SchedulerProfilesRecord;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SchedProfileModLoadRecord;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemUpdatesRecord;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemsRecord;
import edu.utexas.tacc.tapis.systems.model.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.ModuleLoadSpec;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.AuthListType;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

import static edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator.CONTAINS;
import static edu.utexas.tacc.tapis.search.SearchUtils.SearchOperator.NCONTAINS;
import static edu.utexas.tacc.tapis.systems.gen.jooq.Tables.*;
import static edu.utexas.tacc.tapis.systems.gen.jooq.Tables.SYSTEMS;
import static edu.utexas.tacc.tapis.shared.threadlocal.OrderBy.DEFAULT_ORDERBY_DIRECTION;
import static edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl.DEFAULT_LIST_TYPE;

/*
 * Class to handle persistence and queries for Tapis System objects.
 */
public class SystemsDaoImpl implements SystemsDao
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(SystemsDaoImpl.class);

  private static final String EMPTY_JSON = "{}";
  private static final int INVALID_SEQ_ID = -1;

  // Create a static Set of column names for table SYSTEMS
  private static final Set<String> SYSTEMS_FIELDS = new HashSet<>();
  static
  {
    for (Field<?> field : SYSTEMS.fields()) { SYSTEMS_FIELDS.add(field.getName()); }
  }

  // Compiled regexes for splitting around "\." and "\$"
  private static final Pattern DOT_SPLIT = Pattern.compile("\\.");
  private static final Pattern DOLLAR_SPLIT = Pattern.compile("\\$");

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /**
   * Create a new system.
   *
   * @return true if created
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public boolean createSystem(ResourceRequestUser rUser, TSystem system, String changeDescription, String rawData)
          throws TapisException, IllegalStateException {
    String opName = "createSystem";
    // ------------------------- Check Input -------------------------
    if (system == null) LibUtils.logAndThrowNullParmException(opName, "system");
    if (rUser == null) LibUtils.logAndThrowNullParmException(opName, "resourceRequestUser");
    if (StringUtils.isBlank(changeDescription)) LibUtils.logAndThrowNullParmException(opName, "changeDescription");
    if (StringUtils.isBlank(system.getTenant())) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(system.getId())) LibUtils.logAndThrowNullParmException(opName, "systemId");
    if (system.getSystemType() == null) LibUtils.logAndThrowNullParmException(opName, "systemType");
    if (system.getDefaultAuthnMethod() == null) LibUtils.logAndThrowNullParmException(opName, "defaultAuthnMethod");
    
    // Make sure owner, effectiveUserId, etc are set
    String owner = TSystem.DEFAULT_OWNER;
    if (StringUtils.isNotBlank(system.getOwner())) owner = system.getOwner();
    String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
    if (StringUtils.isNotBlank(system.getEffectiveUserId())) effectiveUserId = system.getEffectiveUserId();
    JsonElement jobEnvVariablesJson = TSystem.DEFAULT_JOBENV_VARIABLES;
    if (system.getJobEnvVariables() != null) jobEnvVariablesJson = TapisGsonUtils.getGson().toJsonTree(system.getJobEnvVariables());
    JsonElement jobRuntimesJson = null;
    if (system.getJobRuntimes() != null) jobRuntimesJson = TapisGsonUtils.getGson().toJsonTree(system.getJobRuntimes());
    JsonElement batchLogicalQueuesJson = TSystem.DEFAULT_BATCH_LOGICAL_QUEUES;
    if (system.getBatchLogicalQueues() != null) batchLogicalQueuesJson = TapisGsonUtils.getGson().toJsonTree(system.getBatchLogicalQueues());
    JsonElement jobCapabilitiesJson = TSystem.DEFAULT_JOB_CAPABILITIES;
    if (system.getJobCapabilities() != null) jobCapabilitiesJson = TapisGsonUtils.getGson().toJsonTree(system.getJobCapabilities());
    String[] tagsStrArray = TSystem.EMPTY_STR_ARRAY;
    if (system.getTags() != null) tagsStrArray = system.getTags();
    JsonObject notesObj = TSystem.DEFAULT_NOTES;
    if (system.getNotes() != null) notesObj = (JsonObject) system.getNotes();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      if(!StringUtils.isBlank(system.getParentId())) {
        // in the case of a child system (the parentId is not null) we must guard against race conditions related
        // to the allowChildren flag.  We will read the parent system for update ('lock'), and then check that
        // allowChildren is true.  This also prevents another possible race conditon where a system may bet deleted
        // during the creation of the child as well.
        TSystem parentSystem = getSystemForUpdate(db, system.getTenant(), system.getParentId());
        if((parentSystem == null) || (!parentSystem.isAllowChildren())) {
          throw new IllegalStateException(LibUtils.getMsg("SYSLIB_SYS_CHILDREN_NOT_PERMITTED", rUser, system.getId()));
        }
      }

      // Check to see if system exists (even if deleted). If yes then throw IllegalStateException
      boolean doesExist = checkForSystem(db, system.getTenant(), system.getId(), true);
      if (doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_SYS_EXISTS", rUser, system.getId()));

      // Generate uuid for the new resource
      system.setUuid(UUID.randomUUID());

      Record record = db.insertInto(SYSTEMS)
              .set(SYSTEMS.TENANT, system.getTenant())
              .set(SYSTEMS.ID, system.getId())
              .set(SYSTEMS.DESCRIPTION, system.getDescription())
              .set(SYSTEMS.SYSTEM_TYPE, system.getSystemType())
              .set(SYSTEMS.OWNER, owner)
              .set(SYSTEMS.HOST, system.getHost())
              .set(SYSTEMS.ENABLED, system.isEnabled())
              .set(SYSTEMS.EFFECTIVE_USER_ID, effectiveUserId)
              .set(SYSTEMS.DEFAULT_AUTHN_METHOD, system.getDefaultAuthnMethod())
              .set(SYSTEMS.BUCKET_NAME, system.getBucketName())
              .set(SYSTEMS.ROOT_DIR, system.getRootDir())
              .set(SYSTEMS.PORT, system.getPort())
              .set(SYSTEMS.USE_PROXY, system.isUseProxy())
              .set(SYSTEMS.PROXY_HOST, system.getProxyHost())
              .set(SYSTEMS.PROXY_PORT, system.getProxyPort())
              .set(SYSTEMS.DTN_SYSTEM_ID, system.getDtnSystemId())
              .set(SYSTEMS.DTN_MOUNT_SOURCE_PATH, system.getDtnMountSourcePath())
              .set(SYSTEMS.DTN_MOUNT_POINT, system.getDtnMountPoint())
              .set(SYSTEMS.IS_DTN, system.isDtn())
              .set(SYSTEMS.CAN_EXEC, system.getCanExec())
              .set(SYSTEMS.CAN_RUN_BATCH, system.getCanRunBatch())
              .set(SYSTEMS.ENABLE_CMD_PREFIX, system.isEnableCmdPrefix())
              .set(SYSTEMS.MPI_CMD, system.getMpiCmd())
              .set(SYSTEMS.JOB_RUNTIMES, jobRuntimesJson)
              .set(SYSTEMS.JOB_WORKING_DIR, system.getJobWorkingDir())
              .set(SYSTEMS.JOB_ENV_VARIABLES, jobEnvVariablesJson)
              .set(SYSTEMS.JOB_MAX_JOBS, system.getJobMaxJobs())
              .set(SYSTEMS.JOB_MAX_JOBS_PER_USER, system.getJobMaxJobsPerUser())
              .set(SYSTEMS.BATCH_SCHEDULER, system.getBatchScheduler())
              .set(SYSTEMS.BATCH_LOGICAL_QUEUES, batchLogicalQueuesJson)
              .set(SYSTEMS.BATCH_DEFAULT_LOGICAL_QUEUE, system.getBatchDefaultLogicalQueue())
              .set(SYSTEMS.BATCH_SCHEDULER_PROFILE, system.getBatchSchedulerProfile())
              .set(SYSTEMS.JOB_CAPABILITIES, jobCapabilitiesJson)
              .set(SYSTEMS.TAGS, tagsStrArray)
              .set(SYSTEMS.NOTES, notesObj)
              .set(SYSTEMS.IMPORT_REF_ID, system.getImportRefId())
              .set(SYSTEMS.UUID, system.getUuid())
              .set(SYSTEMS.PARENT_ID, system.getParentId())
              .set(SYSTEMS.ALLOW_CHILDREN, system.isAllowChildren())
              .returningResult(SYSTEMS.SEQ_ID)
              .fetchOne();

      // If record is null it is an error
      if (record == null)
      {
        throw new TapisException(LibUtils.getMsgAuth("SYSLIB_DB_NULL_RESULT", rUser, system.getId(), opName));
      }

      // Generated sequence id
      int seqId = record.getValue(SYSTEMS.SEQ_ID);

      if (seqId < 1) return false;

      // Persist change history record
      addUpdate(db, rUser, system.getId(), seqId, SystemOperation.create, changeDescription, rawData, system.getUuid());

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return true;
  }

  /**
   * Update all updatable attributes of an existing system.
   * Following columns will be updated:
   *   description, host, effectiveUserId, defaultAuthnMethod,
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId, dtnMountPoint, dtnMountSourcePath,
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUers, canRunBatch,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public void putSystem(ResourceRequestUser rUser, TSystem putSystem, String changeDescription, String rawData)
          throws TapisException, IllegalStateException {
    String opName = "putSystem";
    // ------------------------- Check Input -------------------------
    if (putSystem == null) LibUtils.logAndThrowNullParmException(opName, "putSystem");
    if (rUser == null) LibUtils.logAndThrowNullParmException(opName, "resourceRequestUser");
    // Pull out some values for convenience
    String tenantId = putSystem.getTenant();
    String systemId = putSystem.getId();
    // Check required attributes have been provided
    if (StringUtils.isBlank(changeDescription)) LibUtils.logAndThrowNullParmException(opName, "changeDescription");
    if (StringUtils.isBlank(tenantId)) LibUtils.logAndThrowNullParmException(opName, "tenantId");
    if (StringUtils.isBlank(systemId)) LibUtils.logAndThrowNullParmException(opName, "systemId");
    if (putSystem.getSystemType() == null) LibUtils.logAndThrowNullParmException(opName, "systemType");

    // Make sure effectiveUserId, notes, etc are set
    String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
    if (StringUtils.isNotBlank(putSystem.getEffectiveUserId())) effectiveUserId = putSystem.getEffectiveUserId();
    JsonElement jobEnvVariablesJson = TSystem.DEFAULT_JOBENV_VARIABLES;
    if (putSystem.getJobEnvVariables() != null) jobEnvVariablesJson = TapisGsonUtils.getGson().toJsonTree(putSystem.getJobEnvVariables());
    JsonElement jobRuntimesJson = null;
    if (putSystem.getJobRuntimes() != null) jobRuntimesJson = TapisGsonUtils.getGson().toJsonTree(putSystem.getJobRuntimes());
    JsonElement batchLogicalQueuesJson = TSystem.DEFAULT_BATCH_LOGICAL_QUEUES;
    if (putSystem.getBatchLogicalQueues() != null) batchLogicalQueuesJson = TapisGsonUtils.getGson().toJsonTree(putSystem.getBatchLogicalQueues());
    JsonElement jobCapabilitiesJson = TSystem.DEFAULT_JOB_CAPABILITIES;
    if (putSystem.getJobCapabilities() != null) jobCapabilitiesJson = TapisGsonUtils.getGson().toJsonTree(putSystem.getJobCapabilities());
    String[] tagsStrArray = TSystem.EMPTY_STR_ARRAY;
    if (putSystem.getTags() != null) tagsStrArray = putSystem.getTags();
    JsonObject notesObj =  TSystem.DEFAULT_NOTES;
    if (putSystem.getNotes() != null) notesObj = (JsonObject) putSystem.getNotes();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Make sure system exists and has not been deleted.
      boolean doesExist = checkForSystem(db, tenantId, systemId, false);
      if (!doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", rUser, systemId));

      if(!putSystem.isAllowChildren()) {
        // in the case of a parent system that has allowChildren set to false, we must guard against race conditions related
        // to the allowChildren flag.  We will read the parent system for update ('lock'), and then check to see
        // if the system has children.  If not we can proceed, but if it does have children we can't allow the patch to
        // go through.
        TSystem system = getSystemForUpdate(db, tenantId, systemId);
        if((system != null) && hasChildren(tenantId, systemId)) {
          throw new IllegalStateException(LibUtils.getMsg("SYSLIB_ERROR_PARENT_CHILD_CONFLICT", rUser, systemId));
        }
      }

      // Make sure UUID filled in, needed for update record. Pre-populated putSystem may not have it.
      UUID uuid = putSystem.getUuid();
      if (uuid == null) uuid = getUUIDUsingDb(db, tenantId, systemId);

      var result = db.update(SYSTEMS)
              .set(SYSTEMS.DESCRIPTION, putSystem.getDescription())
              .set(SYSTEMS.HOST, putSystem.getHost())
              .set(SYSTEMS.EFFECTIVE_USER_ID, effectiveUserId)
              .set(SYSTEMS.DEFAULT_AUTHN_METHOD, putSystem.getDefaultAuthnMethod())
              .set(SYSTEMS.PORT, putSystem.getPort())
              .set(SYSTEMS.USE_PROXY, putSystem.isUseProxy())
              .set(SYSTEMS.PROXY_HOST, putSystem.getProxyHost())
              .set(SYSTEMS.PROXY_PORT, putSystem.getProxyPort())
              .set(SYSTEMS.DTN_SYSTEM_ID, putSystem.getDtnSystemId())
              .set(SYSTEMS.DTN_MOUNT_POINT, putSystem.getDtnMountPoint())
              .set(SYSTEMS.DTN_MOUNT_SOURCE_PATH, putSystem.getDtnMountSourcePath())
              .set(SYSTEMS.CAN_RUN_BATCH, putSystem.getCanRunBatch())
              .set(SYSTEMS.ENABLE_CMD_PREFIX, putSystem.isEnableCmdPrefix())
              .set(SYSTEMS.MPI_CMD, putSystem.getMpiCmd())
              .set(SYSTEMS.JOB_RUNTIMES, jobRuntimesJson)
              .set(SYSTEMS.JOB_WORKING_DIR, putSystem.getJobWorkingDir())
              .set(SYSTEMS.JOB_ENV_VARIABLES, jobEnvVariablesJson)
              .set(SYSTEMS.JOB_MAX_JOBS, putSystem.getJobMaxJobs())
              .set(SYSTEMS.JOB_MAX_JOBS_PER_USER, putSystem.getJobMaxJobsPerUser())
              .set(SYSTEMS.BATCH_SCHEDULER, putSystem.getBatchScheduler())
              .set(SYSTEMS.BATCH_LOGICAL_QUEUES, batchLogicalQueuesJson)
              .set(SYSTEMS.BATCH_DEFAULT_LOGICAL_QUEUE, putSystem.getBatchDefaultLogicalQueue())
              .set(SYSTEMS.BATCH_SCHEDULER_PROFILE, putSystem.getBatchSchedulerProfile())
              .set(SYSTEMS.ALLOW_CHILDREN, putSystem.isAllowChildren())
              .set(SYSTEMS.JOB_CAPABILITIES, jobCapabilitiesJson)
              .set(SYSTEMS.TAGS, tagsStrArray)
              .set(SYSTEMS.NOTES, notesObj)
              .set(SYSTEMS.IMPORT_REF_ID, putSystem.getImportRefId())
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(systemId))
              .returningResult(SYSTEMS.SEQ_ID)
              .fetchOne();

      // If result is null it is an error
      if (result == null)
      {
        throw new TapisException(LibUtils.getMsgAuth("SYSLIB_DB_NULL_RESULT", rUser, systemId, opName));
      }

      int seqId = result.getValue(SYSTEMS.SEQ_ID);

      // Persist update record
      addUpdate(db, rUser, putSystem.getId(), seqId, SystemOperation.modify, changeDescription, rawData, uuid);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Patch selected attributes of an existing system.
   * Following columns will be updated:
   *   description, host, effectiveUserId, defaultAuthnMethod,
   *   port, useProxy, proxyHost, proxyPort, dtnSystemId, dtnMountPoint, dtnMountSourcePath,
   *   jobRuntimes, jobWorkingDir, jobEnvVariables, jobMaxJobs, jobMaxJobsPerUers, canRunBatch,
   *   batchScheduler, batchLogicalQueues, batchDefaultLogicalQueue, batchSchedulerProfile, jobCapabilities, tags, notes.
   * @throws TapisException - on error
   * @throws IllegalStateException - if system already exists
   */
  @Override
  public void patchSystem(ResourceRequestUser rUser, String systemId, TSystem patchedSystem, String changeDescription, String rawData)
          throws TapisException, IllegalStateException
  {
    String opName = "patchSystem";
    // ------------------------- Check Input -------------------------
    if (patchedSystem == null) LibUtils.logAndThrowNullParmException(opName, "patchedSystem");
    if (rUser == null) LibUtils.logAndThrowNullParmException(opName, "resourceRequestUser");
    // Pull out some values for convenience
    String tenant = rUser.getOboTenantId();
    if (StringUtils.isBlank(changeDescription)) LibUtils.logAndThrowNullParmException(opName, "changeDescription");
    if (StringUtils.isBlank(tenant)) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(systemId)) LibUtils.logAndThrowNullParmException(opName, "systemId");
    if (patchedSystem.getSystemType() == null) LibUtils.logAndThrowNullParmException(opName, "systemType");
    
    // Make sure effectiveUserId, jobEnvVariables, etc are set
    String effectiveUserId = TSystem.DEFAULT_EFFECTIVEUSERID;
    if (StringUtils.isNotBlank(patchedSystem.getEffectiveUserId())) effectiveUserId = patchedSystem.getEffectiveUserId();

    JsonElement jobEnvVariablesJson = TSystem.DEFAULT_JOBENV_VARIABLES;
    if (patchedSystem.getJobEnvVariables() != null) jobEnvVariablesJson = TapisGsonUtils.getGson().toJsonTree(patchedSystem.getJobEnvVariables());

    JsonElement jobRuntimesJson = null;
    if (patchedSystem.getJobRuntimes() != null) jobRuntimesJson = TapisGsonUtils.getGson().toJsonTree(patchedSystem.getJobRuntimes());
    JsonElement batchLogicalQueuesJson = TSystem.DEFAULT_BATCH_LOGICAL_QUEUES;
    if (patchedSystem.getBatchLogicalQueues() != null) batchLogicalQueuesJson = TapisGsonUtils.getGson().toJsonTree(patchedSystem.getBatchLogicalQueues());
    JsonElement jobCapabilitiesJson = TSystem.DEFAULT_JOB_CAPABILITIES;
    if (patchedSystem.getJobCapabilities() != null) jobCapabilitiesJson = TapisGsonUtils.getGson().toJsonTree(patchedSystem.getJobCapabilities());
    String[] tagsStrArray = TSystem.EMPTY_STR_ARRAY;
    if (patchedSystem.getTags() != null) tagsStrArray = patchedSystem.getTags();
    JsonObject notesObj =  TSystem.DEFAULT_NOTES;
    if (patchedSystem.getNotes() != null) notesObj = (JsonObject) patchedSystem.getNotes();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      if(!patchedSystem.isAllowChildren()) {
        // in the case of a parent system that has allowChildren set to false, we must guard against race conditions related
        // to the allowChildren flag.  We will read the parent system for update ('lock'), and then check to see
        // if the system has children.  If not we can proceed, but if it does have children we can't allow the patch to
        // go through.
        TSystem parentSystem = getSystemForUpdate(db, tenant, systemId);
        if((parentSystem == null) && hasChildren(tenant, systemId)) {
          throw new IllegalStateException(LibUtils.getMsg("SYSLIB_ERROR_PARENT_CHILD_CONFLICT", rUser, systemId));
        }
      }


      // Make sure system exists and has not been deleted.
      boolean doesExist = checkForSystem(db, tenant, systemId, false);
      if (!doesExist) throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_NOT_FOUND", rUser, systemId));


      var result = db.update(SYSTEMS)
              .set(SYSTEMS.DESCRIPTION, patchedSystem.getDescription())
              .set(SYSTEMS.HOST, patchedSystem.getHost())
              .set(SYSTEMS.EFFECTIVE_USER_ID, effectiveUserId)
              .set(SYSTEMS.DEFAULT_AUTHN_METHOD, patchedSystem.getDefaultAuthnMethod())
              .set(SYSTEMS.PORT, patchedSystem.getPort())
              .set(SYSTEMS.USE_PROXY, patchedSystem.isUseProxy())
              .set(SYSTEMS.PROXY_HOST, patchedSystem.getProxyHost())
              .set(SYSTEMS.PROXY_PORT, patchedSystem.getProxyPort())
              .set(SYSTEMS.DTN_SYSTEM_ID, patchedSystem.getDtnSystemId())
              .set(SYSTEMS.DTN_MOUNT_POINT, patchedSystem.getDtnMountPoint())
              .set(SYSTEMS.DTN_MOUNT_SOURCE_PATH, patchedSystem.getDtnMountSourcePath())
              .set(SYSTEMS.CAN_RUN_BATCH, patchedSystem.getCanRunBatch())
              .set(SYSTEMS.ENABLE_CMD_PREFIX, patchedSystem.isEnableCmdPrefix())
              .set(SYSTEMS.MPI_CMD, patchedSystem.getMpiCmd())
              .set(SYSTEMS.JOB_RUNTIMES, jobRuntimesJson)
              .set(SYSTEMS.JOB_WORKING_DIR, patchedSystem.getJobWorkingDir())
              .set(SYSTEMS.JOB_ENV_VARIABLES, jobEnvVariablesJson)
              .set(SYSTEMS.JOB_MAX_JOBS, patchedSystem.getJobMaxJobs())
              .set(SYSTEMS.JOB_MAX_JOBS_PER_USER, patchedSystem.getJobMaxJobsPerUser())
              .set(SYSTEMS.BATCH_SCHEDULER, patchedSystem.getBatchScheduler())
              .set(SYSTEMS.BATCH_LOGICAL_QUEUES, batchLogicalQueuesJson)
              .set(SYSTEMS.BATCH_DEFAULT_LOGICAL_QUEUE, patchedSystem.getBatchDefaultLogicalQueue())
              .set(SYSTEMS.BATCH_SCHEDULER_PROFILE, patchedSystem.getBatchSchedulerProfile())
              .set(SYSTEMS.JOB_CAPABILITIES, jobCapabilitiesJson)
              .set(SYSTEMS.TAGS, tagsStrArray)
              .set(SYSTEMS.NOTES, notesObj)
              .set(SYSTEMS.IMPORT_REF_ID, patchedSystem.getImportRefId())
              .set(SYSTEMS.ALLOW_CHILDREN, patchedSystem.isAllowChildren())
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenant),SYSTEMS.ID.eq(systemId))
              .returningResult(SYSTEMS.SEQ_ID)
              .fetchOne();

      // If result is null it is an error
      if (result == null)
      {
        throw new TapisException(LibUtils.getMsgAuth("SYSLIB_DB_NULL_RESULT", rUser, systemId, opName));
      }

      updateChildSystemsFromParent(db, tenant, systemId);

      int seqId = result.getValue(SYSTEMS.SEQ_ID);

      // Persist update record
      addUpdate(db, rUser, systemId, seqId, SystemOperation.modify, changeDescription, rawData, patchedSystem.getUuid());

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Update attribute enabled for a system given system Id and value
   */
  @Override
  public void updateEnabled(ResourceRequestUser rUser, String tenantId, String id, boolean enabled)
          throws TapisException
  {
    String opName = "updateEnabled";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(id)) LibUtils.logAndThrowNullParmException(opName, "systemId");

    // SystemOperation needed for recording the update
    SystemOperation systemOp = enabled ? SystemOperation.enable : SystemOperation.disable;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.update(SYSTEMS)
              .set(SYSTEMS.ENABLED, enabled)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).execute();
      // Persist update record
      String changeDescription = "{\"enabled\":" +  enabled + "}";
      addUpdate(db, rUser, id, INVALID_SEQ_ID, systemOp, changeDescription , null, getUUIDUsingDb(db, tenantId, id));

      updateChildSystemsFromParent(db, tenantId, id);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "systems", id);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Update attribute deleted for a system given system Id and value
   */
  @Override
  public void updateDeleted(ResourceRequestUser rUser, String tenantId, String id, boolean deleted)
          throws TapisException
  {
    String opName = "updateDeleted";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(id)) LibUtils.logAndThrowNullParmException(opName, "systemId");

    // Operation needed for recording the update
    SystemOperation systemOp = deleted ? SystemOperation.delete : SystemOperation.undelete;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.update(SYSTEMS)
              .set(SYSTEMS.DELETED, deleted)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).execute();
      // Persist update record
      String changeDescription = "{\"deleted\":" +  deleted + "}";
      addUpdate(db, rUser, id, INVALID_SEQ_ID, systemOp, changeDescription , null, getUUIDUsingDb(db, tenantId, id));

      // if we undeleted a child system, we need to make sure it has the current info from
      // the parent.
      if(!deleted) {
        String parentId = db.selectFrom(SYSTEMS)
                        .where(SYSTEMS.ID.eq(id).and(SYSTEMS.DELETED.eq(false)))
                        .fetchOne(SYSTEMS.PARENT_ID);
        if(!StringUtils.isBlank(parentId)) {
          TSystem parentSystem = getSystemForUpdate(db, tenantId, parentId);
          if((parentSystem == null) || (!parentSystem.isAllowChildren())) {
            // either we couldn't find the parent, or the parent no long is allowing children
            throw new IllegalStateException(LibUtils.getMsg("SYSLIB_SYS_CHILDREN_NOT_PERMITTED", rUser, id));
          }

          // we really only need to update a single system - the one that was undeleted.  This method
          // updatas all children of "parentId", but it should be relatively quick anyway.  If it becomes
          // a problem, this is a potential optimization.
          updateChildSystemsFromParent(db, tenantId, parentId);
        }
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "sytems", id);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Update owner of a system given system Id and new owner name
   *
   */
  @Override
  public void updateSystemOwner(ResourceRequestUser rUser, String id, String oldOwner, String newOwner) throws TapisException
  {
    String opName = "changeOwner";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(id)) LibUtils.logAndThrowNullParmException(opName, "systemId");
    if (StringUtils.isBlank(newOwner)) LibUtils.logAndThrowNullParmException(opName, "newOwnerName");

    String tenant = rUser.getOboTenantId();

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.update(SYSTEMS)
              .set(SYSTEMS.OWNER, newOwner)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenant),SYSTEMS.ID.eq(id)).execute();
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "systems", id);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  @Override
  public void removeParentId(ResourceRequestUser rUser, String tenantId, String childSystemId) throws TapisException {
    String opName = "removeParentId";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(childSystemId)) {
      LibUtils.logAndThrowNullParmException(opName, "systemId");
    }

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.update(SYSTEMS)
              .set(SYSTEMS.PARENT_ID, (String)null)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(childSystemId), SYSTEMS.DELETED.eq(false)).execute();

      String changeDescription = "{\"parentId\":\"\"}";
      addUpdate(db, rUser, childSystemId, INVALID_SEQ_ID, SystemOperation.modify, changeDescription , null, getUUIDUsingDb(db, tenantId, childSystemId));

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "systems", childSystemId);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  @Override
  public int removeParentIdFromChildren(ResourceRequestUser rUser, String tenantId, String parentSystemId, List<String> childSystemsToUnlink)
          throws TapisException {
    String opName = "removeParentId";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(parentSystemId)) {
      LibUtils.logAndThrowNullParmException(opName, "systemId");
    }

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      List<String> childIds = db.update(SYSTEMS)
              .set(SYSTEMS.PARENT_ID, (String)null)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId), SYSTEMS.PARENT_ID.eq(parentSystemId), SYSTEMS.ID.in(childSystemsToUnlink), SYSTEMS.DELETED.eq(false))
              .returningResult(SYSTEMS.ID).fetch(SYSTEMS.ID);


      if(childIds != null) {
        for (String childId : childIds) {
          String changeDescription = "{\"parentId\":\"\"}";
          addUpdate(db, rUser, childId, INVALID_SEQ_ID, SystemOperation.modify, changeDescription , null, getUUIDUsingDb(db, tenantId, childId));
        }
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
      return childIds.size();
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "systems", parentSystemId);
      return 0;
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  @Override
  public int removeParentIdFromAllChildren(ResourceRequestUser rUser, String tenantId, String parentSystemId)
          throws TapisException {
    String opName = "removeParentId";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(parentSystemId)) {
      LibUtils.logAndThrowNullParmException(opName, "systemId");
    }

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      List<String> childIds = db.update(SYSTEMS)
              .set(SYSTEMS.PARENT_ID, (String)null)
              .set(SYSTEMS.UPDATED, TapisUtils.getUTCTimeNow())
              .where(SYSTEMS.TENANT.eq(tenantId), SYSTEMS.PARENT_ID.eq(parentSystemId), SYSTEMS.DELETED.eq(false))
              .returningResult(SYSTEMS.ID).fetch(SYSTEMS.ID);


      if(childIds != null) {
        for (String childId : childIds) {
          String changeDescription = "{\"parentId\":\"\"}";
          addUpdate(db, rUser, childId, INVALID_SEQ_ID, SystemOperation.modify, changeDescription , null, getUUIDUsingDb(db, tenantId, childId));
        }
      }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
      return childIds.size();
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_UPDATE_FAILURE", "systems", parentSystemId);
      return 0;
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Hard delete a system record given the system name.
   */
  @Override
  public int hardDeleteSystem(String tenantId, String id) throws TapisException
  {
    String opName = "hardDeleteSystem";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(tenantId)) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(id)) LibUtils.logAndThrowNullParmException(opName, "name");

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.deleteFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).execute();
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems");
    }
    finally
    {
      LibUtils.finalCloseDB(conn);
    }
    return 1;
  }

  /**
   * checkDB
   * Check that we can connect with DB and that the main table of the service exists.
   * @return null if all OK else return an exception
   */
  @Override
  public Exception checkDB()
  {
    Exception result = null;
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // execute SELECT to_regclass('tapis_sys.systems');
      // Build and execute a simple postgresql statement to check for the table
      String sql = "SELECT to_regclass('" + SYSTEMS.getName() + "')";
      Result<Record> ret = db.resultQuery(sql).fetch();
      if (ret == null || ret.isEmpty() || ret.getValue(0,0) == null)
      {
        result = new TapisException(LibUtils.getMsg("SYSLIB_CHECKDB_NO_TABLE", SYSTEMS.getName()));
      }
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      result = e;
    }
    finally
    {
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * migrateDB
   * Use Flyway to make sure DB schema is at the latest version
   */
  @Override
  public void migrateDB() throws TapisException
  {
    Flyway flyway = Flyway.configure().dataSource(getDataSource()).load();
    // Use repair as workaround to avoid checksum error during develop/deploy of SNAPSHOT versions when it is not
    // a true migration.
//    flyway.repair();
    flyway.migrate();
  }

  /**
   * checkForSystem
   * @param id - system name
   * @return true if found else false
   * @throws TapisException - on error
   */
  @Override
  public boolean checkForSystem(String tenantId, String id, boolean includeDeleted) throws TapisException {
    // Initialize result.
    boolean result = false;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // Run the sql
      result = checkForSystem(db, tenantId, id, includeDeleted);
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_SELECT_NAME_ERROR", "System", tenantId, id, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * check to see if the system with id equal systemId has child systms (that are not deleted).
   * @param tenantId - id of the tenant
   * @param systemId - id of the system
   * @return true if the system has child systsms (that have not been deleted), or false if not.
   * @throws TapisException
   */
  @Override
  public boolean hasChildren(String tenantId, String systemId) throws TapisException {
    // Initialize result.
    boolean result = false;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // Run the sql
      Boolean b = db.selectFrom(SYSTEMS)
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.PARENT_ID.eq(systemId),SYSTEMS.DELETED.eq(false))
              .limit(1)
              .fetchOne(SYSTEMS.ENABLED);
      if (b != null) {
        result = b;
      }
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "System", tenantId, systemId, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * isEnabled - check if resource with specified Id is enabled
   * @param sysId - system name
   * @return true if enabled else false
   * @throws TapisException - on error
   */
  @Override
  public boolean isEnabled(String tenantId, String sysId) throws TapisException {
    // Initialize result.
    boolean result = false;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // Run the sql
      Boolean b = db.selectFrom(SYSTEMS)
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(sysId),SYSTEMS.DELETED.eq(false))
              .fetchOne(SYSTEMS.ENABLED);
      if (b != null) result = b;
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "System", tenantId, sysId, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * getParent - gets the id of the parent system for the system with id sysId
   * @param sysId - system name
   * @return id of the parent or null if the system has no parent.
   * @throws TapisException - on error
   */
  @Override
  public String getParent(String tenantId, String sysId) throws TapisException {
    // Initialize result.
    String result = null;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // Run the sql
      result = db.selectFrom(SYSTEMS)
              .where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(sysId),SYSTEMS.DELETED.eq(false))
              .fetchOne(SYSTEMS.PARENT_ID);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "System", tenantId, sysId, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  /**
   * getSystem
   * @param id - system name
   * @return System object if found, null if not found
   * @throws TapisException - on error
   */
  @Override
  public TSystem getSystem(String tenantId, String id) throws TapisException
  {
    return getSystem(tenantId, id, false);
  }

  /**
   * getSystem
   * @param id - system name
   * @return System object if found, null if not found
   * @throws TapisException - on error
   */
  @Override
  public TSystem getSystem(String tenantId, String id, boolean includeDeleted)
          throws TapisException
  {
    // Initialize result.
    TSystem result = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      SystemsRecord r;
      if (includeDeleted)
        r = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).fetchOne();
      else
        r = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id),SYSTEMS.DELETED.eq(false)).fetchOne();
      if (r == null) return null;
      else result = getSystemFromRecord(r);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "System", tenantId, id, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return result;
  }

  private TSystem getSystemForUpdate(DSLContext db, String tenantId, String id)
          throws TapisException {
    // Initialize result.
    TSystem result = null;

    // ------------------------- Call SQL ----------------------------
    SystemsRecord r;
    r = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId), SYSTEMS.ID.eq(id), SYSTEMS.DELETED.eq(false)).forUpdate().fetchOne();
    if (r == null) {
      return null;
    } else {
      result = getSystemFromRecord(r);
    }

    return result;
  }

  /**
   * getSystemsCount
   * Count all TSystems matching various search and sort criteria.
   *     Search conditions given as a list of strings or an abstract syntax tree (AST).
   * Conditions in searchList must be processed by SearchUtils.validateAndExtractSearchCondition(cond)
   *   prior to this call for proper validation and treatment of special characters.
   * WARNING: If both searchList and searchAST provided only searchList is used.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param searchAST - AST containing search conditions
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param startAfter - where to start when sorting, e.g. orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param includeDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param viewableIDs - list of IDs to include due to permission READ or MODIFY
   * @param sharedIDs - list of IDs shared with the requester or only shared publicly.
   * @return - count of items
   * @throws TapisException - on error
   */
  @Override
  public int getSystemsCount(ResourceRequestUser rUser, List<String> searchList, ASTNode searchAST,
                             List<OrderBy> orderByList, String startAfter, boolean includeDeleted,
                             AuthListType listType, Set<String> viewableIDs, Set<String> sharedIDs)
          throws TapisException
  {
    // For convenience
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    boolean allItems = AuthListType.ALL.equals(listType);
    boolean publicOnly = AuthListType.SHARED_PUBLIC.equals(listType);

    // If only looking for public items and there are none in the list we are done.
    if (publicOnly && (sharedIDs == null || sharedIDs.isEmpty())) return 0;

    // Ensure we have a valid listType
    if (listType == null) listType = DEFAULT_LIST_TYPE;

    // Ensure we have a non-null orderByList
    List<OrderBy> tmpOrderByList = new ArrayList<>();
    if (orderByList != null) tmpOrderByList = orderByList;

    // Determine the primary orderBy column (i.e. first in list). Used for startAfter
    String majorOrderByStr = null;
    OrderByDir majorSortDirection = DEFAULT_ORDERBY_DIRECTION;
    if (!tmpOrderByList.isEmpty())
    {
      majorOrderByStr = tmpOrderByList.get(0).getOrderByAttr();
      majorSortDirection = tmpOrderByList.get(0).getOrderByDir();
    }

    // Determine if we are doing an asc sort, important for startAfter
    boolean sortAsc = majorSortDirection != OrderByDir.DESC;

    // If startAfter is given then orderBy is required
    if (!StringUtils.isBlank(startAfter) && StringUtils.isBlank(majorOrderByStr))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SORT_START", SYSTEMS.getName()));
    }

    // Validate orderBy columns
    // If orderBy column not found then it is an error
    // For count we do not need the actual column, so we just check that the column exists.
    //   Down below in getSystems() we need the actual column
    for (OrderBy orderBy : tmpOrderByList)
    {
      String orderByStr = orderBy.getOrderByAttr();
      if (StringUtils.isBlank(orderByStr) || !SYSTEMS_FIELDS.contains(SearchUtils.camelCaseToSnakeCase(orderByStr)))
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_NO_COLUMN_SORT", SYSTEMS.getName(), DSL.name(orderByStr));
        throw new TapisException(msg);
      }
    }

    // Begin where condition for the query
    // Start with either tenant = <tenant> or
    //                   tenant = <tenant> and deleted = false
    Condition whereCondition;
    if (includeDeleted) whereCondition = SYSTEMS.TENANT.eq(oboTenant);
    else whereCondition = (SYSTEMS.TENANT.eq(oboTenant)).and(SYSTEMS.DELETED.eq(false));

    // If only selecting items owned by requester we add the condition now.
    if (AuthListType.OWNED.equals(listType)) whereCondition = whereCondition.and(SYSTEMS.OWNER.eq(oboUser));

    // Add searchList or searchAST to where condition
    if (searchList != null)
    {
      whereCondition = addSearchListToWhere(whereCondition, searchList);
    }
    else if (searchAST != null)
    {
      Condition astCondition = createConditionFromAst(searchAST);
      if (astCondition != null) whereCondition = whereCondition.and(astCondition);
    }

    // Add startAfter.
    if (!StringUtils.isBlank(startAfter))
    {
      // Build search string, so we can re-use code for checking and adding a condition
      String searchStr;
      if (sortAsc) searchStr = majorOrderByStr + ".gt." + startAfter;
      else searchStr = majorOrderByStr + ".lt." + startAfter;
      whereCondition = addSearchCondStrToWhere(whereCondition, searchStr, "AND");
    }

    // If selecting allItems or publicOnly, add IN condition
    var setOfIDs = new HashSet<String>();
    if (allItems && viewableIDs != null) setOfIDs.addAll(viewableIDs);
    if (allItems && sharedIDs != null) setOfIDs.addAll(sharedIDs);
    if (publicOnly && sharedIDs != null) setOfIDs.addAll(sharedIDs);

    if (!setOfIDs.isEmpty()) whereCondition = whereCondition.and(SYSTEMS.ID.in(setOfIDs));

    // ------------------------- Build and execute SQL ----------------------------
    int count = 0;
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Execute the select including startAfter
      // NOTE: This is much simpler than the same section in getSystems() because we are not ordering since
      //       we only want the count, and we are not limiting (we want a count of all records).
      Integer countInt = db.selectCount().from(SYSTEMS).where(whereCondition).fetchOne(0,Integer.class);
      count = (countInt == null) ? 0 : countInt;

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return count;
  }

  /**
   * getSystems
   * Retrieve all TSystems matching various search and sort criteria.
   *     Search conditions given as a list of strings or an abstract syntax tree (AST).
   * Conditions in searchList must be processed by SearchUtils.validateAndExtractSearchCondition(cond)
   *   prior to this call for proper validation and treatment of special characters.
   * WARNING: If both searchList and searchAST provided only searchList is used.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param searchList - optional list of conditions used for searching
   * @param searchAST - AST containing search conditions
   * @param limit - indicates maximum number of results to be included, -1 for unlimited
   * @param orderByList - orderBy entries for sorting, e.g. orderBy=created(desc).
   * @param skip - number of results to skip (may not be used with startAfter)
   * @param startAfter - where to start when sorting, e.g. limit=10&orderBy=id(asc)&startAfter=101 (may not be used with skip)
   * @param includeDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param viewableIDs - list of IDs to include due to permission READ or MODIFY
   * @param sharedIDs - list of IDs shared with the requester or only shared publicly.
   * @return - list of TSystem objects
   * @throws TapisException - on error
   */
  @Override
  public List<TSystem> getSystems(ResourceRequestUser rUser, String oboUser,
                                  List<String> searchList, ASTNode searchAST, int limit,
                                  List<OrderBy> orderByList, int skip, String startAfter, boolean includeDeleted,
                                  AuthListType listType, Set<String> viewableIDs, Set<String> sharedIDs)
          throws TapisException
  {
    // The result list should always be non-null.
    List<TSystem> retList = new ArrayList<>();

    // Ensure we have a valid listType
    if (listType == null) listType = DEFAULT_LIST_TYPE;

    // Ensure we have a valid oboUser
    if (StringUtils.isBlank(oboUser)) oboUser = rUser.getOboUserId();

    // For convenience
    String oboTenant = rUser.getOboTenantId();
    boolean allItems = AuthListType.ALL.equals(listType);
    boolean publicOnly = AuthListType.SHARED_PUBLIC.equals(listType);
    boolean ownedOnly = AuthListType.OWNED.equals(listType);

    // If only looking for public items and there are none in the list we are done.
    if (publicOnly && (sharedIDs == null || sharedIDs.isEmpty())) return retList;

    // Ensure we have a non-null orderByList
    List<OrderBy> tmpOrderByList = new ArrayList<>();
    if (orderByList != null) tmpOrderByList = orderByList;

    // Determine the primary orderBy column (i.e. first in list). Used for startAfter
    String majorOrderByStr = null;
    OrderByDir majorSortDirection = DEFAULT_ORDERBY_DIRECTION;
    if (!tmpOrderByList.isEmpty())
    {
      majorOrderByStr = tmpOrderByList.get(0).getOrderByAttr();
      majorSortDirection = tmpOrderByList.get(0).getOrderByDir();
    }

    // Negative skip indicates no skip
    if (skip < 0) skip = 0;

    // Determine if we are doing an asc sort, important for startAfter
    boolean sortAsc = majorSortDirection != OrderByDir.DESC;

    // If startAfter is given then orderBy is required
    if (!StringUtils.isBlank(startAfter) && StringUtils.isBlank(majorOrderByStr))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SORT_START", SYSTEMS.getName()));
    }

// DEBUG Iterate over all columns and show the type
//      Field<?>[] cols = SYSTEMS.fields();
//      for (Field<?> col : cols) {
//        var dataType = col.getDataType();
//        int sqlType = dataType.getSQLType();
//        String sqlTypeName = dataType.getTypeName();
//        _log.debug("Column name: " + col.getName() + " type: " + sqlTypeName);
//      }
// DEBUG

    // Determine and check orderBy columns, build orderFieldList
    // Each OrderField contains the column and direction
    List<OrderField> orderFieldList = new ArrayList<>();
    for (OrderBy orderBy : tmpOrderByList)
    {
      String orderByStr = orderBy.getOrderByAttr();
      Field<?> colOrderBy = SYSTEMS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(orderByStr)));
      if (StringUtils.isBlank(orderByStr) || colOrderBy == null)
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_NO_COLUMN_SORT", SYSTEMS.getName(), DSL.name(orderByStr));
        throw new TapisException(msg);
      }
      if (orderBy.getOrderByDir() == OrderBy.OrderByDir.ASC) orderFieldList.add(colOrderBy.asc());
      else orderFieldList.add(colOrderBy.desc());
    }

    // Begin where condition for the query
    Condition whereCondition;
    if (includeDeleted) whereCondition = SYSTEMS.TENANT.eq(oboTenant);
    else whereCondition = (SYSTEMS.TENANT.eq(oboTenant)).and(SYSTEMS.DELETED.eq(false));

    // Add searchList or searchAST to where condition
    if (searchList != null)
    {
      whereCondition = addSearchListToWhere(whereCondition, searchList);
    }
    else if (searchAST != null)
    {
      Condition astCondition = createConditionFromAst(searchAST);
      if (astCondition != null) whereCondition = whereCondition.and(astCondition);
    }

    // Add startAfter
    if (!StringUtils.isBlank(startAfter))
    {
      // Build search string, so we can re-use code for checking and adding a condition
      String searchStr;
      if (sortAsc) searchStr = majorOrderByStr + ".gt." + startAfter;
      else searchStr = majorOrderByStr + ".lt." + startAfter;
      whereCondition = addSearchCondStrToWhere(whereCondition, searchStr, "AND");
    }

    // Build and add the listType condition:
    //  OWNED = single condition where owner = oboUser
    //  PUBLIC = single condition where id in setOfIDs
    //  ALL = where (owner = oboUser) OR (id in setOfIDs)
    Condition listTypeCondition = null;
    if (ownedOnly)
    {
      listTypeCondition = SYSTEMS.OWNER.eq(oboUser);
    }
    else if (publicOnly)
    {
      // NOTE: We check above for sharedIDs == null or is empty so no need to do it here
      listTypeCondition = SYSTEMS.ID.in(sharedIDs);
    }
    else if (allItems)
    {
      listTypeCondition = SYSTEMS.OWNER.eq(oboUser);
      var setOfIDs = new HashSet<String>();
      if (sharedIDs != null && !sharedIDs.isEmpty()) setOfIDs.addAll(sharedIDs);
      if (viewableIDs != null && !viewableIDs.isEmpty()) setOfIDs.addAll(viewableIDs);
      if (!setOfIDs.isEmpty())
      {
        listTypeCondition = listTypeCondition.or(SYSTEMS.ID.in(setOfIDs));
      }
    }
    whereCondition = whereCondition.and(listTypeCondition);

    // ------------------------- Build and execute SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Execute the select including limit, orderByAttrList, skip and startAfter
      // NOTE: LIMIT + OFFSET is not standard among DBs and often very difficult to get right.
      //       Jooq claims to handle it well.
      Result<SystemsRecord> results;
      org.jooq.SelectConditionStep condStep = db.selectFrom(SYSTEMS).where(whereCondition);
      if (!StringUtils.isBlank(majorOrderByStr) &&  limit >= 0)
      {
        // We are ordering and limiting
        results = condStep.orderBy(orderFieldList).limit(limit).offset(skip).fetch();
      }
      else if (!StringUtils.isBlank(majorOrderByStr))
      {
        // We are ordering but not limiting
        results = condStep.orderBy(orderFieldList).fetch();
      }
      else if (limit >= 0)
      {
        // We are limiting but not ordering
        results = condStep.limit(limit).offset(skip).fetch();
      }
      else
      {
        // We are not limiting and not ordering
        results = condStep.fetch();
      }

      if (results == null || results.isEmpty()) return retList;

      // Fill in batch logical queues and job capabilities list from aux tables
      // NOTE: Looks like jOOQ has fetchGroups() which should allow us to retrieve LogicalQueues and Capabilities
      //       in one call which might improve performance.
//      for (SystemsRecord r : results)
//      {
//        TSystem s = r.into(TSystem.class);
//        s.setJobRuntimes(retrieveJobRuntimes(db, s.getSeqId()));
//        s.setBatchLogicalQueues(retrieveLogicalQueues(db, s.getSeqId()));
//        s.setJobCapabilities(retrieveJobCaps(db, s.getSeqId()));
//        retList.add(s);
//      }

      for (SystemsRecord r : results) { TSystem s = getSystemFromRecord(r); retList.add(s); }

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return retList;
  }

  /**
   * getSystemIDs
   * Fetch all system IDs in a tenant
   * @param tenant - tenant name
   * @param includeDeleted - whether to included systems that have been marked as deleted.
   * @return - List of system names
   * @throws TapisException - on error
   */
  @Override
  public Set<String> getSystemIDs(String tenant, boolean includeDeleted) throws TapisException
  {
    // The result list is always non-null.
    var idList = new HashSet<String>();

    Condition whereCondition;
    if (includeDeleted) whereCondition = SYSTEMS.TENANT.eq(tenant);
    else whereCondition = (SYSTEMS.TENANT.eq(tenant)).and(SYSTEMS.DELETED.eq(false));

    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      // ------------------------- Call SQL ----------------------------
      // Use jOOQ to build query string
      DSLContext db = DSL.using(conn);
      Result<?> result = db.select(SYSTEMS.ID).from(SYSTEMS).where(whereCondition).fetch();
      // Iterate over result
      for (Record r : result) { idList.add(r.get(SYSTEMS.ID)); }
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return idList;
  }

  /**
   * getSystemsSatisfyingConstraints
   * Retrieve all TSystems satisfying capability constraint criteria.
   *     Constraint criteria conditions provided as an abstract syntax tree (AST).
   * @param tenantId - tenant name
   * @param matchAST - AST containing match conditions. If null then nothing matches.
   * @param setOfIDs - list of system IDs to consider. If null all allowed. If empty none allowed.
   * @return - list of TSystem objects
   * @throws TapisException - on error
   */
  @Override
  public List<TSystem> getSystemsSatisfyingConstraints(String tenantId, ASTNode matchAST, Set<String> setOfIDs)
          throws TapisException
  {
    // NOTE: might be possible to optimize this method with a join between systems and capabilities tables.
    // The result list should always be non-null.
    var retList = new ArrayList<TSystem>();

    // If no match criteria or IDs list is empty then we are done.
    if (matchAST == null || (setOfIDs != null && setOfIDs.isEmpty())) return retList;

    // NOTE/TBD: For now return all allowed systems. Once a shared util method is available for matching
    //       as a first pass we can simply iterate through all systems to find matches.
    //       For performance might need to later do matching with DB queries.

    // Get all desired capabilities (category, name) from AST
    List<Capability> capabilitiesInAST = new ArrayList<>();
    getCapabilitiesFromAST(matchAST, capabilitiesInAST);

    List<TSystem> systemsList = null;
    // ------------------------- Build and execute SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      Set<String> allowedIDs = setOfIDs;
      // If IDs is null then all allowed. Use tenant to get all system IDs
      // NOTE: might be able to optimize with a join somewhere
      if (setOfIDs == null) allowedIDs = null; // NOTE is getAllSystemSeqIdsInTenant(db, tenant) still needed?

      // Get all Systems that specify they support the desired Capabilities
      systemsList = getSystemsHavingCapabilities(db, tenantId, capabilitiesInAST, allowedIDs);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }

    // If there was a problem the list to match against might be null
    if (systemsList == null) return retList;

    // Select only those systems satisfying the constraints
    for (TSystem sys : systemsList)
    {
// TBD      if (systemMatchesConstraints(sys, matchAST)) retList.add(sys);
      retList.add(sys);
    }
    return retList;
  }

  /**
   * getSystemOwner
   * @param tenantId - name of tenant
   * @param id - name of system
   * @return Owner or null if no system found
   * @throws TapisException - on error
   */
  @Override
  public String getSystemOwner(String tenantId, String id) throws TapisException
  {
    String owner = null;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      owner = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).fetchOne(SYSTEMS.OWNER);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return owner;
  }

  /**
   * getSystemEffectiveUserId
   * @param tenantId - name of tenant
   * @param id - name of system
   * @return EffectiveUserId or null if no system found
   * @throws TapisException - on error
   */
  @Override
  public String getSystemEffectiveUserId(String tenantId, String id) throws TapisException
  {
    String effectiveUserId = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      effectiveUserId = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).fetchOne(SYSTEMS.EFFECTIVE_USER_ID);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return effectiveUserId;
  }

  /**
   * getSystemDefaultAuthnMethod
   * @param tenantId - name of tenant
   * @param id - name of system
   * @return Default AuthnMethod or null if no system found
   * @throws TapisException - on error
   */
  @Override
  public AuthnMethod getSystemDefaultAuthnMethod(String tenantId, String id) throws TapisException
  {
    AuthnMethod authnMethod = null;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      authnMethod = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).fetchOne(SYSTEMS.DEFAULT_AUTHN_METHOD);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "systems", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return authnMethod;
  }

  /**
   * Add an update record given the system Id and operation type
   *
   */
  @Override
  public void addUpdateRecord(ResourceRequestUser rUser, String sysId, SystemOperation op, String changeDescription, String rawData)
          throws TapisException
  {
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      addUpdate(db, rUser, sysId, INVALID_SEQ_ID, op, changeDescription, rawData, getUUIDUsingDb(db, rUser.getOboTenantId(), sysId));

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * getLoginUser
   * Given a System Id and a tapisUser get the mapping to the loginUser if the map table has an entry.
   * If there is no mapping return null
   * @param id - system name
   * @param tapisUser - Tapis username
   * @return loginUser or null if no mapping
   * @throws TapisException - on error
   */
  @Override
  public String getLoginUser(String tenantId, String id, String tapisUser) throws TapisException
  {
    // Initialize result.
    String loginUser = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      // Run the sql
      loginUser = db.selectFrom(SYSTEMS_LOGIN_USER)
              .where(SYSTEMS_LOGIN_USER.TENANT.eq(tenantId),SYSTEMS_LOGIN_USER.SYSTEM_ID.eq(id),SYSTEMS_LOGIN_USER.TAPIS_USER.eq(tapisUser))
              .fetchOne(SYSTEMS_LOGIN_USER.LOGIN_USER);
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_SELECT_NAME_ERROR", "System_login_user", tenantId, id, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return loginUser;
  }

  /**
   * Create a new mapping for tapisUser to loginUser
   */
  @Override
  public void createOrUpdateLoginUserMapping(String tenantId, String systemId, String tapisUser, String loginUser) throws TapisException
  {

    if (StringUtils.isBlank(tenantId) || StringUtils.isBlank(systemId) || StringUtils.isBlank(tapisUser) ||
        StringUtils.isBlank(loginUser))
    {
      return;
    }
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      boolean recordExists = db.fetchExists(SYSTEMS_LOGIN_USER,SYSTEMS_LOGIN_USER.TENANT.eq(tenantId),
                                            SYSTEMS_LOGIN_USER.SYSTEM_ID.eq(systemId),
                                            SYSTEMS_LOGIN_USER.TAPIS_USER.eq(tapisUser));
      // If record not there insert it, else update it
      if (!recordExists)
      {
        log.debug(LibUtils.getMsg("SYSLIB_CRED_DB_INSERT_LOGINMAP", tenantId, systemId, tapisUser, loginUser));
        int sysSeqId = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(systemId)).fetchOne(SYSTEMS.SEQ_ID);
        db.insertInto(SYSTEMS_LOGIN_USER)
                .set(SYSTEMS_LOGIN_USER.SYSTEM_SEQ_ID, sysSeqId)
                .set(SYSTEMS_LOGIN_USER.TENANT, tenantId)
                .set(SYSTEMS_LOGIN_USER.SYSTEM_ID, systemId)
                .set(SYSTEMS_LOGIN_USER.TAPIS_USER, tapisUser)
                .set(SYSTEMS_LOGIN_USER.LOGIN_USER, loginUser)
                .execute();
      }
      else
      {
        log.debug(LibUtils.getMsg("SYSLIB_CRED_DB_UPDATE_LOGINMAP", tenantId, systemId, tapisUser, loginUser));
        db.update(SYSTEMS_LOGIN_USER)
                .set(SYSTEMS_LOGIN_USER.LOGIN_USER, loginUser)
                .where(SYSTEMS_LOGIN_USER.TENANT.eq(tenantId),
                       SYSTEMS_LOGIN_USER.SYSTEM_ID.eq(systemId),
                       SYSTEMS_LOGIN_USER.TAPIS_USER.eq(tapisUser))
                .execute();
      }
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "systems_login_user");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * Delete a mapping entry for tapisUser to loginUser
   */
  @Override
  public void deleteLoginUserMapping(ResourceRequestUser rUser, String tenantId, String sysId, String tapisUser)
          throws TapisException
  {
    // If anything missing throw an exception. These values make up the primary key
    if (StringUtils.isBlank(tenantId) || StringUtils.isBlank(sysId) || StringUtils.isBlank(tapisUser))
    {
      throw new TapisException(LibUtils.getMsgAuth("SYSLIB_DB_DEL_LOGINMAP_ERR", rUser, tenantId, sysId, tapisUser));
    }
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.deleteFrom(SYSTEMS_LOGIN_USER)
              .where(SYSTEMS_LOGIN_USER.TENANT.eq(tenantId),SYSTEMS_LOGIN_USER.SYSTEM_ID.eq(sysId),SYSTEMS_LOGIN_USER.TAPIS_USER.eq(tapisUser))
              .execute();
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "systems_login_user");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /* ********************************************************************** */
  /*                         Scheduler Profile Methods                      */
  /* ********************************************************************** */

  /**
   * Create a new scheduler profile
   *
   * @throws TapisException - on error
   * @throws IllegalStateException - if profile already exists
   */
  @Override
  public void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, IllegalStateException
  {
    String opName = "createSchedulerProfile";
    // ------------------------- Check Input -------------------------
    if (schedulerProfile == null) LibUtils.logAndThrowNullParmException(opName, "schedulerProfile");
    if (rUser == null) LibUtils.logAndThrowNullParmException(opName, "resourceRequestUser");
    if (StringUtils.isBlank(schedulerProfile.getTenant())) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(schedulerProfile.getName())) LibUtils.logAndThrowNullParmException(opName, "schedulerProfileName");

    String tenantId = schedulerProfile.getTenant();
    String name = schedulerProfile.getName();
    var moduleLoads = schedulerProfile.getModuleLoads();

    // Make sure owner is set
    String owner = TSystem.DEFAULT_OWNER;
    if (StringUtils.isNotBlank(schedulerProfile.getOwner())) owner = schedulerProfile.getOwner();

    // Do some pre-processing of hiddenOptions
    // Convert hiddenOptions array from enum to string
    String[] hiddenOptionsStrArray = null;
    if (schedulerProfile.getHiddenOptions() != null)
    {
      hiddenOptionsStrArray = schedulerProfile.getHiddenOptions().stream().map(SchedulerProfile.HiddenOption::name).toArray(String[]::new);
    }

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      // Check to see if it exists. If yes then throw IllegalStateException
      if (db.fetchExists(SCHEDULER_PROFILES,SCHEDULER_PROFILES.TENANT.eq(tenantId),SCHEDULER_PROFILES.NAME.eq(name)))
        throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_PRF_EXISTS", rUser, schedulerProfile.getName()));

      // Generate uuid for the new resource
      schedulerProfile.setUuid(UUID.randomUUID());

      // Create the record in the main table.
      var record = db.insertInto(SCHEDULER_PROFILES)
              .set(SCHEDULER_PROFILES.TENANT, schedulerProfile.getTenant())
              .set(SCHEDULER_PROFILES.NAME, schedulerProfile.getName())
              .set(SCHEDULER_PROFILES.DESCRIPTION, schedulerProfile.getDescription())
              .set(SCHEDULER_PROFILES.OWNER, owner)
              .set(SCHEDULER_PROFILES.HIDDEN_OPTIONS, hiddenOptionsStrArray)
              .set(SCHEDULER_PROFILES.UUID, schedulerProfile.getUuid())
              .returningResult(SCHEDULER_PROFILES.SEQ_ID).fetchOne();
      // If record is null it is an error
      if (record == null)
      {
        throw new TapisException(LibUtils.getMsgAuth("SYSLIB_DB_NULL_RESULT", rUser, schedulerProfile.getName(), opName));
      }
      // Generated sequence id
      int seqId = record.getValue(SCHEDULER_PROFILES.SEQ_ID);

      // Create any moduleLoadSpec records in the aux table.
      if (moduleLoads != null && !moduleLoads.isEmpty())
      {
        for (ModuleLoadSpec m : moduleLoads)
        {
          String[] modulesToLoadStrArray = null;
          if (m.getModulesToLoad() != null) modulesToLoadStrArray = m.getModulesToLoad();
          db.insertInto(SCHED_PROFILE_MOD_LOAD)
                   .set(SCHED_PROFILE_MOD_LOAD.SCHED_PROFILE_SEQ_ID, seqId)
                   .set(SCHED_PROFILE_MOD_LOAD.TENANT, schedulerProfile.getTenant())
                   .set(SCHED_PROFILE_MOD_LOAD.SCHED_PROFILE_NAME, schedulerProfile.getName())
                   .set(SCHED_PROFILE_MOD_LOAD.MODULE_LOAD_COMMAND, m.getModuleLoadCommand())
                   .set(SCHED_PROFILE_MOD_LOAD.MODULES_TO_LOAD, modulesToLoadStrArray).execute();
        }
      }
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_INSERT_FAILURE", "scheduler_profiles");
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * getSchedulerProfile
   * @param name - system name
   * @return Profile object if found, null if not found
   * @throws TapisException - on error
   */
  @Override
  public SchedulerProfile getSchedulerProfile(String tenantId, String name) throws TapisException
  {
    // Initialize result.
    SchedulerProfile sp = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      SchedulerProfilesRecord r;
      r = db.selectFrom(SCHEDULER_PROFILES).where(SCHEDULER_PROFILES.TENANT.eq(tenantId),SCHEDULER_PROFILES.NAME.eq(name)).fetchOne();
      if (r == null) return null;
//      else result = r.into(SchedulerProfile.class);
      // Convert type for list of hidden options.
      // NOTE: Currently the custom conversion config for jOOQ does not seem to work for an array of enums.
      //       See tapis-systemslib/pom.xml
      // So for now manually create a SchedulerProfile from the query result.
      List<SchedulerProfile.HiddenOption> hoList2 = null;
      String[] hoList1 = r.getHiddenOptions();
      if (hoList1 != null)
      {
        hoList2 = new ArrayList<>();
        for (String ho : hoList1) { hoList2.add(SchedulerProfile.HiddenOption.valueOf(ho)); }
      }

      // Fetch any moduleLoadSpec records
      List<ModuleLoadSpec> moduleLoads = getSchedulerProfileModuleLoads(db, tenantId, name);

      sp = new SchedulerProfile(r.getTenant(), r.getName(), r.getDescription(), r.getOwner(), moduleLoads, hoList2,
                                r.getUuid(), r.getCreated().toInstant(ZoneOffset.UTC), r.getUpdated().toInstant(ZoneOffset.UTC));
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "SchedulerProfile", tenantId, name, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return sp;
  }

  /**
   * getSchedulerProfiles
   * @return list of scheduler profiles
   * @throws TapisException - on error
   */
  @Override
  public List<SchedulerProfile> getSchedulerProfiles(String tenantId) throws TapisException
  {
    List<SchedulerProfile> retList1;
    var retList2 = new ArrayList<SchedulerProfile>();
    // ------------------------- Build and execute SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      var records = db.selectFrom(SCHEDULER_PROFILES).where(SCHEDULER_PROFILES.TENANT.eq(tenantId)).fetch();
      if (records == null || records.isEmpty()) return Collections.emptyList();

      for (SchedulerProfilesRecord spr : records)
      {
        String[] hoList1 = spr.getHiddenOptions();
        // Convert type for list of hidden options.
        // NOTE: Currently the custom conversion config for jOOQ does not seem to work for an array of enums.
        //       See tapis-systemslib/pom.xml
        // So for now manually create a SchedulerProfile from the query result.
        List<SchedulerProfile.HiddenOption> hoList2 = null;
        if (hoList1 != null)
        {
          hoList2 = new ArrayList<>();
          for (String hoStr : hoList1) { hoList2.add(SchedulerProfile.HiddenOption.valueOf(hoStr));
          }
        }
        // Fetch any moduleLoadSpec records
        List<ModuleLoadSpec> moduleLoads = getSchedulerProfileModuleLoads(db, tenantId, spr.getName());

        // Create the scheduler profile and add it to the list
        SchedulerProfile sp2 = new SchedulerProfile(spr.getTenant(), spr.getName(), spr.getDescription(),
                                                    spr.getOwner(), moduleLoads, hoList2, spr.getUuid(),
                                                    spr.getCreated().toInstant(ZoneOffset.UTC),
                                                    spr.getUpdated().toInstant(ZoneOffset.UTC));
        retList2.add(sp2);
      }
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "scheduler_profiles", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return retList2;
  }

  /**
   * Delete a scheduler profile.
   */
  @Override
  public int deleteSchedulerProfile(String tenantId, String name) throws TapisException
  {
    String opName = "deleteSchedulerProfile";
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(tenantId)) LibUtils.logAndThrowNullParmException(opName, "tenant");
    if (StringUtils.isBlank(name)) LibUtils.logAndThrowNullParmException(opName, "name");

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      db.deleteFrom(SCHEDULER_PROFILES).where(SCHEDULER_PROFILES.TENANT.eq(tenantId),SCHEDULER_PROFILES.NAME.eq(name)).execute();
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      LibUtils.rollbackDB(conn, e,"DB_DELETE_FAILURE", "scheduler_profiles");
    }
    finally
    {
      LibUtils.finalCloseDB(conn);
    }
    return 1;
  }

  /**
   * checkForSchedulerProfile
   * @param name - name of profile
   * @return true if found else false
   * @throws TapisException - on error
   */
  @Override
  public boolean checkForSchedulerProfile(String tenantId, String name) throws TapisException
  {
    Connection conn = null;
    try
    {
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      return db.fetchExists(SCHEDULER_PROFILES,SCHEDULER_PROFILES.TENANT.eq(tenantId),SCHEDULER_PROFILES.NAME.eq(name));
    }
    catch (Exception e)
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_SELECT_ERROR", "SchedulerProfile", tenantId, name, e.getMessage());
      throw new TapisException(msg,e);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
  }

  /**
   * getSchedulerProfileOwner
   * @param tenant - name of tenant
   * @param name - name of profile
   * @return Owner or null if no resource found
   * @throws TapisException - on error
   */
  @Override
  public String getSchedulerProfileOwner(String tenant, String name) throws TapisException
  {
    String owner = null;
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);
      owner = db.selectFrom(SCHEDULER_PROFILES)
                .where(SCHEDULER_PROFILES.TENANT.eq(tenant),SCHEDULER_PROFILES.NAME.eq(name))
                .fetchOne(SCHEDULER_PROFILES.OWNER);

      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"DB_QUERY_ERROR", "scheduler_profiles", e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return owner;
  }

  /**
   * Get systems updates records for given system ID
   * @param systemId - System name
   * @return List of SystemHistoryItem objects
   * @throws TapisException - for Tapis related exceptions
   */
  @Override
  public List<SystemHistoryItem> getSystemHistory(String oboTenant, String systemId) throws TapisException {
    // Initialize result.
    List<SystemHistoryItem> resultList = new ArrayList<SystemHistoryItem>();

    // Begin where condition for the query
    Condition whereCondition = SYSTEM_UPDATES.OBO_TENANT.eq(oboTenant).and(SYSTEM_UPDATES.SYSTEM_ID.eq(systemId));
    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();
      DSLContext db = DSL.using(conn);

      SelectConditionStep<SystemUpdatesRecord> results;
      results = db.selectFrom(SYSTEM_UPDATES).where(whereCondition);

      for (Record r : results) { SystemHistoryItem s = getSystemHistoryFromRecord(r); resultList.add(s); }
      // Close out and commit
      LibUtils.closeAndCommitDB(conn, null, null);
    }
    catch (Exception e)
    {
      // Rollback transaction and throw an exception
      LibUtils.rollbackDB(conn, e,"SYSLIB_DB_SELECT_ERROR", "SystemUpdates", systemId, e.getMessage());
    }
    finally
    {
      // Always return the connection back to the connection pool.
      LibUtils.finalCloseDB(conn);
    }
    return resultList;
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /**
   *  Return a connection from the static datasource.  Create the datasource
   *  on demand if it doesn't exist.
   *
   * @return a database connection
   * @throws TapisException on error
   */
  private static synchronized Connection getConnection()
          throws TapisException
  {
    // Use the existing datasource.
    DataSource ds = getDataSource();

    // Get the connection.
    Connection conn = null;
    try {conn = ds.getConnection();}
    catch (Exception e) {
      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
      log.error(msg, e);
      throw new TapisDBConnectionException(msg, e);
    }

    return conn;
  }

  /* ---------------------------------------------------------------------- */
  /* getDataSource:                                                         */
  /* ---------------------------------------------------------------------- */
  private static DataSource getDataSource() throws TapisException
  {
    // Use the existing datasource.
    DataSource ds = TapisDataSource.getDataSource();
    if (ds == null) {
      try {
        // Get a database connection.
        RuntimeParameters parms = RuntimeParameters.getInstance();
        ds = TapisDataSource.getDataSource(parms.getInstanceName(),
                parms.getDbConnectionPoolName(),
                parms.getJdbcURL(),
                parms.getDbUser(),
                parms.getDbPassword(),
                parms.getDbConnectionPoolSize(),
                parms.getDbMeterMinutes());
      }
      catch (TapisException e) {
        // Details are already logged at exception site.
        String msg = MsgUtils.getMsg("DB_FAILED_DATASOURCE");
        log.error(msg, e);
        throw new TapisException(msg, e);
      }
    }

    return ds;
  }

  /**
   * Given an sql connection and basic info add a change history record
   * If seqId <= 0 then seqId is fetched.
   * NOTE: Both system tenant and user tenant are recorded. If a service makes an update on behalf of itself
   *       the tenants will differ.
   *
   * @param db - Database connection
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param id - Id of the system being updated
   * @param seqId - Sequence Id of system being updated. If < 1 it is fetched from the DB using the system id.
   * @param op - Operation, such as create, modify, etc.
   * @param changeDescriptionJson - JSON representing the update - with secrets scrubbed
   * @param rawData - Json supplied by client - secrets should be scrubbed
   */
  private void addUpdate(DSLContext db, ResourceRequestUser rUser, String id, int seqId,
                         SystemOperation op, String changeDescriptionJson, String rawData, UUID uuid)
  {
    // Make sure we have something for the description since it cannot be null.
    String updJsonStr = (StringUtils.isBlank(changeDescriptionJson)) ? EMPTY_JSON : changeDescriptionJson;
    if (seqId < 1)
    {
      seqId = db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(rUser.getOboTenantId()),SYSTEMS.ID.eq(id)).fetchOne(SYSTEMS.SEQ_ID);
    }
    // Persist update record
    db.insertInto(SYSTEM_UPDATES)
            .set(SYSTEM_UPDATES.SYSTEM_SEQ_ID, seqId)
            .set(SYSTEM_UPDATES.JWT_TENANT, rUser.getJwtTenantId())
            .set(SYSTEM_UPDATES.JWT_USER, rUser.getJwtUserId())
            .set(SYSTEM_UPDATES.OBO_TENANT, rUser.getOboTenantId())
            .set(SYSTEM_UPDATES.OBO_USER, rUser.getOboUserId())
            .set(SYSTEM_UPDATES.SYSTEM_ID, id)
            .set(SYSTEM_UPDATES.OPERATION, op)
            .set(SYSTEM_UPDATES.DESCRIPTION, TapisGsonUtils.getGson().fromJson(updJsonStr, JsonElement.class))
            .set(SYSTEM_UPDATES.RAW_DATA, rawData)
            .set(SYSTEM_UPDATES.UUID, uuid)
            .execute();
  }

  /**
   * Given an sql connection check to see if specified system exists and has/has not been deleted
   * @param db - jooq context
   * @param tenantId - name of tenant
   * @param id - name of system
   * @param includeDeleted -if deleted systems should be included
   * @return - true if system exists, else false
   */
  private static boolean checkForSystem(DSLContext db, String tenantId, String id, boolean includeDeleted)
  {
    if (includeDeleted) return db.fetchExists(SYSTEMS,SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id));
    else return db.fetchExists(SYSTEMS,SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id),SYSTEMS.DELETED.eq(false));
  }

  /**
   * Given an sql connection fetch list of ModuleLoadSpec records for a scheduler profile
   * @param db - jooq context
   * @param tenantId - name of tenant
   * @param name - name of scheduler profile
   * @return - list of ModuleLoadSpec
   */
  private static List<ModuleLoadSpec> getSchedulerProfileModuleLoads(DSLContext db, String tenantId, String name)
  {
    List<ModuleLoadSpec> moduleLoads = new ArrayList<>();
    if (db == null || StringUtils.isBlank(tenantId) || StringUtils.isBlank(name)) return moduleLoads;
    var retList = db.selectFrom(SCHED_PROFILE_MOD_LOAD)
            .where(SCHED_PROFILE_MOD_LOAD.TENANT.eq(tenantId), SCHED_PROFILE_MOD_LOAD.SCHED_PROFILE_NAME.eq(name))
            .fetch();
    for (SchedProfileModLoadRecord item : retList)
    {
      String modLoadCmd = item.getValue(SCHED_PROFILE_MOD_LOAD.MODULE_LOAD_COMMAND);
      var modulesToLoad = item.getValue(SCHED_PROFILE_MOD_LOAD.MODULES_TO_LOAD);
      var modLoadSpec = new ModuleLoadSpec(modLoadCmd, modulesToLoad);
      moduleLoads.add(modLoadSpec);
    }
    return moduleLoads;
  }

  /**
   * Get all system sequence IDs for specified tenant
   * @param db - DB connection
   * @param tenantId - tenant name
   * @return list of sequence IDs
   */
  private static List<Integer> getAllSystemSeqIdsInTenant(DSLContext db, String tenantId)
  {
    List<Integer> retList = new ArrayList<>();
    if (db == null || StringUtils.isBlank(tenantId)) return retList;
    retList = db.select(SYSTEMS.SEQ_ID).from(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId)).fetchInto(Integer.class);
    return retList;
  }

  /**
   * Add searchList to where condition. All conditions are joined using AND
   * Validate column name, search comparison operator
   *   and compatibility of column type + search operator + column value
   * @param whereCondition base where condition
   * @param searchList List of conditions to add to the base condition
   * @return resulting where condition
   * @throws TapisException on error
   */
  private static Condition addSearchListToWhere(Condition whereCondition, List<String> searchList)
          throws TapisException
  {
    if (searchList == null || searchList.isEmpty()) return whereCondition;
    // Parse searchList and add conditions to the WHERE clause
    for (String condStr : searchList)
    {
      whereCondition = addSearchCondStrToWhere(whereCondition, condStr, "AND");
    }
    return whereCondition;
  }

  /**
   * Create a condition for abstract syntax tree nodes by recursively walking the tree
   * @param astNode Abstract syntax tree node to add to the base condition
   * @return resulting condition
   * @throws TapisException on error
   */
  private static Condition createConditionFromAst(ASTNode astNode) throws TapisException
  {
    if (astNode == null || astNode instanceof ASTLeaf)
    {
      // A leaf node is a column name or value. Nothing to process since we only process a complete condition
      //   having the form column_name.op.value. We should never make it to here
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST1", (astNode == null ? "null" : astNode.toString()));
      throw new TapisException(msg);
    }
    else if (astNode instanceof ASTUnaryExpression)
    {
      // A unary node should have no operator and contain a binary node with two leaf nodes.
      // NOTE: Currently unary operators not supported. If support is provided for unary operators (such as NOT) then
      //   changes will be needed here.
      ASTUnaryExpression unaryNode = (ASTUnaryExpression) astNode;
      if (!StringUtils.isBlank(unaryNode.getOp()))
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_UNARY_OP", unaryNode.getOp(), unaryNode.toString());
        throw new TapisException(msg);
      }
      // Recursive call
      return createConditionFromAst(unaryNode.getNode());
    }
    else if (astNode instanceof ASTBinaryExpression)
    {
      // It is a binary node
      ASTBinaryExpression binaryNode = (ASTBinaryExpression) astNode;
      // Recursive call
      return createConditionFromBinaryExpression(binaryNode);
    }
    return null;
  }

  /**
   * Create a condition from an abstract syntax tree binary node
   * @param binaryNode Abstract syntax tree binary node to add to the base condition
   * @return resulting condition
   * @throws TapisException on error
   */
  private static Condition createConditionFromBinaryExpression(ASTBinaryExpression binaryNode) throws TapisException
  {
    // If we are given a null then something went very wrong.
    if (binaryNode == null)
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST2"));
    }
    // If operator is AND or OR then make recursive call for each side and join together
    // For other operators build the condition left.op.right and add it
    String op = binaryNode.getOp();
    ASTNode leftNode = binaryNode.getLeft();
    ASTNode rightNode = binaryNode.getRight();
    if (StringUtils.isBlank(op))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST3", binaryNode.toString()));
    }
    else if (op.equalsIgnoreCase("AND"))
    {
      // Recursive calls
      Condition cond1 = createConditionFromAst(leftNode);
      Condition cond2 = createConditionFromAst(rightNode);
      if (cond1 == null || cond2 == null)
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString()));
      }
      return cond1.and(cond2);

    }
    else if (op.equalsIgnoreCase("OR"))
    {
      // Recursive calls
      Condition cond1 = createConditionFromAst(leftNode);
      Condition cond2 = createConditionFromAst(rightNode);
      if (cond1 == null || cond2 == null)
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST4", binaryNode.toString()));
      }
      return cond1.or(cond2);

    }
    else
    {
      // End of recursion. Create a single condition.
      // Since operator is not an AND or an OR we should have 2 unary nodes or a unary and leaf node
      String lValue;
      String rValue;
      if (leftNode instanceof ASTLeaf) lValue = ((ASTLeaf) leftNode).getValue();
      else if (leftNode instanceof ASTUnaryExpression) lValue =  ((ASTLeaf) ((ASTUnaryExpression) leftNode).getNode()).getValue();
      else
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST5", binaryNode.toString()));
      }
      if (rightNode instanceof ASTLeaf) rValue = ((ASTLeaf) rightNode).getValue();
      else if (rightNode instanceof ASTUnaryExpression) rValue =  ((ASTLeaf) ((ASTUnaryExpression) rightNode).getNode()).getValue();
      else
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_AST6", binaryNode.toString()));
      }
      // Build the string for the search condition, left.op.right
      String condStr = String.format("%s.%s.%s", lValue, binaryNode.getOp(), rValue);
      // Validate and create a condition from the string
      return addSearchCondStrToWhere(null, condStr, null);
    }
  }

  /**
   * Take a string containing a single condition and create a new condition or join it to an existing condition.
   * Validate column name, search comparison operator and compatibility of column type + search operator + column value
   * @param whereCondition existing condition. If null a new condition is returned.
   * @param searchStr Single search condition in the form column_name.op.value
   * @param joinOp If whereCondition is not null use AND or OR to join the condition with the whereCondition
   * @return resulting where condition
   * @throws TapisException on error
   */
  private static Condition addSearchCondStrToWhere(Condition whereCondition, String searchStr, String joinOp)
          throws TapisException
  {
    // If we have no search string then return what we were given
    if (StringUtils.isBlank(searchStr)) return whereCondition;
    // If we are given a condition but no indication of how to join new condition to it then return what we were given
    if (whereCondition != null && StringUtils.isBlank(joinOp)) return whereCondition;
    if (whereCondition != null && joinOp != null && !joinOp.equalsIgnoreCase("AND") && !joinOp.equalsIgnoreCase("OR"))
    {
      return whereCondition;
    }

    // Parse search value into column name, operator and value
    // Format must be column_name.op.value
    String[] parsedStrArray = DOT_SPLIT.split(searchStr, 3);
    // Validate column name
    String column = parsedStrArray[0];
    // First, check to see if column is on list of unsupported attributes.
    if (TSystem.SEARCH_ATTRS_UNSUPPORTED.contains(DSL.name(column).toString()))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_SRCH_ATTR_UNSUPPORTED", SYSTEMS.getName(), DSL.name(column)));
    }

    Field<?> col = SYSTEMS.field(DSL.name(column));
    // Check for column name passed in as camelcase
    if (col == null)
    {
      col = SYSTEMS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(column)));
    }
    // If column not found then it is an error
    if (col == null)
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_NO_COLUMN", SYSTEMS.getName(), DSL.name(column)));
    }
    // Validate and convert operator string
    String opStr = parsedStrArray[1].toUpperCase();
    SearchOperator op = SearchUtils.getSearchOperator(opStr);
    if (op == null)
    {
      String msg = MsgUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_OP", opStr, SYSTEMS.getName(), DSL.name(column));
      throw new TapisException(msg);
    }

    // Check that column value is compatible for column type and search operator
    String val = parsedStrArray[2];
    checkConditionValidity(col, op, val);

     // If val is a timestamp then convert the string(s) to a form suitable for SQL
    // Use a utility method since val may be a single item or a list of items, e.g. for the BETWEEN operator
    if (col.getDataType().getSQLType() == Types.TIMESTAMP)
    {
      val = SearchUtils.convertValuesToTimestamps(op, val);
    }

    // Create the condition
    Condition newCondition = createCondition(col, op, val);
    // If specified add the condition to the WHERE clause
    if (StringUtils.isBlank(joinOp) || whereCondition == null) return newCondition;
    else if (joinOp.equalsIgnoreCase("AND")) return whereCondition.and(newCondition);
    else if (joinOp.equalsIgnoreCase("OR")) return whereCondition.or(newCondition);
    return newCondition;
  }

  /**
   * Validate condition expression based on column type, search operator and column string value.
   * Use java.sql.Types for validation.
   * @param col jOOQ column
   * @param op Operator
   * @param valStr Column value as string
   * @throws TapisException on error
   */
  private static void checkConditionValidity(Field<?> col, SearchOperator op, String valStr) throws TapisException
  {
    var dataType = col.getDataType();
    int sqlType = dataType.getSQLType();
    String sqlTypeName = dataType.getTypeName();
//    var t2 = dataType.getSQLDataType();
//    var t3 = dataType.getCastTypeName();
//    var t4 = dataType.getSQLType();
//    var t5 = dataType.getType();

    // Make sure we support the sqlType
    if (SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType) == null)
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_UNSUPPORTED_SQLTYPE", SYSTEMS.getName(), col.getName(), op.name(), sqlTypeName);
      throw new TapisException(msg);
    }
    // Check that operation is allowed for column data type
    if (!SearchUtils.ALLOWED_OPS_BY_TYPE.get(sqlType).contains(op))
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_TYPE", SYSTEMS.getName(), col.getName(), op.name(), sqlTypeName);
      throw new TapisException(msg);
    }

    // Check that value (or values for op that takes a list) are compatible with sqlType
    if (!SearchUtils.validateTypeAndValueList(sqlType, op, valStr, sqlTypeName, SYSTEMS.getName(), col.getName()))
    {
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_VALUE", op.name(), sqlTypeName, valStr, SYSTEMS.getName(), col.getName());
      throw new TapisException(msg);
    }
  }

  /**
   * Add condition to SQL where clause given column, operator, value info
   * @param col jOOQ column
   * @param op Operator
   * @param val Column value
   * @return Resulting where clause
   */
  private static Condition createCondition(Field col, SearchOperator op, String val)
  {
    SearchOperator op1 = op;
    List<String> valList = Collections.emptyList();
    if (SearchUtils.listOpSet.contains(op)) valList = SearchUtils.getValueList(val);
    // If operator is IN or NIN and column type is array then handle it as CONTAINS or NCONTAINS
    if ((col.getDataType().getSQLType() == Types.ARRAY) && SearchOperator.IN.equals(op)) op1 = CONTAINS;
    if ((col.getDataType().getSQLType() == Types.ARRAY) && SearchOperator.NIN.equals(op)) op1 = NCONTAINS;
    Condition c = null;
    switch (op1) {
      case EQ -> c = col.eq(val);
      case NEQ -> c = col.ne(val);
      case LT -> c =  col.lt(val);
      case LTE -> c = col.le(val);
      case GT -> c =  col.gt(val);
      case GTE -> c = col.ge(val);
      case LIKE -> c = col.like(val);
      case NLIKE -> c = col.notLike(val);
      case IN -> c = col.in(valList);
      case NIN -> c = col.notIn(valList);
      case CONTAINS -> c = textArrayOverlaps(col, valList.toArray(), false);
      case NCONTAINS -> c = textArrayOverlaps(col, valList.toArray(), true);
      case BETWEEN -> c = col.between(valList.get(0), valList.get(1));
      case NBETWEEN -> c = col.notBetween(valList.get(0), valList.get(1));
    }
    return c;
  }

  /**
   * Get all capabilities contained in an abstract syntax tree by recursively walking the tree
   * @param astNode Abstract syntax tree node containing constraint matching conditions
   * @throws TapisException on error
   */
  private static void getCapabilitiesFromAST(ASTNode astNode, List<Capability> capList) throws TapisException
  {
    if (astNode == null || astNode instanceof ASTLeaf)
    {
      // A leaf node is "category$name" or value. Nothing to process since we only process a complete condition
      //   having the form category$name op value. We should never make it to here
      String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST1", (astNode == null ? "null" : astNode.toString()));
      throw new TapisException(msg);
    }
    else if (astNode instanceof ASTUnaryExpression)
    {
      // A unary node should have no operator and contain a binary node with two leaf nodes.
      // NOTE: Currently unary operators not supported. If support is provided for unary operators (such as NOT) then
      //   changes will be needed here.
      ASTUnaryExpression unaryNode = (ASTUnaryExpression) astNode;
      if (!StringUtils.isBlank(unaryNode.getOp()))
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_INVALID_SEARCH_UNARY_OP", unaryNode.getOp(), unaryNode.toString());
        throw new TapisException(msg);
      }
      // Recursive call
      getCapabilitiesFromAST(unaryNode.getNode(), capList);
    }
    else if (astNode instanceof ASTBinaryExpression)
    {
      // It is a binary node
      ASTBinaryExpression binaryNode = (ASTBinaryExpression) astNode;
      // Recursive call
      getCapabilitiesFromBinaryExpression(binaryNode, capList);
    }
  }

  /**
   * Add capabilities from an abstract syntax tree binary node
   * @param binaryNode Abstract syntax tree binary node to add
   * @throws TapisException on error
   */
  private static void getCapabilitiesFromBinaryExpression(ASTBinaryExpression binaryNode, List<Capability> capList)
          throws TapisException
  {
    // If we are given a null then something went very wrong.
    if (binaryNode == null)
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST2"));
    }
    // If operator is AND or OR then make recursive call for each side
    // Since we are just collecting capabilities we do not distinguish between AND, OR
    // For other operators extract the capability and return
    String op = binaryNode.getOp();
    ASTNode leftNode = binaryNode.getLeft();
    ASTNode rightNode = binaryNode.getRight();
    if (StringUtils.isBlank(op))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST3", binaryNode.toString()));
    }
    else if (op.equalsIgnoreCase("AND") || op.equalsIgnoreCase("OR"))
    {
      // Recursive calls
      getCapabilitiesFromAST(leftNode, capList);
      getCapabilitiesFromAST(rightNode, capList);
    }
    else
    {
      // End of recursion. Extract the capability and return
      // Since operator is not an AND or an OR we should have 2 unary nodes or a unary and leaf node
      // lValue should be in the form category-name or category$name
      // rValue should be the Capability value.
      String lValue;
      String rValue;
      if (leftNode instanceof ASTLeaf) lValue = ((ASTLeaf) leftNode).getValue();
      else if (leftNode instanceof ASTUnaryExpression) lValue =  ((ASTLeaf) ((ASTUnaryExpression) leftNode).getNode()).getValue();
      else
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST5", binaryNode.toString()));
      }
      if (rightNode instanceof ASTLeaf) rValue = ((ASTLeaf) rightNode).getValue();
      else if (rightNode instanceof ASTUnaryExpression) rValue =  ((ASTLeaf) ((ASTUnaryExpression) rightNode).getNode()).getValue();
      else
      {
        throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST6", binaryNode.toString()));
      }
      // Validate and create a capability using lValue, rValue from node
      Capability cap = getCapabilityFromNode(lValue, rValue, binaryNode);
      capList.add(cap);
    }
  }

  /**
   * Construct a Capability based on lValue, rValue from a binary ASTNode containing a constraint matching condition
   * Validate and extract capability attributes: category, name and value.
   *   lValue must be in the form category$name or category$name
   * @param lValue - left string value from the condition in the form category-name or category-name
   * @param rValue - right string value from the condition
   * @return - capability
   * @throws TapisException on error
   */
  private static Capability getCapabilityFromNode(String lValue, String rValue, ASTBinaryExpression binaryNode)
          throws TapisException
  {
    // If lValue is empty it is an error
    if (StringUtils.isBlank(lValue))
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST7", binaryNode));
    }
    // Validate and extract components from lValue
    // Parse lValue into category, and name
    // Format must be column_name.op.value
    String[] parsedStrArray = DOLLAR_SPLIT.split(lValue, 2);
    // Must have at least two items
    if (parsedStrArray.length < 2)
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST7", binaryNode));
    }
    String categoryStr = parsedStrArray[0];
    Capability.Category category = null;
    try { category = Capability.Category.valueOf(categoryStr.toUpperCase()); }
    catch (IllegalArgumentException e)
    {
      throw new TapisException(LibUtils.getMsg("SYSLIB_DB_INVALID_MATCH_AST7", binaryNode));
    }
    String name = parsedStrArray[1];
    Capability.Datatype datatype = null;
    int precedence = -1;
    Capability cap = new Capability(category, name, datatype, precedence, rValue);
    return cap;
  }

  /**
   * Given an sql connection retrieve the system uuid.
   * @param db - jooq context
   * @param tenantId - name of tenant
   * @param id - Id of system
   * @return - uuid
   */
  private static UUID getUUIDUsingDb(DSLContext db, String tenantId, String id)
  {
    return db.selectFrom(SYSTEMS).where(SYSTEMS.TENANT.eq(tenantId),SYSTEMS.ID.eq(id)).fetchOne(SYSTEMS.UUID);
  }


  /**
   * Given an sql connection, a tenant, a list of Category names and a list of system IDs to consider,
   *   fetch all systems that have a Capability matching a category, name.
   * @param db - jooq context
   * @param tenantId - name of tenant
   * @param capabilityList - list of Capabilities from AST (category, name)
   * @param allowedIDs - list of system IDs to consider.
   * @return - true if system exists, else false
   */
  private static List<TSystem> getSystemsHavingCapabilities(DSLContext db, String tenantId, List<Capability> capabilityList,
                                                            Set<String> allowedIDs)
  {
    List<TSystem> retList = new ArrayList<>();
    if (allowedIDs == null || allowedIDs.isEmpty()) return retList;

// TBD    // Begin where condition for the query
//    Condition whereCondition = (SYSTEMS.TENANT.eq(tenantId)).and(SYSTEMS.DELETED.eq(false));
//
//    Field catCol = CAPABILITIES.CATEGORY;
//    Field nameCol = CAPABILITIES.NAME;
//
//    // For each capability add a condition joined by OR
//    Condition newCondition1 = null;
//    for (Capability cap : capabilityList)
//    {
//      Condition newCondition2 = catCol.eq(cap.getCategory().name());
//      newCondition2 = newCondition2.and(nameCol.eq(cap.getName()));
//      if (newCondition1 == null) newCondition1 = newCondition2;
//      else newCondition1 = newCondition1.or(newCondition2);
//    }
//    whereCondition = whereCondition.and(newCondition1);
//
//    // TBD: Work out raw SQL, copy it here and translate it into jOOQ.
//    /*
//     * --  select S.id,S.name as s_name, C.id as c_id, C.category,C.name,C.value from systems as S
//     * select S.* from systems as S
//     *   join capabilities as C on (S.id = C.system_id)
//     *   where c.category = 'SCHEDULER' and c.name = 'Type'
//     *   and S.id in (222, 230, 245);
//     *
//     * select S.* from systems as S
//     *   inner join capabilities as C on (S.id = C.system_id)
//     *   where (c.category = 'SCHEDULER' and c.name = 'Type') OR
//     *   (c.category = 'SCHEDULER' and c.name = 'Type')
//     *   AND S.id in (222, 230, 245);
//     */
//
//    // Add IN condition for list of IDs
//    whereCondition = whereCondition.and(SYSTEMS.ID.in(allowedIDs));
//
//    // Inner join on capabilities table
//    // Execute the select
//
//    Result<SystemsRecord> results = db.selectFrom(SYSTEMS.join(CAPABILITIES).on(SYSTEMS.SEQ_ID.eq(CAPABILITIES.SYSTEM_SEQ_ID)))
//                                      .where(whereCondition).fetchInto(SYSTEMS);
////    Result<SystemsRecord> results = db.select(SYSTEMS.fields()).from(SYSTEMS)
////            .innerJoin(CAPABILITIES).on(SYSTEMS.SEQ_ID.eq(CAPABILITIES.SYSTEM_ID))
////            .where(whereCondition).fetchInto(SYSTEMS);
//
//    if (results == null || results.isEmpty()) return retList;
//
//    // Fill in batch logical queues and job capabilities list from aux tables
//    // NOTE might be able to use fetchGroups to populate these.
//    for (SystemsRecord r : results)
//    {
//      TSystem s = r.into(TSystem.class);
//      s.setJobRuntimes(retrieveJobRuntimes(db, s.getSeqId()));
//      s.setBatchLogicalQueues(retrieveLogicalQueues(db, s.getSeqId()));
//      s.setJobCapabilities(retrieveJobCaps(db, s.getSeqId()));
//      retList.add(s);
//    }
    return retList;
  }

  /**
   * Check items in select list against DB field names
   * @param selectList - list of items to check
   */
  private static void checkSelectListAgainstColumnNames(List<String> selectList) throws TapisException
  {
    for (String selectItem : selectList)
    {
      Field<?> colSelectItem = SYSTEMS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(selectItem)));
      if (!StringUtils.isBlank(selectItem) && colSelectItem == null)
      {
        String msg = LibUtils.getMsg("SYSLIB_DB_NO_COLUMN_SELECT", SYSTEMS.getName(), DSL.name(selectItem));
        throw new TapisException(msg);
      }
    }
  }

  /*
   * Given a record from a select, create a TSystem object
   */
  private static TSystem getSystemFromRecord(SystemsRecord r)
  {
    TSystem system;
    int sysSeqId = r.get(SYSTEMS.SEQ_ID);

    // Convert LocalDateTime to Instant. Note that although "Local" is in the type, timestamps from the DB are in UTC.
    Instant created = r.getCreated().toInstant(ZoneOffset.UTC);
    Instant updated = r.getUpdated().toInstant(ZoneOffset.UTC);

    // Convert JSONB columns to native types
    JsonElement jobRuntimesJson = r.getJobRuntimes();
    List<JobRuntime> jobRuntimes = null;
    if (jobRuntimesJson != null && !jobRuntimesJson.isJsonNull())
    {
      jobRuntimes = Arrays.asList(TapisGsonUtils.getGson().fromJson(jobRuntimesJson, JobRuntime[].class));
    }
    JsonElement jobEnvVariablesJson = r.getJobEnvVariables();
    List<KeyValuePair> jobEnvVariables = Arrays.asList(TapisGsonUtils.getGson().fromJson(jobEnvVariablesJson, KeyValuePair[].class));
    JsonElement logicalQueuesJson = r.getBatchLogicalQueues();
    List<LogicalQueue> logicalQueues = Arrays.asList(TapisGsonUtils.getGson().fromJson(logicalQueuesJson, LogicalQueue[].class));
    JsonElement capabilitiesJson = r.getJobCapabilities();
    List<Capability> capabilities = Arrays.asList(TapisGsonUtils.getGson().fromJson(capabilitiesJson, Capability[].class));

    system = new TSystem(sysSeqId, r.getTenant(), r.getId(), r.getDescription(),
            r.getSystemType(), r.getOwner(), r.getHost(), r.getEnabled(),
            r.getEffectiveUserId(), r.getDefaultAuthnMethod(), r.getBucketName(),
            r.getRootDir(),
            r.getPort(), r.getUseProxy(), r.getProxyHost(), r.getProxyPort(),
            r.getDtnSystemId(), r.getDtnMountPoint(), r.getDtnMountSourcePath(),
            r.getIsDtn(), r.getCanExec(), jobRuntimes, r.getJobWorkingDir(),
            jobEnvVariables, r.getJobMaxJobs(), r.getJobMaxJobsPerUser(),
            r.getCanRunBatch(), r.getEnableCmdPrefix(), r.getMpiCmd(),
            r.getBatchScheduler(), logicalQueues, r.getBatchDefaultLogicalQueue(),
            r.getBatchSchedulerProfile(), capabilities, r.getTags(), r.getNotes(),
            r.getImportRefId(), r.getUuid(), r.getDeleted(), r.getAllowChildren(),
            r.getParentId(), created, updated);
    return system;
  }

  /*
   * Given a record from a select, create a SystemHistoryItem object
   */
  private SystemHistoryItem getSystemHistoryFromRecord(Record r)
  {
	return new SystemHistoryItem(r.get(SYSTEM_UPDATES.JWT_TENANT), r.get(SYSTEM_UPDATES.JWT_USER),
                                 r.get(SYSTEM_UPDATES.OBO_TENANT), r.get(SYSTEM_UPDATES.OBO_USER), r.get(SYSTEM_UPDATES.OPERATION),
	                             r.get(SYSTEM_UPDATES.DESCRIPTION), r.get(SYSTEM_UPDATES.CREATED).toInstant(ZoneOffset.UTC));
  }

  private void updateChildSystemsFromParent(DSLContext db, String tenant, String parentId) {
    SystemsRecord systemRecord = (SystemsRecord) db.selectFrom(SYSTEMS)
            .where(SYSTEMS.TENANT.eq(tenant), SYSTEMS.ID.eq(parentId))
            .fetchOne();

    updateChildSystemsFromParent(db, getSystemFromRecord(systemRecord));
  }

  private void updateChildSystemsFromParent(DSLContext db, TSystem parentSystem) {
    // Make sure owner, effectiveUserId, etc are set
    JsonElement jobEnvVariablesJson = TapisGsonUtils.getGson().toJsonTree(parentSystem.getJobEnvVariables());
    JsonElement jobRuntimesJson = TapisGsonUtils.getGson().toJsonTree(parentSystem.getJobRuntimes());
    JsonElement batchLogicalQueuesJson = TapisGsonUtils.getGson().toJsonTree(parentSystem.getBatchLogicalQueues());
    JsonElement jobCapabilitiesJson = TapisGsonUtils.getGson().toJsonTree(parentSystem.getJobCapabilities());
    String[] tagsStrArray = parentSystem.getTags();
    JsonObject notesObj = (JsonObject) parentSystem.getNotes();

    int rowsUpdated = db.update(SYSTEMS)
            .set(SYSTEMS.TENANT, parentSystem.getTenant())
            .set(SYSTEMS.DESCRIPTION, parentSystem.getDescription())
            .set(SYSTEMS.SYSTEM_TYPE, parentSystem.getSystemType())
            .set(SYSTEMS.HOST, parentSystem.getHost())
            .set(SYSTEMS.DEFAULT_AUTHN_METHOD, parentSystem.getDefaultAuthnMethod())
            .set(SYSTEMS.BUCKET_NAME, parentSystem.getBucketName())
            .set(SYSTEMS.PORT, parentSystem.getPort())
            .set(SYSTEMS.USE_PROXY, parentSystem.isUseProxy())
            .set(SYSTEMS.PROXY_HOST, parentSystem.getProxyHost())
            .set(SYSTEMS.PROXY_PORT, parentSystem.getProxyPort())
            .set(SYSTEMS.DTN_SYSTEM_ID, parentSystem.getDtnSystemId())
            .set(SYSTEMS.DTN_MOUNT_SOURCE_PATH, parentSystem.getDtnMountSourcePath())
            .set(SYSTEMS.DTN_MOUNT_POINT, parentSystem.getDtnMountPoint())
            .set(SYSTEMS.IS_DTN, parentSystem.isDtn())
            .set(SYSTEMS.CAN_EXEC, parentSystem.getCanExec())
            .set(SYSTEMS.CAN_RUN_BATCH, parentSystem.getCanRunBatch())
            .set(SYSTEMS.ENABLE_CMD_PREFIX, parentSystem.isEnableCmdPrefix())
            .set(SYSTEMS.MPI_CMD, parentSystem.getMpiCmd())
            .set(SYSTEMS.JOB_RUNTIMES, jobRuntimesJson)
            .set(SYSTEMS.JOB_WORKING_DIR, parentSystem.getJobWorkingDir())
            .set(SYSTEMS.JOB_ENV_VARIABLES, jobEnvVariablesJson)
            .set(SYSTEMS.JOB_MAX_JOBS, parentSystem.getJobMaxJobs())
            .set(SYSTEMS.JOB_MAX_JOBS_PER_USER, parentSystem.getJobMaxJobsPerUser())
            .set(SYSTEMS.BATCH_SCHEDULER, parentSystem.getBatchScheduler())
            .set(SYSTEMS.BATCH_LOGICAL_QUEUES, batchLogicalQueuesJson)
            .set(SYSTEMS.BATCH_DEFAULT_LOGICAL_QUEUE, parentSystem.getBatchDefaultLogicalQueue())
            .set(SYSTEMS.BATCH_SCHEDULER_PROFILE, parentSystem.getBatchSchedulerProfile())
            .set(SYSTEMS.JOB_CAPABILITIES, jobCapabilitiesJson)
            .set(SYSTEMS.TAGS, tagsStrArray)
            .set(SYSTEMS.NOTES, notesObj)
            .set(SYSTEMS.IMPORT_REF_ID, parentSystem.getImportRefId())
            .set(SYSTEMS.UUID, parentSystem.getUuid())
            .set(SYSTEMS.ALLOW_CHILDREN, parentSystem.isAllowChildren())
            .where(SYSTEMS.TENANT.eq(parentSystem.getTenant()), SYSTEMS.PARENT_ID.eq(parentSystem.getId()), SYSTEMS.DELETED.isFalse())
            .execute();
    log.info("Child Systems Updated:" + rowsUpdated);
  }

  /*
   * Implement the array overlap construct in jooq.
   * Given a column as a Field<T[]> and a java array create a jooq condition that
   * returns true if column contains any of the values in the array.
   */
  private static <T> Condition textArrayOverlaps(Field<T[]> col, T[] array, boolean negate)
  {
    Condition cond = DSL.condition("{0} && {1}::text[]", col, DSL.array(array));
    if (negate) return cond.not();
    else return cond;
  }

}
