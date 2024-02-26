package edu.utexas.tacc.tapis.systems.api.responses.results;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import static edu.utexas.tacc.tapis.systems.api.resources.SystemResource.SUMMARY_ATTRS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;

/*
    Class representing a TSystem result to be returned
    Provides for selecting only certain attributes to be included.
 */
public final class TapisSystemDTO
{
  private static final Gson gson = TapisGsonUtils.getGson();

  public boolean isPublic;
  public boolean isDynamicEffectiveUser;
  public Set<String> sharedWithUsers;

  public String tenant;
  public String id;
  public String description;
  public TSystem.SystemType systemType;
  public String owner;
  public String host;
  public boolean enabled;
  public String effectiveUserId;
  public TSystem.AuthnMethod defaultAuthnMethod;
  public Credential authnCredential;
  public String bucketName;
  public String rootDir;
  public int port;
  public boolean useProxy;
  public String proxyHost;
  public int proxyPort;
  public String dtnSystemId;
  public boolean canExec;
  public boolean canRunBatch;
  public boolean enableCmdPrefix;
  public String mpiCmd;
  public List<JobRuntime> jobRuntimes;
  public String jobWorkingDir;
  public List<KeyValuePair> jobEnvVariables;
  public int jobMaxJobs;
  public int jobMaxJobsPerUser;
  public TSystem.SchedulerType batchScheduler;
  public List<LogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public String batchSchedulerProfile;
  public List<Capability> jobCapabilities;
  public String[] tags;
  public Object notes;
  public String importRefId;
  public UUID uuid;
  public boolean allowChildren;
  public String parentId;
  public boolean deleted;
  public Instant created;
  public Instant updated;

  public TapisSystemDTO(TSystem s)
  {
    // All other attributes come directly from TSystem
    tenant = s.getTenant();
    id = s.getId();
    description = s.getDescription();
    systemType = s.getSystemType();
    owner = s.getOwner();
    host = s.getHost();
    enabled = s.isEnabled();
    effectiveUserId = s.getEffectiveUserId();
    defaultAuthnMethod = s.getDefaultAuthnMethod();
    authnCredential = s.getAuthnCredential();
    bucketName = s.getBucketName();
    rootDir = s.getRootDir();
    port = s.getPort();
    useProxy = s.isUseProxy();
    proxyHost = s.getProxyHost();
    proxyPort = s.getProxyPort();
    dtnSystemId = s.getDtnSystemId();
    canExec = s.getCanExec();
    canRunBatch = s.getCanRunBatch();
    enableCmdPrefix = s.isEnableCmdPrefix();
    mpiCmd = s.getMpiCmd();
    jobRuntimes = s.getJobRuntimes();
    jobEnvVariables = s.getJobEnvVariables();
    jobWorkingDir = s.getJobWorkingDir();
    jobMaxJobs = s.getJobMaxJobs();
    jobMaxJobsPerUser = s.getJobMaxJobsPerUser();
    batchScheduler = s.getBatchScheduler();
    batchLogicalQueues = s.getBatchLogicalQueues();
    batchDefaultLogicalQueue = s.getBatchDefaultLogicalQueue();
    batchSchedulerProfile = s.getBatchSchedulerProfile();
    jobCapabilities = s.getJobCapabilities();
    tags = s.getTags();
    notes = s.getNotes();
    importRefId = s.getImportRefId();
    uuid = s.getUuid();
    deleted = s.isDeleted();
    created = s.getCreated();
    updated = s.getUpdated();
    // Check for -1 in max values and return Integer.MAX_VALUE instead.
    //   As requested by Jobs service.
    if (jobMaxJobs < 0) jobMaxJobs = Integer.MAX_VALUE;
    if (jobMaxJobsPerUser < 0) jobMaxJobsPerUser = Integer.MAX_VALUE;
    isPublic = s.isPublic();
    isDynamicEffectiveUser = s.isDynamicEffectiveUser();
    sharedWithUsers = s.getSharedWithUsers();
    allowChildren = s.isAllowChildren();
    parentId = s.getParentId();
  }

  /**
   * Create a JsonObject containing the id attribute and any attribute in the selectSet that matches the name
   * of a public field in this class
   * If selectSet is null or empty then all attributes are included.
   * else if selectSet contains "allAttributes" then all attributes are included regardless of other items in set
   * else if selectSet contains "summaryAttributes" then summary attributes are included regardless of other items in set
   * @return JsonObject containing attributes in the select list.
   */
  public JsonObject getDisplayObject(List<String> selectList)
  {
    // Check for special case of returning all attributes
    if (selectList == null || selectList.isEmpty() || selectList.contains(SEL_ALL_ATTRS))
    {
      return allAttrs();
    }

    var retObj = new JsonObject();

    // If summaryAttrs included then add them
    if (selectList.contains(SEL_SUMMARY_ATTRS)) addSummaryAttrs(retObj);

    // Include specified list of attributes
    // If ID not in list we add it anyway.
    if (!selectList.contains(ID_FIELD)) addDisplayField(retObj, ID_FIELD);
    for (String attrName : selectList)
    {
      addDisplayField(retObj, attrName);
    }
    return retObj;
  }

  // Build a JsonObject with all displayable attributes
  private JsonObject allAttrs()
  {
    String jsonStr = gson.toJson(this);
    return gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
  }

  // Add summary attributes to a json object
  private void addSummaryAttrs(JsonObject jsonObject)
  {
    for (String attrName: SUMMARY_ATTRS)
    {
      addDisplayField(jsonObject, attrName);
    }
  }

  /**
   * Add specified attribute name to the JsonObject that is to be returned as the displayable object.
   * If attribute does not exist in this class then it is a no-op.
   *
   * @param jsonObject Base JsonObject that will be returned.
   * @param attrName Attribute name to add to the JsonObject
   */
  private void addDisplayField(JsonObject jsonObject, String attrName)
  {
    String jsonStr;
    switch (attrName) {
      case TENANT_FIELD -> jsonObject.addProperty(TENANT_FIELD, tenant);
      case ID_FIELD -> jsonObject.addProperty(ID_FIELD, id);
      case DESCRIPTION_FIELD ->jsonObject.addProperty(DESCRIPTION_FIELD, description);
      case SYSTEM_TYPE_FIELD -> jsonObject.addProperty(SYSTEM_TYPE_FIELD, systemType.name());
      case OWNER_FIELD -> jsonObject.addProperty(OWNER_FIELD, owner);
      case HOST_FIELD -> jsonObject.addProperty(HOST_FIELD, host);
      case ENABLED_FIELD -> jsonObject.addProperty(ENABLED_FIELD, Boolean.toString(enabled));
      case EFFECTIVE_USER_ID_FIELD -> jsonObject.addProperty(EFFECTIVE_USER_ID_FIELD, effectiveUserId);
      case DEFAULT_AUTHN_METHOD_FIELD -> jsonObject.addProperty(DEFAULT_AUTHN_METHOD_FIELD, defaultAuthnMethod.name());
      case AUTHN_CREDENTIAL_FIELD -> {
        jsonStr = gson.toJson(authnCredential);
        jsonObject.add(AUTHN_CREDENTIAL_FIELD, gson.fromJson(jsonStr, JsonObject.class));
      }
      case BUCKET_NAME_FIELD -> jsonObject.addProperty(BUCKET_NAME_FIELD, bucketName);
      case ROOT_DIR_FIELD -> jsonObject.addProperty(ROOT_DIR_FIELD, rootDir);
      case PORT_FIELD -> jsonObject.addProperty(PORT_FIELD, port);
      case USE_PROXY_FIELD -> jsonObject.addProperty(USE_PROXY_FIELD, Boolean.toString(useProxy));
      case PROXY_HOST_FIELD -> jsonObject.addProperty(PROXY_HOST_FIELD, proxyHost);
      case PROXY_PORT_FIELD -> jsonObject.addProperty(PROXY_PORT_FIELD, proxyPort);
      case DTN_SYSTEM_ID_FIELD -> jsonObject.addProperty(DTN_SYSTEM_ID_FIELD, dtnSystemId);
      case CAN_EXEC_FIELD -> jsonObject.addProperty(CAN_EXEC_FIELD, Boolean.toString(canExec));
      case CAN_RUN_BATCH_FIELD -> jsonObject.addProperty(CAN_RUN_BATCH_FIELD, Boolean.toString(canRunBatch));
      case ENABLE_CMD_PREFIX_FIELD -> jsonObject.addProperty(ENABLE_CMD_PREFIX_FIELD, Boolean.toString(enableCmdPrefix));
      case ALLOW_CHILDREN_FIELD -> jsonObject.addProperty(ALLOW_CHILDREN_FIELD, Boolean.toString(allowChildren));
      case MPI_CMD_FIELD -> jsonObject.addProperty(MPI_CMD_FIELD, mpiCmd);
      case PARENT_ID_FIELD -> jsonObject.addProperty(PARENT_ID_FIELD, parentId);
      case JOB_RUNTIMES_FIELD -> jsonObject.add(JOB_RUNTIMES_FIELD, gson.toJsonTree(jobRuntimes));
      case JOB_WORKING_DIR_FIELD -> jsonObject.addProperty(JOB_WORKING_DIR_FIELD, jobWorkingDir);
      case JOB_ENV_VARIABLES_FIELD -> jsonObject.add(JOB_ENV_VARIABLES_FIELD, gson.toJsonTree(jobEnvVariables));
      case JOB_MAX_JOBS_FIELD -> jsonObject.addProperty(JOB_MAX_JOBS_FIELD, jobMaxJobs);
      case JOB_MAX_JOBS_PER_USER_FIELD -> jsonObject.addProperty(JOB_MAX_JOBS_PER_USER_FIELD, jobMaxJobsPerUser);
      case BATCH_SCHEDULER_FIELD -> jsonObject.addProperty(BATCH_SCHEDULER_FIELD, batchScheduler.name());
      case BATCH_LOGICAL_QUEUES_FIELD -> jsonObject.add(BATCH_LOGICAL_QUEUES_FIELD, gson.toJsonTree(batchLogicalQueues));
      case BATCH_DEFAULT_LOGICAL_QUEUE_FIELD -> jsonObject.addProperty(BATCH_DEFAULT_LOGICAL_QUEUE_FIELD, batchDefaultLogicalQueue);
      case BATCH_SCHEDULER_PROFILE_FIELD -> jsonObject.addProperty(BATCH_SCHEDULER_PROFILE_FIELD, batchSchedulerProfile);
      case JOB_CAPABILITIES_FIELD -> jsonObject.add(JOB_CAPABILITIES_FIELD, gson.toJsonTree(jobCapabilities));
      case TAGS_FIELD -> jsonObject.add(TAGS_FIELD, gson.toJsonTree(tags));
      case NOTES_FIELD -> {
        jsonStr = gson.toJson(notes);
        jsonObject.add(NOTES_FIELD, gson.fromJson(jsonStr, JsonObject.class));
      }
      case IMPORT_REF_ID -> jsonObject.addProperty(IMPORT_REF_ID, importRefId);
      case UUID_FIELD -> jsonObject.addProperty(UUID_FIELD, uuid.toString());
      case DELETED_FIELD -> jsonObject.addProperty(DELETED_FIELD, Boolean.toString(deleted));
      case CREATED_FIELD -> jsonObject.addProperty(CREATED_FIELD, created.toString());
      case UPDATED_FIELD -> jsonObject.addProperty(UPDATED_FIELD, updated.toString());
      case IS_PUBLIC_FIELD -> jsonObject.addProperty(IS_PUBLIC_FIELD, Boolean.toString(isPublic));
      case IS_DYNAMIC_EFFECTIVE_USER -> jsonObject.addProperty(IS_DYNAMIC_EFFECTIVE_USER, Boolean.toString(isDynamicEffectiveUser));
      case SHARED_WITH_USERS_FIELD -> jsonObject.add(SHARED_WITH_USERS_FIELD, gson.toJsonTree(sharedWithUsers));
    }
  }
}
