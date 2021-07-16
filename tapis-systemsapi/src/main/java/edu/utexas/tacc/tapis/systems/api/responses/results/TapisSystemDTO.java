package edu.utexas.tacc.tapis.systems.api.responses.results;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.api.model.ResultJobCapability;
import edu.utexas.tacc.tapis.systems.api.model.ResultJobRuntime;
import edu.utexas.tacc.tapis.systems.api.model.ResultLogicalQueue;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.api.utils.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import static edu.utexas.tacc.tapis.systems.api.resources.SystemResource.SUMMARY_ATTRS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.AUTHN_CREDENTIAL_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_DEFAULT_LOGICAL_QUEUE_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_LOGICAL_QUEUES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_SCHEDULER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BUCKET_NAME_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.CAN_EXEC_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_AUTHN_METHOD_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DELETED_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DESCRIPTION_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_MOUNT_POINT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_MOUNT_SOURCE_PATH_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_SYSTEM_ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EFFECTIVE_USER_ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ENABLED_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.HOST_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.IS_DTN_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_CAPABILITIES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_ENV_VARIABLES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_IS_BATCH_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_MAX_JOBS_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_MAX_JOBS_PER_USER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_RUNTIMES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_WORKING_DIR_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.NOTES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PORT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PROXY_HOST_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PROXY_PORT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ROOT_DIR_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.SYSTEM_TYPE_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TAGS_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TENANT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.USE_PROXY_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.UUID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.CREATED_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.UPDATED_FIELD;

/*
    Class representing a TSystem result to be returned
 */
public final class TapisSystemDTO
{
  private static final Gson gson = TapisGsonUtils.getGson();

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
  public String dtnMountPoint;
  public String dtnMountSourcePath;
  public boolean isDtn;
  public boolean canExec;
  public List<ResultJobRuntime> jobRuntimes;
  public String jobWorkingDir;
  public List<KeyValuePair> jobEnvVariables;
  public int jobMaxJobs;
  public int jobMaxJobsPerUser;
  public boolean jobIsBatch;
  public TSystem.SchedulerType batchScheduler;
  public List<ResultLogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public List<ResultJobCapability> jobCapabilities;
  public String[] tags;
  public Object notes;
  public UUID uuid;
  public boolean deleted;
  public Instant created;
  public Instant updated;

  public TapisSystemDTO(TSystem s)
  {
    // Convert jobEnvVariables from array of "key=value" to list of KeyValuePair
    jobEnvVariables = ApiUtils.getKeyValuesAsList(s.getJobEnvVariables());
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
    dtnMountPoint = s.getDtnMountPoint();
    dtnMountSourcePath = s.getDtnMountSourcePath();
    isDtn = s.isDtn();
    canExec = s.getCanExec();
    jobRuntimes = null;
    if (s.getJobRuntimes() != null && !s.getJobRuntimes().isEmpty())
    {
      jobRuntimes = new ArrayList<>();
      for (JobRuntime rt : s.getJobRuntimes()) { jobRuntimes.add(new ResultJobRuntime(rt)); }
    }
    jobWorkingDir = s.getJobWorkingDir();
    jobMaxJobs = s.getJobMaxJobs();
    jobMaxJobsPerUser = s.getJobMaxJobsPerUser();
    jobIsBatch = s.getJobIsBatch();
    batchScheduler = s.getBatchScheduler();
    batchLogicalQueues = new ArrayList<>();
    if (s.getBatchLogicalQueues() != null)
      for (LogicalQueue q : s.getBatchLogicalQueues())
      {
        batchLogicalQueues.add(new ResultLogicalQueue(q));
      }
    batchDefaultLogicalQueue = s.getBatchDefaultLogicalQueue();
    jobCapabilities = new ArrayList<>();
    if (s.getJobCapabilities() != null)
      for (Capability jc : s.getJobCapabilities())
      {
        jobCapabilities.add(new ResultJobCapability(jc));
      }
    tags = s.getTags();
    notes = s.getNotes();
    uuid = s.getUuid();
    deleted = s.isDeleted();
    created = s.getCreated();
    updated = s.getUpdated();
    // Check for -1 in max values and return Integer.MAX_VALUE instead.
    //   As requested by Jobs service.
    if (jobMaxJobs < 0) jobMaxJobs = Integer.MAX_VALUE;
    if (jobMaxJobsPerUser < 0) jobMaxJobsPerUser = Integer.MAX_VALUE;
  }

  /**
   * Create a JsonObject containing the id attribute and any attribute in the selectSet that matches the name
   * of a public field in this class
   * If selectSet is null or empty then all attributes are included.
   * If selectSet contains "allAttributes" then all attributes are included regardless of other items in set
   * If selectSet contains "summaryAttributes" then summary attributes are included regardless of other items in set
   * @return JsonObject containing attributes in the select list.
   */
  public JsonObject getDisplayObject(List<String> selectList)
  {
    // Check for special case of returning all attributes
    if (selectList == null || selectList.isEmpty() || selectList.contains("allAttributes"))
    {
      return allAttrs();
    }

    var retObj = new JsonObject();

    // If summaryAttrs included then add them
    if (selectList.contains("summaryAttributes")) addSummaryAttrs(retObj);

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
      case DTN_MOUNT_POINT_FIELD -> jsonObject.addProperty(DTN_MOUNT_POINT_FIELD, dtnMountPoint);
      case DTN_MOUNT_SOURCE_PATH_FIELD -> jsonObject.addProperty(DTN_MOUNT_SOURCE_PATH_FIELD, dtnMountSourcePath);
      case DTN_SYSTEM_ID_FIELD -> jsonObject.addProperty(DTN_SYSTEM_ID_FIELD, dtnSystemId);
      case IS_DTN_FIELD -> jsonObject.addProperty(IS_DTN_FIELD, Boolean.toString(isDtn));
      case CAN_EXEC_FIELD -> jsonObject.addProperty(CAN_EXEC_FIELD, Boolean.toString(canExec));
      case JOB_RUNTIMES_FIELD -> jsonObject.add(JOB_RUNTIMES_FIELD, gson.toJsonTree(jobRuntimes));
      case JOB_WORKING_DIR_FIELD -> jsonObject.addProperty(JOB_WORKING_DIR_FIELD, jobWorkingDir);
      case JOB_ENV_VARIABLES_FIELD -> jsonObject.add(JOB_ENV_VARIABLES_FIELD, gson.toJsonTree(jobEnvVariables));
      case JOB_MAX_JOBS_FIELD -> jsonObject.addProperty(JOB_MAX_JOBS_FIELD, jobMaxJobs);
      case JOB_MAX_JOBS_PER_USER_FIELD -> jsonObject.addProperty(JOB_MAX_JOBS_PER_USER_FIELD, jobMaxJobsPerUser);
      case JOB_IS_BATCH_FIELD -> jsonObject.addProperty(JOB_IS_BATCH_FIELD, Boolean.toString(jobIsBatch));
      case BATCH_SCHEDULER_FIELD -> jsonObject.addProperty(BATCH_SCHEDULER_FIELD, batchScheduler.name());
      case BATCH_LOGICAL_QUEUES_FIELD -> jsonObject.add(BATCH_LOGICAL_QUEUES_FIELD, gson.toJsonTree(batchLogicalQueues));
      case BATCH_DEFAULT_LOGICAL_QUEUE_FIELD -> jsonObject.addProperty(BATCH_DEFAULT_LOGICAL_QUEUE_FIELD, batchDefaultLogicalQueue);
      case JOB_CAPABILITIES_FIELD -> jsonObject.add(JOB_CAPABILITIES_FIELD, gson.toJsonTree(jobCapabilities));
      case TAGS_FIELD -> jsonObject.add(TAGS_FIELD, gson.toJsonTree(tags));
      case NOTES_FIELD -> {
        jsonStr = gson.toJson(notes);
        jsonObject.add(NOTES_FIELD, gson.fromJson(jsonStr, JsonObject.class));
      }
      case UUID_FIELD -> jsonObject.addProperty(UUID_FIELD, uuid.toString());
      case DELETED_FIELD -> jsonObject.addProperty(DELETED_FIELD, Boolean.toString(deleted));
      case CREATED_FIELD -> jsonObject.addProperty(CREATED_FIELD, created.toString());
      case UPDATED_FIELD -> jsonObject.addProperty(UPDATED_FIELD, updated.toString());
    }
  }
}
