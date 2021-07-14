package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.gson.JsonObject;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

/*
 * Tapis System representing a server or collection of servers exposed through a
 * single host name or ip address. Each system is associated with a specific tenant.
 * Id of the system must be URI safe, see RFC 3986.
 *   Allowed characters: Alphanumeric  [0-9a-zA-Z] and special characters [-._~].
 * Each system has an owner, effective access user, protocol attributes
 *   and flag indicating if it is currently enabled.
 *
 * Tenant + id must be unique
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 * Note Credential is immutable so no need for copy.
 */
public final class TSystem
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Set of reserved system names
  public static final Set<String> RESERVED_ID_SET = new HashSet<>(Set.of("HEALTHCHECK", "READYCHECK", "SEARCH"));

  public static final String PERMISSION_WILDCARD = "*";
  // Allowed substitution variables
  public static final String APIUSERID_VAR = "${apiUserId}";
  public static final String OWNER_VAR = "${owner}";
  public static final String TENANT_VAR = "${tenant}";
  public static final String EFFUSERID_VAR = "${effectiveUserId}";

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};

  // Attribute names, also used as field names in Json
  public static final String TENANT_FIELD = "tenant";
  public static final String ID_FIELD = "id";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String SYSTEM_TYPE_FIELD = "systemType";
  public static final String OWNER_FIELD = "owner";
  public static final String HOST_FIELD = "host";
  public static final String ENABLED_FIELD = "enabled";
  public static final String EFFECTIVE_USER_ID_FIELD = "effectiveUserId";
  public static final String DEFAULT_AUTHN_METHOD_FIELD = "defaultAuthnMethod";
  public static final String AUTHN_CREDENTIAL_FIELD = "authnCredential";
  public static final String BUCKET_NAME_FIELD = "bucketName";
  public static final String ROOT_DIR_FIELD = "rootDir";
  public static final String PORT_FIELD = "port";
  public static final String USE_PROXY_FIELD = "useProxy";
  public static final String PROXY_HOST_FIELD = "proxyHost";
  public static final String PROXY_PORT_FIELD = "proxyPort";
  public static final String DTN_SYSTEM_ID_FIELD = "dtnSystemId";
  public static final String DTN_MOUNT_POINT_FIELD = "dtnMountPoint";
  public static final String DTN_MOUNT_SOURCE_PATH_FIELD = "dtnMountSourcePath";
  public static final String IS_DTN_FIELD = "isDtn";
  public static final String CAN_EXEC_FIELD = "canExec";
  public static final String JOB_RUNTIMES_FIELD = "jobRuntimes";
  public static final String JOB_WORKING_DIR_FIELD = "jobWorkingDir";
  public static final String JOB_ENV_VARIABLES_FIELD = "jobEnvVariables";
  public static final String JOB_MAX_JOBS_FIELD = "jobMaxJobs";
  public static final String JOB_MAX_JOBS_PER_USER_FIELD = "jobMaxJobsPerUser";
  public static final String JOB_IS_BATCH_FIELD = "jobsIsBatch";
  public static final String BATCH_SCHEDULER_FIELD = "batchScheduler";
  public static final String BATCH_LOGICAL_QUEUES_FIELD = "batchLogicalQueues";
  public static final String BATCH_DEFAULT_LOGICAL_QUEUE_FIELD = "batchDefaultLogicalQueue";
  public static final String JOB_CAPABILITIES_FIELD = "jobCapabilities";
  public static final String TAGS_FIELD = "tags";
  public static final String NOTES_FIELD = "notes";
  public static final String UUID_FIELD = "uuid";
  public static final String CREATED_FIELD = "created";
  public static final String UPDATED_FIELD = "updated";

  // Default values
  public static final String[] EMPTY_STR_ARRAY = new String[0];
  public static final String DEFAULT_OWNER = APIUSERID_VAR;
  public static final boolean DEFAULT_ENABLED = true;
  public static final String DEFAULT_EFFECTIVEUSERID = APIUSERID_VAR;
  public static final String[] DEFAULT_JOBENV_VARIABLES = EMPTY_STR_ARRAY;
  public static final JsonObject DEFAULT_NOTES = TapisGsonUtils.getGson().fromJson("{}", JsonObject.class);
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = null;
  public static final int DEFAULT_PROXYPORT = -1;
  public static final int DEFAULT_JOBMAXJOBS = -1;
  public static final int DEFAULT_JOBMAXJOBSPERUSER = -1;

  // Message keys
  private static final String CREATE_MISSING_ATTR = "SYSLIB_CREATE_MISSING_ATTR";
  private static final String INVALID_STR_ATTR = "SYSLIB_INVALID_STR_ATTR";
  private static final String TOO_LONG_ATTR = "SYSLIB_TOO_LONG_ATTR";

  // Validation patterns
  //ID Must start alphabetic and contain only alphanumeric and 4 special characters: - . _ ~
  private final String PATTERN_VALID_ID = "^[a-zA-Z]([a-zA-Z0-9]|[-\\._~])*";

  // Validation constants
  private final Integer MAX_ID_LEN = 80;
  private final Integer MAX_DESCRIPTION_LEN = 2048;
  private final Integer MAX_PATH_LEN = 4096;
  private final Integer MAX_USERNAME_LEN = 60;
  private final Integer MAX_BUCKETNAME_LEN = 63;
  private final Integer MAX_QUEUENAME_LEN = 128;
  private final Integer MAX_HPCQUEUENAME_LEN = 128;
  private final Integer MAX_RUNTIME_VER_LEN = 128;
  private final Integer MAX_CAPABILITYNAME_LEN = 128;
  private final Integer MAX_TAG_LEN = 128;

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum SystemType {LINUX, S3}
  public enum SystemOperation {create, read, modify, execute, delete, undelete, hardDelete, changeOwner, enable, disable,
                               getPerms, grantPerms, revokePerms, setCred, removeCred, getCred}
  public enum Permission {READ, MODIFY, EXECUTE}
  public enum AuthnMethod {PASSWORD, PKI_KEYS, ACCESS_KEY, CERT}
  public enum SchedulerType {SLURM, CONDOR, PBS, SGE, UGE, TORQUE}

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // NOTE: In order to use jersey's SelectableEntityFilteringFeature fields cannot be final.
  private int seqId;         // Unique database sequence number
  private String tenant;     // Name of the tenant for which the system is defined
  private final String id;       // Name of the system
  private String description; // Full description of the system
  private final SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private String owner;      // User who owns the system and has full privileges
  private String host;       // Host name or IP address
  private boolean enabled; // Indicates if systems is currently enabled
  private String effectiveUserId; // User to use when accessing system, may be static or dynamic
  private AuthnMethod defaultAuthnMethod; // How access authorization is handled by default
  private Credential authnCredential; // Credential to be stored in or retrieved from the Security Kernel
  private String bucketName; // Name of bucket for system of type OBJECT_STORE
  private String rootDir;    // Effective root directory for system of type LINUX, can also be used for system of type OBJECT_STORE
  private int port;          // Port number used to access the system
  private boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private String proxyHost;  // Name or IP address of proxy host
  private int proxyPort;     // Port number for proxy host
  private String dtnSystemId;
  private String dtnMountPoint;
  private String dtnMountSourcePath;
  private boolean isDtn;
  private final boolean canExec; // Indicates if system will be used to execute jobs
  private List<JobRuntime> jobRuntimes;
  private String jobWorkingDir; // Parent directory from which a job is run. Relative to effective root dir.
  private String[] jobEnvVariables;
  private int jobMaxJobs;
  private int jobMaxJobsPerUser;
  private boolean jobIsBatch;
  private SchedulerType batchScheduler;
  private List<LogicalQueue> batchLogicalQueues;
  private String batchDefaultLogicalQueue;
  private List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private String[] tags; // List of arbitrary tags as strings
  private Object notes;   // Simple metadata as json.
  private UUID uuid;
  private boolean deleted;

  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor using only required attributes.
   */
  public TSystem(String id1, SystemType systemType1, String host1, AuthnMethod defaultAuthnMethod1, boolean canExec1)
  {
    id = id1;
    systemType = systemType1;
    host = host1;
    defaultAuthnMethod = defaultAuthnMethod1;
    canExec = canExec1;
  }

  /**
   * Constructor using non-updatable attributes.
   * Rather than exposing otherwise unnecessary setters we use a special constructor.
   */
  public TSystem(TSystem t, String tenant1, String id1, SystemType systemType1, boolean isDtn1, boolean canExec1)
  {
    if (t==null || StringUtils.isBlank(tenant1) || StringUtils.isBlank(id1) || systemType1 == null )
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    tenant = tenant1;
    id = id1;
    systemType = systemType1;
    isDtn = isDtn1;
    canExec = canExec1;

    seqId = t.getSeqId();
    created = t.getCreated();
    updated = t.getUpdated();
    description = t.getDescription();
    owner = t.getOwner();
    host = t.getHost();
    enabled = t.isEnabled();
    effectiveUserId = t.getEffectiveUserId();
    defaultAuthnMethod = t.getDefaultAuthnMethod();
    authnCredential = t.getAuthnCredential();
    bucketName = t.getBucketName();
    rootDir = t.getRootDir();
    port = t.getPort();
    useProxy = t.isUseProxy();
    proxyHost = t.getProxyHost();
    proxyPort = t.getProxyPort();
    dtnSystemId = t.getDtnSystemId();
    dtnMountPoint = t.getDtnMountPoint();
    dtnMountSourcePath = t.dtnMountSourcePath;
    jobRuntimes = t.getJobRuntimes();
    jobWorkingDir = t.getJobWorkingDir();
    jobEnvVariables = t.getJobEnvVariables();
    jobMaxJobs = t.getJobMaxJobs();
    jobMaxJobsPerUser = t.getJobMaxJobsPerUser();
    jobIsBatch = t.getJobIsBatch();
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = t.getBatchDefaultLogicalQueue();
    jobCapabilities = t.getJobCapabilities();
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
    uuid = t.getUuid();
    deleted = t.isDeleted();
  }

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   * Note that jobRuntimes, batchLogicalQueues and jobCapabilities must be set separately.
   */
  public TSystem(int seqId1, String tenant1, String id1, String description1, SystemType systemType1,
                 String owner1, String host1, boolean enabled1, String effectiveUserId1, AuthnMethod defaultAuthnMethod1,
                 String bucketName1, String rootDir1,
                 int port1, boolean useProxy1, String proxyHost1, int proxyPort1,
                 String dtnSystemId1, String dtnMountPoint1, String dtnMountSourcePath1, boolean isDtn1,
                 boolean canExec1, String jobWorkingDir1, String[] jobEnvVariables1, int jobMaxJobs1,
                 int jobMaxJobsPerUser1, boolean jobIsBatch1, SchedulerType batchScheduler1, String batchDefaultLogicalQueue1,
                 String[] tags1, Object notes1, UUID uuid1, boolean deleted1,
                 Instant created1, Instant updated1)
  {
    seqId = seqId1;
    tenant = tenant1;
    id = id1;
    description = description1;
    systemType = systemType1;
    owner = owner1;
    host = host1;
    enabled = enabled1;
    effectiveUserId = effectiveUserId1;
    defaultAuthnMethod = defaultAuthnMethod1;
    bucketName = bucketName1;
    rootDir = rootDir1;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    dtnSystemId = dtnSystemId1;
    dtnMountPoint = dtnMountPoint1;
    dtnMountSourcePath = dtnMountSourcePath1;
    isDtn = isDtn1;
    canExec = canExec1;
    jobWorkingDir = jobWorkingDir1;
    jobEnvVariables = (jobEnvVariables1 == null) ? DEFAULT_JOBENV_VARIABLES : jobEnvVariables1.clone();
    jobMaxJobs = jobMaxJobs1;
    jobMaxJobsPerUser = jobMaxJobsPerUser1;
    jobIsBatch = jobIsBatch1;
    batchScheduler = batchScheduler1;
    batchDefaultLogicalQueue = batchDefaultLogicalQueue1;
    tags = (tags1 == null) ? EMPTY_STR_ARRAY : tags1.clone();
    notes = notes1;
    uuid = uuid1;
    deleted = deleted1;
    created = created1;
    updated = updated1;
  }

  /**
   * Copy constructor. Returns a deep copy of a TSystem object.
   * The getters make defensive copies as needed. Note Credential is immutable so no need for copy.
   */
  public TSystem(TSystem t)
  {
    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    seqId = t.getSeqId();
    created = t.getCreated();
    updated = t.getUpdated();
    uuid = t.getUuid();
    deleted = t.isDeleted();
    tenant = t.getTenant();
    id = t.getId();
    description = t.getDescription();
    systemType = t.getSystemType();
    owner = t.getOwner();
    host = t.getHost();
    enabled = t.isEnabled();
    effectiveUserId = t.getEffectiveUserId();
    defaultAuthnMethod = t.getDefaultAuthnMethod();
    authnCredential = t.getAuthnCredential();
    bucketName = t.getBucketName();
    rootDir = t.getRootDir();
    port = t.getPort();
    useProxy = t.isUseProxy();
    proxyHost = t.getProxyHost();
    proxyPort = t.getProxyPort();
    dtnSystemId = t.getDtnSystemId();
    dtnMountPoint = t.getDtnMountPoint();
    dtnMountSourcePath = t.dtnMountSourcePath;
    isDtn = t.isDtn();
    canExec = t.getCanExec();
    jobRuntimes = t.getJobRuntimes();
    jobWorkingDir = t.getJobWorkingDir();
    jobEnvVariables = t.getJobEnvVariables();
    jobMaxJobs = t.getJobMaxJobs();
    jobMaxJobsPerUser = t.getJobMaxJobsPerUser();
    jobIsBatch = t.getJobIsBatch();
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = t.getBatchDefaultLogicalQueue();
    jobCapabilities = t.getJobCapabilities();
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************

  /**
   * Set defaults for a TSystem
   */
  public void setDefaults()
  {
    if (StringUtils.isBlank(getOwner())) setOwner(DEFAULT_OWNER);
    if (StringUtils.isBlank(getEffectiveUserId())) setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
    if (getTags() == null) setTags(EMPTY_STR_ARRAY);
    if (getNotes() == null) setNotes(DEFAULT_NOTES);
    // If jobIsBatch and qlist has one value then set default q to that value
    if (getJobIsBatch() && getBatchLogicalQueues() != null && getBatchLogicalQueues().size() == 1)
    {
      setBatchDefaultLogicalQueue(getBatchLogicalQueues().get(0).getName());
    }
  }
  /**
   * Resolve variables for TSystem attributes
   */
  public void resolveVariables(String apiUserId)
  {
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with apiUser.
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) setOwner(apiUserId);

    // Perform variable substitutions that happen at create time: bucketName, rootDir, jobWorkingDir
    // NOTE: effectiveUserId is not processed. Var reference is retained and substitution done as needed when system is retrieved.
    //    ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
    String[] allVarSubstitutions = {apiUserId, owner, tenant};
    setBucketName(StringUtils.replaceEach(bucketName, ALL_VARS, allVarSubstitutions));
    setRootDir(StringUtils.replaceEach(rootDir, ALL_VARS, allVarSubstitutions));
    setJobWorkingDir(StringUtils.replaceEach(jobWorkingDir, ALL_VARS, allVarSubstitutions));
  }

  /**
   * Check constraints on TSystem attributes.
   * Make checks that do not require a dao or service call.
   * Check only internal consistency and restrictions.
   *
   * @return  list of error messages, empty list if no errors
   */
  public List<String> checkAttributeRestrictions()
  {
    var errMessages = new ArrayList<String>();
    checkAttrRequired(errMessages);
    checkAttrValidity(errMessages);
    checkAttrStringLengths(errMessages);
    if (canExec) checkAttrCanExec(errMessages);
    if (isDtn) checkAttrIsDtn(errMessages);
    if (jobIsBatch) checkAttrJobIsBatch(errMessages);
    if (systemType == SystemType.S3) checkAttrObjectStore(errMessages);
    checkAttrMisc(errMessages);
    return errMessages;
  }

  // ************************************************************************
  // *********************** Private methods *********************************
  // ************************************************************************

  /**
   * Check for missing required attributes
   *   systemId, systemType, host, authnMethod.
   */
  private void checkAttrRequired(List<String> errMessages)
  {
    if (StringUtils.isBlank(id)) errMessages.add(LibUtils.getMsg(CREATE_MISSING_ATTR, ID_FIELD));
    if (systemType == null) errMessages.add(LibUtils.getMsg(CREATE_MISSING_ATTR, SYSTEM_TYPE_FIELD));
    if (StringUtils.isBlank(host)) errMessages.add(LibUtils.getMsg(CREATE_MISSING_ATTR, HOST_FIELD));
    if (defaultAuthnMethod == null) errMessages.add(LibUtils.getMsg(CREATE_MISSING_ATTR, DEFAULT_AUTHN_METHOD_FIELD));
  }

  /**
   * Check for invalid attributes
   *   systemId, host
   *   rootDir must start with /
   */
  private void checkAttrValidity(List<String> errMessages)
  {
    if (!StringUtils.isBlank(id) && !isValidId(id)) errMessages.add(LibUtils.getMsg(INVALID_STR_ATTR, ID_FIELD, id));

    if (!StringUtils.isBlank(host) && !isValidHost(host))
      errMessages.add(LibUtils.getMsg(INVALID_STR_ATTR, HOST_FIELD, host));

    if (!StringUtils.isBlank(rootDir) && !rootDir.startsWith("/"))
      errMessages.add(LibUtils.getMsg("SYSLIB_LINUX_ROOTDIR_NOSLASH", rootDir));
  }

  /**
   * Check for attribute strings that exceed limits
   *   id, description, owner, effectiveUserId, bucketName, rootDir
   *   dtnSystemId, dtnMountPoint, dtnMountSourcePath, jobWorkingDir
   */
  private void checkAttrStringLengths(List<String> errMessages)
  {
    if (!StringUtils.isBlank(id) && id.length() > MAX_ID_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, ID_FIELD, MAX_ID_LEN));
    }

    if (!StringUtils.isBlank(dtnSystemId) && dtnSystemId.length() > MAX_ID_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, DTN_SYSTEM_ID_FIELD, MAX_ID_LEN));
    }

    if (!StringUtils.isBlank(description) && description.length() > MAX_DESCRIPTION_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, DESCRIPTION_FIELD, MAX_DESCRIPTION_LEN));
    }

    if (!StringUtils.isBlank(owner) && owner.length() > MAX_USERNAME_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, OWNER_FIELD, MAX_USERNAME_LEN));
    }

    if (!StringUtils.isBlank(effectiveUserId) && effectiveUserId.length() > MAX_USERNAME_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, EFFECTIVE_USER_ID_FIELD, MAX_USERNAME_LEN));
    }

    if (!StringUtils.isBlank(bucketName) && bucketName.length() > MAX_BUCKETNAME_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, BUCKET_NAME_FIELD, MAX_BUCKETNAME_LEN));
    }

    if (!StringUtils.isBlank(rootDir) && rootDir.length() > MAX_PATH_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, ROOT_DIR_FIELD, MAX_PATH_LEN));
    }

    if (!StringUtils.isBlank(dtnMountPoint) && dtnMountPoint.length() > MAX_PATH_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, DTN_MOUNT_POINT_FIELD, MAX_PATH_LEN));
    }

    if (!StringUtils.isBlank(dtnMountSourcePath) && dtnMountSourcePath.length() > MAX_PATH_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, DTN_MOUNT_SOURCE_PATH_FIELD, MAX_PATH_LEN));
    }

    if (!StringUtils.isBlank(jobWorkingDir) && jobWorkingDir.length() > MAX_PATH_LEN)
    {
      errMessages.add(LibUtils.getMsg(TOO_LONG_ATTR, JOB_WORKING_DIR_FIELD, MAX_PATH_LEN));
    }
  }

  /**
   * Check attributes related to canExec
   *  If canExec is true then jobWorkingDir must be set and jobRuntimes must have at least one entry.
   */
  private void checkAttrCanExec(List<String> errMessages)
  {
    if (StringUtils.isBlank(jobWorkingDir)) errMessages.add(LibUtils.getMsg("SYSLIB_CANEXEC_NO_JOBWORKINGDIR_INPUT"));
    if (jobRuntimes == null || jobRuntimes.isEmpty()) errMessages.add(LibUtils.getMsg("SYSLIB_CANEXEC_NO_JOBRUNTIME_INPUT"));
  }

  /**
   * Check attributes related to isDtn
   *  If isDtn is true then canExec must be false and the following attributes may not be set:
   *    dtnSystemId, dtnMountSourcePath, dtnMountPoint, all job execution related attributes.
   */
  private void checkAttrIsDtn(List<String> errMessages)
  {
    if (canExec) errMessages.add(LibUtils.getMsg("SYSLIB_DTN_CANEXEC"));

    if (!StringUtils.isBlank(dtnSystemId) || !StringUtils.isBlank(dtnMountPoint) ||
            !StringUtils.isBlank(dtnMountSourcePath))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_DTN_DTNATTRS"));
    }
    if (!StringUtils.isBlank(jobWorkingDir) ||
              !(jobCapabilities == null || jobCapabilities.isEmpty()) ||
              !(jobRuntimes == null || jobRuntimes.isEmpty()) ||
              !(jobEnvVariables == null || jobEnvVariables.length == 0) ||
              !(batchScheduler == null) ||
              !StringUtils.isBlank(batchDefaultLogicalQueue) ||
              !(batchLogicalQueues == null || batchLogicalQueues.isEmpty()) )
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_DTN_JOBATTRS"));
    }
  }

  /**
   * Check attributes related to jobIsBatch
   * If jobIsBatch is true
   *   batchScheduler must be specified
   *   batchLogicalQueues must not be empty
   *   batchLogicalDefaultQueue must be set
   *   batchLogicalDefaultQueue must be in the list of queues
   *   If batchLogicalQueues has more then one item then batchDefaultLogicalQueue must be set
   *   batchDefaultLogicalQueue must be in the list of logical queues.
   */
  private void checkAttrJobIsBatch(List<String> errMessages)
  {
    if (batchScheduler == null) errMessages.add(LibUtils.getMsg("SYSLIB_ISBATCH_NOSCHED"));

    if (batchLogicalQueues == null || batchLogicalQueues.isEmpty())
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_ISBATCH_NOQUEUES"));
    }

    if (StringUtils.isBlank(batchDefaultLogicalQueue)) errMessages.add(LibUtils.getMsg("SYSLIB_ISBATCH_NODEFAULTQ"));

    // Check that default queue is in the list of queues
    if (!StringUtils.isBlank(batchDefaultLogicalQueue))
    {
      boolean inList = false;
      if (batchLogicalQueues != null)
      {
        for (LogicalQueue lq : batchLogicalQueues)
        {
          if (batchDefaultLogicalQueue.equals(lq.getName()))
          {
            inList = true;
            break;
          }
        }
      }
      if (!inList) errMessages.add(LibUtils.getMsg("SYSLIB_ISBATCH_DEFAULTQ_NOTINLIST", batchDefaultLogicalQueue));
    }
  }

  /**
   * Check attributes related to systems of type OBJECT_STORE
   *  If type is OBJECT_STORE then bucketName must be set, isExec and isDtn must be false.
   */
  private void checkAttrObjectStore(List<String> errMessages)
  {
    // bucketName must be set
    if (StringUtils.isBlank(bucketName)) errMessages.add(LibUtils.getMsg("SYSLIB_OBJSTORE_NOBUCKET_INPUT"));
    // canExec must be false
    if (canExec) errMessages.add(LibUtils.getMsg("SYSLIB_OBJSTORE_CANEXEC_INPUT"));
    // isDtn must be false
    if (isDtn) errMessages.add(LibUtils.getMsg("SYSLIB_OBJSTORE_ISDTN_INPUT"));
  }

  /**
   * Check misc attribute restrictions
   *  If systemType is LINUX then rootDir is required.
   *  effectiveUserId is restricted.
   *  If effectiveUserId is dynamic then providing credentials is disallowed
   *  If credential is provided and contains ssh keys then validate them
   */
  private void checkAttrMisc(List<String> errMessages)
  {
    // LINUX system requires rootDir
    if (systemType == SystemType.LINUX && StringUtils.isBlank(rootDir))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_LINUX_NOROOTDIR"));
    }

    // For CERT authn the effectiveUserId cannot be static string other than owner
    if (defaultAuthnMethod.equals(AuthnMethod.CERT) &&
            !effectiveUserId.equals(TSystem.APIUSERID_VAR) &&
            !effectiveUserId.equals(TSystem.OWNER_VAR) &&
            !StringUtils.isBlank(owner) &&
            !effectiveUserId.equals(owner))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_INVALID_EFFECTIVEUSERID_INPUT"));
    }

    // If effectiveUserId is dynamic then providing credentials is disallowed
    if (effectiveUserId.equals(TSystem.APIUSERID_VAR) && authnCredential != null)
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_CRED_DISALLOWED_INPUT"));
    }

    // If credential is provided and contains ssh keys then validate private key format
    if (authnCredential != null && !StringUtils.isBlank(authnCredential.getPrivateKey()))
    {
      if (!authnCredential.isValidPrivateSshKey())
        errMessages.add(LibUtils.getMsg("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY1"));
    }
  }

  /**
   * Validate an ID string.
   * Must start alphabetic and contain only alphanumeric and 4 special characters: - . _ ~
   */
  private boolean isValidId(String id) { return id.matches(PATTERN_VALID_ID); }

  /**
   * Validate a host string.
   * Check if a string is a valid hostname or IP address.
   * Use methods from org.apache.commons.validator.routines.
   */
  private boolean isValidHost(String host)
  {
    // First check for valid IP address, then for valid domain name
    if (DomainValidator.getInstance().isValid(host) || InetAddressValidator.getInstance().isValid(host)) return true;
    else return false;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public int getSeqId() { return seqId; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }

  public SystemType getSystemType() { return systemType; }

  public String getId() { return id; }

  public String getDescription() { return description; }
  public TSystem setDescription(String d) { description = d; return this; }

  public String getOwner() { return owner; }
  public TSystem setOwner(String s) { owner = s;  return this;}

  public String getHost() { return host; }
  public TSystem setHost(String s) { host = s; return this; }

  public boolean isEnabled() { return enabled; }
  public TSystem setEnabled(boolean b) { enabled = b;  return this; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public TSystem setEffectiveUserId(String s) { effectiveUserId = s; return this; }

  public AuthnMethod getDefaultAuthnMethod() { return defaultAuthnMethod; }
  public TSystem setDefaultAuthnMethod(AuthnMethod a) { defaultAuthnMethod = a; return this; }

  public Credential getAuthnCredential() { return authnCredential; }
  public TSystem setAuthnCredential(Credential c) {authnCredential = c; return this; }

  public String getBucketName() { return bucketName; }
  public TSystem setBucketName(String s) { bucketName = s; return this; }

  public String getRootDir() { return rootDir; }
  public TSystem setRootDir(String s) { rootDir = s; return this; }

  public int getPort() { return port; }
  public TSystem setPort(int i) { port = i; return this; }

  public boolean isUseProxy() { return useProxy; }
  public TSystem setUseProxy(boolean b) { useProxy = b; return this; }

  public String getProxyHost() { return proxyHost; }
  public TSystem setProxyHost(String s) { proxyHost = s; return this; }

  public int getProxyPort() { return proxyPort; }
  public TSystem setProxyPort(int i) { proxyPort = i; return this; }

  public String getDtnSystemId() { return dtnSystemId; }
  public TSystem setDtnSystemId(String s) { dtnSystemId = s; return this; }

  public String getDtnMountPoint() { return dtnMountPoint; }
  public TSystem setDtnMountPoint(String s) { dtnMountPoint = s; return this; }

  public String getDtnMountSourcePath() { return dtnMountSourcePath; }
  public TSystem setDtnMountSourcePath(String s) { dtnMountSourcePath = s; return this; }

  public boolean isDtn() { return isDtn; }

  public boolean getCanExec() { return canExec; }

  public List<JobRuntime> getJobRuntimes() {
    return (jobRuntimes == null) ? null : new ArrayList<>(jobRuntimes);
  }
  public TSystem setJobRuntimes(List<JobRuntime> jrs) {
    jobRuntimes = (jrs == null) ? null : new ArrayList<>(jrs);
    return this;
  }

  public String getJobWorkingDir() { return jobWorkingDir; }
  public TSystem setJobWorkingDir(String s) { jobWorkingDir = s; return this; }

  public String[] getJobEnvVariables() {
    return (jobEnvVariables == null) ? DEFAULT_JOBENV_VARIABLES : jobEnvVariables.clone();
  }
  public TSystem setJobEnvVariables(String[] jev) {
    jobEnvVariables = (jev == null) ? DEFAULT_JOBENV_VARIABLES : jev.clone();
    return this;
  }

  public int getJobMaxJobs() { return jobMaxJobs; }
  public TSystem setJobMaxJobs(int i) { jobMaxJobs = i; return this; }

  public int getJobMaxJobsPerUser() { return jobMaxJobsPerUser; }
  public TSystem setJobMaxJobsPerUser(int i) { jobMaxJobsPerUser = i; return this; }

  public boolean getJobIsBatch() { return jobIsBatch; }
  public TSystem setJobIsBatch(boolean b) { jobIsBatch = b; return this; }

  public SchedulerType getBatchScheduler() { return batchScheduler; }
  public TSystem setBatchScheduler(SchedulerType s) { batchScheduler = s; return this; }

  public List<LogicalQueue> getBatchLogicalQueues() {
    return (batchLogicalQueues == null) ? null : new ArrayList<>(batchLogicalQueues);
  }
  public TSystem setBatchLogicalQueues(List<LogicalQueue> q) {
    batchLogicalQueues = (q == null) ? null : new ArrayList<>(q);
    return this;
  }

  public String getBatchDefaultLogicalQueue() { return batchDefaultLogicalQueue; }
  public TSystem setBatchDefaultLogicalQueue(String s) { batchDefaultLogicalQueue = s; return this; }

  public List<Capability> getJobCapabilities() {
    return (jobCapabilities == null) ? null : new ArrayList<>(jobCapabilities);
  }
  public TSystem setJobCapabilities(List<Capability> c) {
    jobCapabilities = (c == null) ? null : new ArrayList<>(c);
    return this;
  }

  public String[] getTags() {
    return (tags == null) ? EMPTY_STR_ARRAY : tags.clone();
  }
  public TSystem setTags(String[] t) {
    tags = (t == null) ? EMPTY_STR_ARRAY : t.clone();
    return this;
  }

  public Object getNotes() { return notes; }
  public TSystem setNotes(Object n) { notes = n; return this; }

  public UUID getUuid() { return uuid; }
  public TSystem setUuid(UUID u) { uuid = u; return this; }

  public boolean isDeleted() { return deleted; }
}
