package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

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
  public static final Set<String> RESERVED_ID_SET = new HashSet<>(Set.of("HEALTHCHECK", "READYCHECK", "SEARCH",
                                                                         "SCHEDULERPROFILE"));

  // Set of attributes (i.e. column names) not supported in searches
  public static final Set<String> SEARCH_ATTRS_UNSUPPORTED =
          new HashSet<>(Set.of("authn_credential", "job_runtimes", "job_env_variables", "batch_logical_queues",
                               "notes", "job_capabilities"));

  public static final String PERMISSION_WILDCARD = "*";

  //
  public static final String APIUSERID_STR = "apiUserId";
  public static final String EFFUSERID_STR = "effectiveUserId";

  // Substitution variables
  public static final String APIUSERID_VAR = String.format("${%s}", APIUSERID_STR);
  public static final String EFFUSERID_VAR = String.format("${%s}", EFFUSERID_STR);
  public static final String OWNER_VAR = "${owner}";
  public static final String TENANT_VAR = "${tenant}";
  public static final String HOST_EVAL = "HOST_EVAL";

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
  private static final String[] ROOTDIR_VARS = {OWNER_VAR, TENANT_VAR};

  // Attribute names, also used as field names in Json
  public static final String TENANT_FIELD = "tenant";
  public static final String ID_FIELD = "id";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String SYSTEM_TYPE_FIELD = "systemType";
  public static final String OWNER_FIELD = "owner";
  public static final String HOST_FIELD = "host";
  public static final String ENABLED_FIELD = "enabled";
  public static final String DELETED_FIELD = "deleted";
  public static final String EFFECTIVE_USER_ID_FIELD = EFFUSERID_STR;
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
  public static final String IS_PUBLIC_FIELD = "isPublic";
  public static final String IS_DYNAMIC_EFFECTIVE_USER = "isDynamicEffectiveUser";
  public static final String IS_DTN_FIELD = "isDtn";
  public static final String CAN_EXEC_FIELD = "canExec";
  public static final String CAN_RUN_BATCH_FIELD = "canRunBatch";
  public static final String ENABLE_CMD_PREFIX_FIELD = "enableCmdPrefix";
  public static final String MPI_CMD_FIELD = "mpiCmd";
  public static final String JOB_RUNTIMES_FIELD = "jobRuntimes";
  public static final String JOB_WORKING_DIR_FIELD = "jobWorkingDir";
  public static final String JOB_ENV_VARIABLES_FIELD = "jobEnvVariables";
  public static final String JOB_MAX_JOBS_FIELD = "jobMaxJobs";
  public static final String JOB_MAX_JOBS_PER_USER_FIELD = "jobMaxJobsPerUser";
  public static final String BATCH_SCHEDULER_FIELD = "batchScheduler";
  public static final String BATCH_LOGICAL_QUEUES_FIELD = "batchLogicalQueues";
  public static final String BATCH_DEFAULT_LOGICAL_QUEUE_FIELD = "batchDefaultLogicalQueue";
  public static final String BATCH_SCHEDULER_PROFILE_FIELD = "batchSchedulerProfile";
  public static final String JOB_CAPABILITIES_FIELD = "jobCapabilities";
  public static final String TAGS_FIELD = "tags";
  public static final String NOTES_FIELD = "notes";
  public static final String PARENT_ID = "parentId";
  public static final String ALLOW_CHILDREN = "allowChildren";
  public static final String IMPORT_REF_ID = "importRefId";
  public static final String UUID_FIELD = "uuid";
  public static final String CREATED_FIELD = "created";
  public static final String UPDATED_FIELD = "updated";

  // Default values
  public static final String[] EMPTY_STR_ARRAY = new String[0];
  public static final String DEFAULT_OWNER = APIUSERID_VAR;
  public static final boolean DEFAULT_ENABLED = true;
  public static final String DEFAULT_EFFECTIVEUSERID = APIUSERID_VAR;
  public static final JsonElement DEFAULT_JOBENV_VARIABLES = TapisGsonUtils.getGson().fromJson("[]", JsonElement.class);
  public static final JsonElement DEFAULT_BATCH_LOGICAL_QUEUES = TapisGsonUtils.getGson().fromJson("[]", JsonElement.class);
  public static final JsonElement DEFAULT_JOB_CAPABILITIES = TapisGsonUtils.getGson().fromJson("[]", JsonElement.class);
  public static final JsonObject DEFAULT_NOTES = TapisGsonUtils.getGson().fromJson("{}", JsonObject.class);
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = null;
  public static final int DEFAULT_PROXYPORT = -1;
  public static final int DEFAULT_JOBMAXJOBS = -1;
  public static final int DEFAULT_JOBMAXJOBSPERUSER = -1;
  public static final boolean DEFAULT_CAN_RUN_BATCH = false;
  public static final boolean DEFAULT_IS_PUBLIC = false;
  public static final boolean DEFAULT_IS_DYNAMIC_EFFECTIVE_USER = false;
  public static final boolean DEFAULT_ENABLE_CMD_PREFIX = false;
  public static final boolean DEFAULT_CHILD_ENABLED = true;

  // Validation pattern strings
  // ID Must start alphabetic and contain only alphanumeric and 4 special characters: - . _ ~
  public static final String PATTERN_STR_VALID_ID = "^[a-zA-Z]([a-zA-Z0-9]|[-\\._~])*";

  // If rootDir contains HOST_EVAL then rootDir must match a certain pattern at start: "HOST_EVAL($VARIABLE)...
  // Web search indicates for linux, env var names should start with single alpha/underscore followed by 0 or more alphanum/underscore
  // Must start with "HOST_EVAL($", followed by 1 alpha/underscore followed by 0 or more alphanum/underscore followed by ")"
  public static final String PATTERN_STR_HOST_EVAL = "^HOST_EVAL\\(\\$[a-zA-Z_]+([a-zA-Z0-9_])*\\)(.*)";

  // Validation constants
  public static final Integer MAX_ID_LEN = 80;
  public static final Integer MAX_DESCRIPTION_LEN = 2048;
  public static final Integer MAX_PATH_LEN = 4096;
  public static final Integer MAX_USERNAME_LEN = 60;
  public static final Integer MAX_BUCKETNAME_LEN = 63;
  public static final Integer MAX_QUEUENAME_LEN = 128;
  public static final Integer MAX_HPCQUEUENAME_LEN = 128;
  public static final Integer MAX_RUNTIME_VER_LEN = 128;
  public static final Integer MAX_CAPABILITYNAME_LEN = 128;
  public static final Integer MAX_TAG_LEN = 128;

  // Message keys
  public static final String CREATE_MISSING_ATTR = "SYSLIB_CREATE_MISSING_ATTR";
  public static final String INVALID_STR_ATTR = "SYSLIB_INVALID_STR_ATTR";
  public static final String TOO_LONG_ATTR = "SYSLIB_TOO_LONG_ATTR";


  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum SystemType {LINUX, S3, IRODS, GLOBUS}
  public enum SystemOperation {create, read, modify, execute, delete, undelete, hardDelete, changeOwner, enable, disable,
                               getPerms, grantPerms, revokePerms, setCred, removeCred, getCred, checkCred,
                               getGlobusAuthInfo, setAccessRefreshTokens}
  public enum Permission {READ, MODIFY, EXECUTE}
  public enum AuthnMethod {PASSWORD, PKI_KEYS, ACCESS_KEY, TOKEN, CERT}
  public enum SchedulerType {SLURM, CONDOR, PBS, SGE, UGE, TORQUE}

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private boolean isPublic = DEFAULT_IS_PUBLIC;
  private boolean isDynamicEffectiveUser = DEFAULT_IS_DYNAMIC_EFFECTIVE_USER;

  // NOTE: In order to use jersey's SelectableEntityFilteringFeature fields cannot be final.
  private int seqId;         // Unique database sequence number
  private String tenant;     // Name of the tenant for which the system is defined
  private final String id;       // Name of the system
  private String description; // Full description of the system
  private final SystemType systemType; // Type of system, e.g. LINUX, S3
  private String owner;      // User who owns the system and has full privileges
  private String host;       // Host name or IP address
  private boolean enabled; // Indicates if systems is currently enabled
  private String effectiveUserId; // User to use when accessing system, may be static or dynamic
  private AuthnMethod defaultAuthnMethod; // How access authorization is handled by default
  private Credential authnCredential; // Credential to be stored in or retrieved from the Security Kernel
  private String bucketName; // Name of bucket for system of type S3
  private String rootDir;    // Effective root directory for system of type LINUX, can also be used for system of type S3
  private int port;          // Port number used to access the system
  private boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private String proxyHost;  // Name or IP address of proxy host
  private int proxyPort;     // Port number for proxy host
  private String dtnSystemId;
  private String dtnMountPoint;
  private String dtnMountSourcePath;
  private boolean isDtn;
  private final boolean canExec; // Indicates if system will be used to execute jobs
  private boolean canRunBatch;
  private boolean enableCmdPrefix = DEFAULT_ENABLE_CMD_PREFIX;
  private String mpiCmd;
  private List<JobRuntime> jobRuntimes;
  private String jobWorkingDir; // Parent directory from which a job is run. Relative to effective root dir.
  private List<KeyValuePair> jobEnvVariables;
  private int jobMaxJobs;
  private int jobMaxJobsPerUser;
  private SchedulerType batchScheduler;
  private List<LogicalQueue> batchLogicalQueues;
  private String batchDefaultLogicalQueue;
  private String batchSchedulerProfile;
  private List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private String[] tags; // List of arbitrary tags as strings
  private Object notes;   // Simple metadata as json.
  private String importRefId;
  private UUID uuid;
  private boolean deleted;
  private String parentId;      // User who owns the system and has full privileges
  private boolean allowChildren;

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
    canRunBatch = t.getCanRunBatch();
    enableCmdPrefix = t.isEnableCmdPrefix();
    mpiCmd = t.getMpiCmd();
    jobRuntimes = t.getJobRuntimes();
    jobWorkingDir = t.getJobWorkingDir();
    jobEnvVariables = t.getJobEnvVariables();
    jobMaxJobs = t.getJobMaxJobs();
    jobMaxJobsPerUser = t.getJobMaxJobsPerUser();
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = t.getBatchDefaultLogicalQueue();
    batchSchedulerProfile = t.getBatchSchedulerProfile();
    jobCapabilities = t.getJobCapabilities();
    allowChildren = t.isAllowChildren();
    parentId = t.getParentId();
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
    importRefId = t.getImportRefId();
    uuid = t.getUuid();
    deleted = t.isDeleted();
    isPublic = t.isPublic();
    isDynamicEffectiveUser = t.isDynamicEffectiveUser();
  }

  /**
   * Constructor for creating a child system based on a parent system.
   * @param parentSystem
   * @param childId
   * @param childEffectiveUserId
   * @param childRootDir
   * @param childOwner
   */
  public TSystem(TSystem parentSystem, String childId, String childEffectiveUserId, String childRootDir, String childOwner, boolean enabled) {
    this(parentSystem.getSeqId(), parentSystem.getTenant(), childId, parentSystem.getDescription(),
            parentSystem.getSystemType(), childOwner, parentSystem.getHost(), enabled,
            childEffectiveUserId, parentSystem.getDefaultAuthnMethod(), parentSystem.getBucketName(),
            childRootDir, parentSystem.getPort(), parentSystem.isUseProxy(), parentSystem.getProxyHost(),
            parentSystem.getProxyPort(), parentSystem.getDtnSystemId(), parentSystem.getDtnMountPoint(), parentSystem.getDtnMountSourcePath(),
            parentSystem.isDtn(), parentSystem.getCanExec(), parentSystem.getJobRuntimes(), parentSystem.getJobWorkingDir(),
            parentSystem.getJobEnvVariables(), parentSystem.getJobMaxJobs(), parentSystem.getJobMaxJobsPerUser(), parentSystem.getCanRunBatch(),
            parentSystem.isEnableCmdPrefix(), parentSystem.getMpiCmd(), parentSystem.getBatchScheduler(), parentSystem.getBatchLogicalQueues(),
            parentSystem.getBatchDefaultLogicalQueue(), parentSystem.getBatchSchedulerProfile(), parentSystem.getJobCapabilities(),
            parentSystem.getTags(), parentSystem.getNotes(), parentSystem.getImportRefId(), parentSystem.getUuid(),
            parentSystem.isDeleted(), /* allowChildren:false */  false, parentSystem.getId(),
            parentSystem.getCreated(), parentSystem.getUpdated());
  }

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public TSystem(int seqId1, String tenant1, String id1, String description1, SystemType systemType1,
                 String owner1, String host1, boolean enabled1, String effectiveUserId1, AuthnMethod defaultAuthnMethod1,
                 String bucketName1, String rootDir1,
                 int port1, boolean useProxy1, String proxyHost1, int proxyPort1,
                 String dtnSystemId1, String dtnMountPoint1, String dtnMountSourcePath1, boolean isDtn1,
                 boolean canExec1, List<JobRuntime> jobRuntimes1, String jobWorkingDir1, List<KeyValuePair> jobEnvVariables1,
                 int jobMaxJobs1, int jobMaxJobsPerUser1, boolean canRunBatch1, boolean enableCmdPrefix1, String mpiCmd1,
                 SchedulerType batchScheduler1, List<LogicalQueue> batchLogicalQueues1, String batchDefaultLogicalQueue1,
                 String batchSchedulerProfile1, List<Capability> jobCapabilities1,
                 String[] tags1, Object notes1, String importRefId1, UUID uuid1, boolean deleted1, boolean allowChildren1,
                 String parentId1, Instant created1, Instant updated1)
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
    jobRuntimes = jobRuntimes1;
    jobWorkingDir = jobWorkingDir1;
    jobEnvVariables = jobEnvVariables1;
    jobMaxJobs = jobMaxJobs1;
    jobMaxJobsPerUser = jobMaxJobsPerUser1;
    canRunBatch = canRunBatch1;
    enableCmdPrefix = enableCmdPrefix1;
    mpiCmd = mpiCmd1;
    batchScheduler = batchScheduler1;
    batchLogicalQueues = batchLogicalQueues1;
    batchDefaultLogicalQueue = batchDefaultLogicalQueue1;
    batchSchedulerProfile = batchSchedulerProfile1;
    jobCapabilities = jobCapabilities1;
    tags = (tags1 == null) ? EMPTY_STR_ARRAY : tags1.clone();
    notes = notes1;
    importRefId = importRefId1;
    allowChildren = allowChildren1;
    uuid = uuid1;
    deleted = deleted1;
    created = created1;
    updated = updated1;
    parentId = parentId1;
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
    canRunBatch = t.getCanRunBatch();
    enableCmdPrefix = t.isEnableCmdPrefix();
    mpiCmd = t.getMpiCmd();
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = t.getBatchDefaultLogicalQueue();
    batchSchedulerProfile = t.getBatchSchedulerProfile();
    jobCapabilities = t.getJobCapabilities();
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
    importRefId = t.getImportRefId();
    isPublic = t.isPublic();
    isDynamicEffectiveUser = t.isDynamicEffectiveUser();
    allowChildren = t.isAllowChildren();
    parentId = t.getParentId();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************

  /**
   * Set defaults for a TSystem for attributes: owner, effectiveUserId, tags, notes
   */
  public void setDefaults()
  {
    if (StringUtils.isBlank(getOwner())) setOwner(DEFAULT_OWNER);
    if (StringUtils.isBlank(getEffectiveUserId())) setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
    if (getTags() == null) setTags(EMPTY_STR_ARRAY);
    if (getNotes() == null) setNotes(DEFAULT_NOTES);
    // If canRunBatch and qlist has one value then set default q to that value
    if (getCanRunBatch() && getBatchLogicalQueues() != null && getBatchLogicalQueues().size() == 1)
    {
      setBatchDefaultLogicalQueue(getBatchLogicalQueues().get(0).getName());
    }
    // Process request to create list of job env variables with proper defaults.
    setJobEnvVariables(processJobEnvVariables(jobEnvVariables));
  }
  /**
   * Resolve variables for TSystem attributes
   */
  public void resolveVariablesAtCreate(String oboUser)
  {
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with oboUser.
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) setOwner(oboUser);

    // Handle case where effectiveUserId is "${owner}". Only at create time does this need resolving.
    // When this method is done effectiveUserId should be either "${apiUserId} or a static string with no
    //   Tapis Systems variables to resolve.
    if (OWNER_VAR.equals(effectiveUserId)) setEffectiveUserId(owner);

    // Perform variable substitutions that happen at create time: bucketName, rootDir, jobWorkingDir
    //    ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
    //    ROOTDIR_VARS = {OWNER_VAR, TENANT_VAR};
    String[] allVarSubstitutions = {oboUser, owner, tenant};
    String[] rootDirVarSubstitutions = {owner, tenant};
    setBucketName(StringUtils.replaceEach(bucketName, ALL_VARS, allVarSubstitutions));
    setJobWorkingDir(StringUtils.replaceEach(jobWorkingDir, ALL_VARS, allVarSubstitutions));
    setRootDir(StringUtils.replaceEach(rootDir, ROOTDIR_VARS, rootDirVarSubstitutions));
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
    if (canRunBatch) checkAttrCanRunBatch(errMessages);
    if (systemType == SystemType.S3) checkAttrS3(errMessages);
    checkAttrMisc(errMessages);
    return errMessages;
  }

  /**
   * Validate an ID string.
   * Must start alphabetic and contain only alphanumeric and 4 special characters: - . _ ~
   */
  public static boolean isValidId(String id) { return id.matches(PATTERN_STR_VALID_ID); }

  /*
   * Process jobEnvVariables from request to create list of job env variables with proper defaults.
   */
  public static List<KeyValuePair> processJobEnvVariables(List<KeyValuePair> requestEnvVars)
  {
    var envVars = new ArrayList<KeyValuePair>();
    // If no items return an empty list
    if (requestEnvVars == null || requestEnvVars.isEmpty()) return envVars;

    // Process each item. Constructor will set appropriate defaults
    for (KeyValuePair kv : requestEnvVars)
    {
      envVars.add(new KeyValuePair(kv.getKey(), kv.getValue(), kv.getDescription(), kv.getInputMode(), kv.getNotes()));
    }
    return envVars;
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
   *   For LINUX or IRODS rootDir must start with /
   */
  private void checkAttrValidity(List<String> errMessages)
  {
    if (!StringUtils.isBlank(id) && !isValidId(id)) errMessages.add(LibUtils.getMsg(INVALID_STR_ATTR, ID_FIELD, id));

    if (!StringUtils.isBlank(host) && !isValidHost(host))
      errMessages.add(LibUtils.getMsg(INVALID_STR_ATTR, HOST_FIELD, host));

    if (SystemType.LINUX.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      if (!StringUtils.isBlank(rootDir) && !rootDir.startsWith("/"))
        errMessages.add(LibUtils.getMsg("SYSLIB_LINUX_ROOTDIR_NOSLASH", rootDir));
    }
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
   *  If canExec is true then:
   *   - systemType must be LINUX
   *   - jobWorkingDir must be set
   *   - jobRuntimes must have at least one entry.
   */
  private void checkAttrCanExec(List<String> errMessages)
  {
    if (!SystemType.LINUX.equals(systemType)) errMessages.add(LibUtils.getMsg("SYSLIB_CANEXEC_INVALID_SYSTYPE", systemType.name()));
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
              !(jobEnvVariables == null || jobEnvVariables.isEmpty()) ||
              !(batchScheduler == null) ||
              !StringUtils.isBlank(batchDefaultLogicalQueue) ||
              !StringUtils.isBlank(batchSchedulerProfile) ||
              !(batchLogicalQueues == null || batchLogicalQueues.isEmpty()) )
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_DTN_JOBATTRS"));
    }
  }

  /**
   * Check attributes related to canRunBatch
   * If canRunBatch is true
   *   batchScheduler must be specified
   *   batchLogicalQueues must not be empty
   *   batchLogicalDefaultQueue must be set
   *   batchLogicalDefaultQueue must be in the list of queues
   *   If batchLogicalQueues has more than one item then batchDefaultLogicalQueue must be set
   *   batchDefaultLogicalQueue must be in the list of logical queues.
   */
  private void checkAttrCanRunBatch(List<String> errMessages)
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
   * Check attributes related to systems of type S3
   *  If type is S3 then bucketName must be set, isExec and isDtn must be false.
   */
  private void checkAttrS3(List<String> errMessages)
  {
    // bucketName must be set
    if (StringUtils.isBlank(bucketName)) errMessages.add(LibUtils.getMsg("SYSLIB_S3_NOBUCKET_INPUT"));
    // canExec must be false
    if (canExec) errMessages.add(LibUtils.getMsg("SYSLIB_S3_CANEXEC_INPUT"));
    // isDtn must be false
    if (isDtn) errMessages.add(LibUtils.getMsg("SYSLIB_S3_ISDTN_INPUT"));
  }

  /**
   * Check misc attribute restrictions
   *  If systemType is LINUX or IRODS then:
   *           - rootDir is required
   *  If systemType is IRODS then port is required.
   *  effectiveUserId is restricted.
   *  If effectiveUserId is dynamic then providing credentials is disallowed
   *  If credential is provided and contains ssh keys then validate them
   */
  private void checkAttrMisc(List<String> errMessages)
  {
    // LINUX and IRODS systems require rootDir
    if ((systemType == SystemType.LINUX || systemType == SystemType.IRODS) && StringUtils.isBlank(rootDir))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_NOROOTDIR", systemType.name()));
    }

    // IRODS systems require port
    if (systemType == SystemType.IRODS && !isValidPort(port))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_NOPORT", systemType.name()));
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
   * Validate a host string.
   * Check if a string is a valid hostname or IP address.
   * Use methods from org.apache.commons.validator.routines.
   */
  private boolean isValidHost(String host)
  {
    // First check for valid IP address, then for valid domain name
    boolean allowLocal = true;
    if (DomainValidator.getInstance(allowLocal).isValid(host) || InetAddressValidator.getInstance().isValid(host)) return true;
    else return false;
  }

  /**
   * Validate a port number. Range is 1-65535
   */
  private boolean isValidPort(int p)
  {
    return (p > 0 && p <= 65535);
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

  public boolean getCanRunBatch() { return canRunBatch; }
  public TSystem setCanRunBatch(boolean b) { canRunBatch = b; return this; }

  public String getMpiCmd() { return mpiCmd; }
  public TSystem setMpiCmd(String s) { mpiCmd = s; return this; }

  public boolean isEnableCmdPrefix() {
    return enableCmdPrefix;
  }

  public void setEnableCmdPrefix(boolean enableCmdPrefix) {
    this.enableCmdPrefix = enableCmdPrefix;
  }

  public List<JobRuntime> getJobRuntimes() {
    return (jobRuntimes == null) ? null : new ArrayList<>(jobRuntimes);
  }
  public TSystem setJobRuntimes(List<JobRuntime> jrs) {
    jobRuntimes = (jrs == null) ? null : new ArrayList<>(jrs);
    return this;
  }

  public String getJobWorkingDir() { return jobWorkingDir; }
  public TSystem setJobWorkingDir(String s) { jobWorkingDir = s; return this; }

  public List<KeyValuePair> getJobEnvVariables() {
    return (jobEnvVariables == null) ? null : new ArrayList<>(jobEnvVariables);
  }
  public TSystem setJobEnvVariables(List<KeyValuePair> jev) {
    jobEnvVariables = (jev == null) ? null : new ArrayList<>(jev);
    return this;
  }

  public int getJobMaxJobs() { return jobMaxJobs; }
  public TSystem setJobMaxJobs(int i) { jobMaxJobs = i; return this; }

  public int getJobMaxJobsPerUser() { return jobMaxJobsPerUser; }
  public TSystem setJobMaxJobsPerUser(int i) { jobMaxJobsPerUser = i; return this; }

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

  public String getBatchSchedulerProfile() { return batchSchedulerProfile; }
  public TSystem setBatchSchedulerProfile(String s) { batchSchedulerProfile = s; return this; }

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

  public String getImportRefId() { return importRefId; }
  public TSystem setImportRefId(String s) { importRefId = s; return this; }

  public UUID getUuid() { return uuid; }
  public TSystem setUuid(UUID u) { uuid = u; return this; }

  public boolean isDeleted() { return deleted; }

  public boolean isPublic() { return isPublic; }
  public void setIsPublic(boolean b) { isPublic = b;  }
  public boolean isDynamicEffectiveUser() { return isDynamicEffectiveUser; }
  public void setIsDynamicEffectiveUser(boolean b) { isDynamicEffectiveUser = b;  }
  public String getParentId() {
    return parentId;
  }
  public boolean isAllowChildren() {
    return allowChildren;
  }
  public void setAllowChildren(boolean allowChildren) {
    this.allowChildren = allowChildren;
  }
}
