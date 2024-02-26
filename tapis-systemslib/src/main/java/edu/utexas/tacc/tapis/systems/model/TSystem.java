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

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.PathSanitizer;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import static edu.utexas.tacc.tapis.systems.model.KeyValuePair.KeyValueInputMode.FIXED;
import static edu.utexas.tacc.tapis.systems.model.KeyValuePair.KeyValueInputMode.REQUIRED;
import static edu.utexas.tacc.tapis.systems.model.KeyValuePair.RESERVED_PREFIX;
import static edu.utexas.tacc.tapis.systems.model.KeyValuePair.VALUE_NOT_SET;

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
  public static final Set<String> RESERVED_ID_SET
          = new HashSet<>(Set.of("HEALTHCHECK", "READYCHECK", "SEARCH", "SCHEDULERPROFILE",
                                 "SHARE", "UNSHARE", "SHARE_PUBLIC", "UNSHARE_PUBLIC"));

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

  private static final String[] ALL_VARS = {APIUSERID_VAR, OWNER_VAR, TENANT_VAR};
  private static final String[] ROOTDIR_VARS = {OWNER_VAR, TENANT_VAR};

  // Special select strings used for determining what attributes are returned in a response
  public static final String SEL_ALL_ATTRS = "allAttributes";
  public static final String SEL_SUMMARY_ATTRS = "summaryAttributes";

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
  public static final String IS_PUBLIC_FIELD = "isPublic";
  public static final String IS_DYNAMIC_EFFECTIVE_USER = "isDynamicEffectiveUser";
  public static final String CAN_EXEC_FIELD = "canExec";
  public static final String CAN_RUN_BATCH_FIELD = "canRunBatch";
  public static final String ENABLE_CMD_PREFIX_FIELD = "enableCmdPrefix";
  public static final String MPI_CMD_FIELD = "mpiCmd";
  public static final String JOB_RUNTIMES_FIELD = "jobRuntimes";
  public static final String JOB_RUNTIMES_VERSION_FIELD = "version";
  public static final String JOB_WORKING_DIR_FIELD = "jobWorkingDir";
  public static final String JOB_ENV_VARIABLES_FIELD = "jobEnvVariables";
  public static final String JOB_MAX_JOBS_FIELD = "jobMaxJobs";
  public static final String JOB_MAX_JOBS_PER_USER_FIELD = "jobMaxJobsPerUser";
  public static final String BATCH_SCHEDULER_FIELD = "batchScheduler";
  public static final String BATCH_LOGICAL_QUEUES_FIELD = "batchLogicalQueues";
  public static final String HPCQ_NAME_FIELD = "hpcQueueName";
  public static final String BATCH_DEFAULT_LOGICAL_QUEUE_FIELD = "batchDefaultLogicalQueue";
  public static final String BATCH_SCHEDULER_PROFILE_FIELD = "batchSchedulerProfile";
  public static final String JOB_CAPABILITIES_FIELD = "jobCapabilities";
  public static final String SHARED_WITH_USERS_FIELD = "sharedWithUsers";
  public static final String TAGS_FIELD = "tags";
  public static final String NOTES_FIELD = "notes";
  public static final String PARENT_ID_FIELD = "parentId";
  public static final String ALLOW_CHILDREN_FIELD = "allowChildren";
  public static final String IMPORT_REF_ID = "importRefId";
  public static final String UUID_FIELD = "uuid";
  public static final String CREATED_FIELD = "created";
  public static final String UPDATED_FIELD = "updated";

  // Private field names used only in this class for messages.
  private static final String NAME_FIELD = "name";
  private static final String VALUE_FIELD = "value";

  // Default values
  public static final String DEFAULT_ROOTDIR = "";
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
  // ID Must start alphanumeric and contain only alphanumeric and 4 special characters: - . _ ~
  public static final String PATTERN_STR_VALID_ID = "^[a-zA-Z0-9]([a-zA-Z0-9]|[-\\._~])*";

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
  private static final String CTL_CHR_ATTR = "SYSLIB_CTL_CHR_ATTR";

  // Label to use when name is missing.
  private static final String UNNAMED = "unnamed";


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
  private Set<String> sharedWithUsers;

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
  private String rootDir = DEFAULT_ROOTDIR; // Effective root directory
  private int port;          // Port number used to access the system
  private boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private String proxyHost;  // Name or IP address of proxy host
  private int proxyPort;     // Port number for proxy host
  private String dtnSystemId;
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
    id = LibUtils.stripStr(id1);
    systemType = systemType1;
    host = LibUtils.stripStr(host1);
    defaultAuthnMethod = defaultAuthnMethod1;
    canExec = canExec1;
  }

  /**
   * Constructor using non-updatable attributes.
   * Rather than exposing otherwise unnecessary setters we use a special constructor.
   */
  public TSystem(TSystem t, String tenant1, String id1, SystemType systemType1, boolean canExec1)
  {
    if (t==null || StringUtils.isBlank(tenant1) || StringUtils.isBlank(id1) || systemType1 == null )
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    tenant = LibUtils.stripStr(tenant1);
    id = LibUtils.stripStr(id1);
    systemType = systemType1;
    canExec = canExec1;

    seqId = t.getSeqId();
    created = t.getCreated();
    updated = t.getUpdated();
    description = t.getDescription();
    owner = LibUtils.stripStr(t.getOwner());
    host = LibUtils.stripStr(t.getHost());
    enabled = t.isEnabled();
    effectiveUserId = LibUtils.stripStr(t.getEffectiveUserId());
    defaultAuthnMethod = t.getDefaultAuthnMethod();
    authnCredential = t.getAuthnCredential();
    bucketName = LibUtils.stripStr(t.getBucketName());
    rootDir = LibUtils.stripStr(t.getRootDir());
    port = t.getPort();
    useProxy = t.isUseProxy();
    proxyHost = LibUtils.stripStr(t.getProxyHost());
    proxyPort = t.getProxyPort();
    dtnSystemId = LibUtils.stripStr(t.getDtnSystemId());
    canRunBatch = t.getCanRunBatch();
    enableCmdPrefix = t.isEnableCmdPrefix();
    mpiCmd = LibUtils.stripStr(t.getMpiCmd());
    jobRuntimes = t.getJobRuntimes();
    jobWorkingDir = LibUtils.stripStr(t.getJobWorkingDir());
    jobEnvVariables = t.getJobEnvVariables();
    jobMaxJobs = t.getJobMaxJobs();
    jobMaxJobsPerUser = t.getJobMaxJobsPerUser();
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = LibUtils.stripStr(t.getBatchDefaultLogicalQueue());
    batchSchedulerProfile = LibUtils.stripStr(t.getBatchSchedulerProfile());
    jobCapabilities = t.getJobCapabilities();
    allowChildren = t.isAllowChildren();
    parentId = LibUtils.stripStr(t.getParentId());
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
    importRefId = t.getImportRefId();
    uuid = t.getUuid();
    deleted = t.isDeleted();
    isPublic = t.isPublic();
    isDynamicEffectiveUser = t.isDynamicEffectiveUser();
    // Strip whitespace as appropriate for string attributes.
    stripWhitespace();
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
            parentSystem.getProxyPort(), parentSystem.getDtnSystemId(),
            parentSystem.getCanExec(), parentSystem.getJobRuntimes(), parentSystem.getJobWorkingDir(),
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
                 String dtnSystemId1,
                 boolean canExec1, List<JobRuntime> jobRuntimes1, String jobWorkingDir1, List<KeyValuePair> jobEnvVariables1,
                 int jobMaxJobs1, int jobMaxJobsPerUser1, boolean canRunBatch1, boolean enableCmdPrefix1, String mpiCmd1,
                 SchedulerType batchScheduler1, List<LogicalQueue> batchLogicalQueues1, String batchDefaultLogicalQueue1,
                 String batchSchedulerProfile1, List<Capability> jobCapabilities1,
                 String[] tags1, Object notes1, String importRefId1, UUID uuid1, boolean deleted1, boolean allowChildren1,
                 String parentId1, Instant created1, Instant updated1)
  {
    seqId = seqId1;
    tenant = LibUtils.stripStr(tenant1);
    id = LibUtils.stripStr(id1);
    owner = LibUtils.stripStr(owner1);
    description = description1;
    systemType = systemType1;
    host = LibUtils.stripStr(host1);
    enabled = enabled1;
    effectiveUserId = LibUtils.stripStr(effectiveUserId1);
    defaultAuthnMethod = defaultAuthnMethod1;
    bucketName = LibUtils.stripStr(bucketName1);
    rootDir = (rootDir1 == null) ? DEFAULT_ROOTDIR : LibUtils.stripStr(rootDir1);
    port = port1;
    useProxy = useProxy1;
    proxyHost = LibUtils.stripStr(proxyHost1);
    proxyPort = proxyPort1;
    dtnSystemId = LibUtils.stripStr(dtnSystemId1);
    canExec = canExec1;
    jobRuntimes = jobRuntimes1;
    jobWorkingDir = LibUtils.stripStr(jobWorkingDir1);
    jobEnvVariables = jobEnvVariables1;
    jobMaxJobs = jobMaxJobs1;
    jobMaxJobsPerUser = jobMaxJobsPerUser1;
    canRunBatch = canRunBatch1;
    enableCmdPrefix = enableCmdPrefix1;
    mpiCmd = LibUtils.stripStr(mpiCmd1);
    batchScheduler = batchScheduler1;
    batchLogicalQueues = batchLogicalQueues1;
    batchDefaultLogicalQueue = LibUtils.stripStr(batchDefaultLogicalQueue1);
    batchSchedulerProfile = LibUtils.stripStr(batchSchedulerProfile1);
    jobCapabilities = jobCapabilities1;
    tags = (tags1 == null) ? EMPTY_STR_ARRAY : tags1.clone();
    notes = notes1;
    importRefId = importRefId1;
    allowChildren = allowChildren1;
    uuid = uuid1;
    deleted = deleted1;
    created = created1;
    updated = updated1;
    parentId = LibUtils.stripStr(parentId1);
    // Strip whitespace as appropriate for string attributes.
    stripWhitespace();
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
    tenant = LibUtils.stripStr(t.getTenant());
    id = LibUtils.stripStr(t.getId());
    description = t.getDescription();
    systemType = t.getSystemType();
    owner = LibUtils.stripStr(t.getOwner());
    host = LibUtils.stripStr(t.getHost());
    enabled = t.isEnabled();
    effectiveUserId = LibUtils.stripStr(t.getEffectiveUserId());
    defaultAuthnMethod = t.getDefaultAuthnMethod();
    authnCredential = t.getAuthnCredential();
    bucketName = LibUtils.stripStr(t.getBucketName());
    rootDir = LibUtils.stripStr(t.getRootDir());
    port = t.getPort();
    useProxy = t.isUseProxy();
    proxyHost = LibUtils.stripStr(t.getProxyHost());
    proxyPort = t.getProxyPort();
    dtnSystemId = LibUtils.stripStr(t.getDtnSystemId());
    canExec = t.getCanExec();
    jobRuntimes = t.getJobRuntimes();
    jobWorkingDir = LibUtils.stripStr(t.getJobWorkingDir());
    jobEnvVariables = t.getJobEnvVariables();
    jobMaxJobs = t.getJobMaxJobs();
    jobMaxJobsPerUser = t.getJobMaxJobsPerUser();
    canRunBatch = t.getCanRunBatch();
    enableCmdPrefix = t.isEnableCmdPrefix();
    mpiCmd = LibUtils.stripStr(t.getMpiCmd());
    batchScheduler = t.getBatchScheduler();
    batchLogicalQueues = t.getBatchLogicalQueues();
    batchDefaultLogicalQueue = LibUtils.stripStr(t.getBatchDefaultLogicalQueue());
    batchSchedulerProfile = LibUtils.stripStr(t.getBatchSchedulerProfile());
    jobCapabilities = t.getJobCapabilities();
    tags = (t.getTags() == null) ? EMPTY_STR_ARRAY : t.getTags().clone();
    notes = t.getNotes();
    importRefId = t.getImportRefId();
    isPublic = t.isPublic();
    isDynamicEffectiveUser = t.isDynamicEffectiveUser();
    allowChildren = t.isAllowChildren();
    parentId = LibUtils.stripStr(t.getParentId());
    // Strip whitespace as appropriate for string attributes.
    stripWhitespace();
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
    if (canRunBatch) checkAttrCanRunBatch(errMessages);
    if (systemType == SystemType.S3) checkAttrS3(errMessages);
    checkAttrMisc(errMessages);
    checkAttrControlCharacters(errMessages);
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

    if (SystemType.LINUX.equals(systemType) || SystemType.IRODS.equals(systemType))
    {
      if (StringUtils.isBlank(rootDir))
        errMessages.add(LibUtils.getMsg("SYSLIB_NOROOTDIR1", id, systemType));
      else if (!rootDir.startsWith("/"))
        errMessages.add(LibUtils.getMsg("SYSLIB_ROOTDIR_NOSLASH", rootDir));
    }
  }

  /**
   * Check for attribute strings that exceed limits
   *   id, description, owner, effectiveUserId, bucketName, rootDir
   *   dtnSystemId, jobWorkingDir
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
   *  If type is S3 then bucketName must be set, isExec must be false.
   */
  private void checkAttrS3(List<String> errMessages)
  {
    // bucketName must be set
    if (StringUtils.isBlank(bucketName)) errMessages.add(LibUtils.getMsg("SYSLIB_S3_NOBUCKET_INPUT"));
    // canExec must be false
    if (canExec) errMessages.add(LibUtils.getMsg("SYSLIB_S3_CANEXEC_INPUT"));
  }

  /**
   * Check misc attribute restrictions
   *  If systemType is LINUX or IRODS then:
   *           - rootDir is required
   *  If systemType is IRODS then port is required.
   *  effectiveUserId is restricted.
   *  If effectiveUserId is dynamic then providing credentials is disallowed
   *  If credential is provided and contains ssh keys then validate them
   *  If canExec is false then dtnSystemId may not be set.
   *  If jobEnvVariables set then check them (see below for restrictions)
   */
  private void checkAttrMisc(List<String> errMessages)
  {
    // LINUX and IRODS systems require rootDir
    if ((SystemType.LINUX.equals(systemType) || SystemType.IRODS.equals(systemType)) && StringUtils.isBlank(rootDir))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_NOROOTDIR", systemType.name()));
    }

    // IRODS systems require port
    if (SystemType.IRODS.equals(systemType) && !isValidPort(port))
    {
      String sysTypeName = systemType == null ? "null" : systemType.name();
      errMessages.add(LibUtils.getMsg("SYSLIB_NOPORT", sysTypeName));
    }

    // For CERT authn the effectiveUserId cannot be static string other than owner
    if (AuthnMethod.CERT.equals(defaultAuthnMethod) &&
            !TSystem.APIUSERID_VAR.equals(effectiveUserId) &&
            !TSystem.OWNER_VAR.equals(effectiveUserId) &&
            !StringUtils.isBlank(owner) && !owner.equals(effectiveUserId))
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_INVALID_EFFECTIVEUSERID_INPUT"));
    }

    // If effectiveUserId is dynamic then providing credentials is disallowed
    if (TSystem.APIUSERID_VAR.equals(effectiveUserId) && authnCredential != null)
    {
      errMessages.add(LibUtils.getMsg("SYSLIB_CRED_DISALLOWED_INPUT"));
    }

    // If credential is provided and contains ssh keys then validate private key format
    if (authnCredential != null && !StringUtils.isBlank(authnCredential.getPrivateKey()))
    {
      if (!authnCredential.isValidPrivateSshKey())
        errMessages.add(LibUtils.getMsg("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY1"));
    }

    // If canExec is false then dtnSystemId may not be set.
    if (!canExec && !StringUtils.isBlank(dtnSystemId))
      errMessages.add(LibUtils.getMsg("SYSLIB_DTN_CANEXEC_FALSE", dtnSystemId));

    // Check for inputMode=FIXED and value == "!tapis_not_set"
    // Check for inputMode=REQUIRED and value != "!tapis_not_set"
    // Check for variables that begin with "_tapis". This is not allowed. Jobs will not accept them.
    if (jobEnvVariables != null)
    {
      for (KeyValuePair kv : jobEnvVariables)
      {
        // Name must not be empty
        if (StringUtils.isBlank(kv.getKey()))
        {
          errMessages.add(LibUtils.getMsg("SYSLIB_ENV_VAR_NO_NAME", kv.getValue()));
        }
        if (FIXED.equals(kv.getInputMode()) && VALUE_NOT_SET.equals(kv.getValue()))
        {
          errMessages.add(LibUtils.getMsg("SYSLIB_ENV_VAR_FIXED_UNSET", kv.getKey(), kv.getValue()));
        }
        else if (REQUIRED.equals(kv.getInputMode()) && !VALUE_NOT_SET.equals(kv.getValue()))
        {
          errMessages.add(LibUtils.getMsg("SYSLIB_ENV_VAR_REQUIRED_SET", kv.getKey(), kv.getValue()));
        }
        if (StringUtils.startsWith(kv.getKey(), RESERVED_PREFIX))
        {
          errMessages.add(LibUtils.getMsg("SYSLIB_ENV_VAR_INVALID_PREFIX", kv.getKey(), kv.getValue()));
        }
      }
    }
  }

  /**
   * Check for control characters for attributes that do not allow them.
   */
  private void checkAttrControlCharacters(List<String> errMessages)
  {
    checkForControlChars(errMessages, id, ID_FIELD);
    checkForControlChars(errMessages, owner, OWNER_FIELD);
    checkForControlChars(errMessages, tenant, TENANT_FIELD);
    checkForControlChars(errMessages, host, HOST_FIELD);
    checkForControlChars(errMessages, effectiveUserId, EFFECTIVE_USER_ID_FIELD);
    checkForControlChars(errMessages, bucketName, BUCKET_NAME_FIELD);
    checkForControlChars(errMessages, rootDir, ROOT_DIR_FIELD);
    checkForControlChars(errMessages, proxyHost, PROXY_HOST_FIELD);
    checkForControlChars(errMessages, dtnSystemId, DTN_SYSTEM_ID_FIELD);
    checkForControlChars(errMessages, mpiCmd, MPI_CMD_FIELD);
    checkForControlChars(errMessages, batchDefaultLogicalQueue, BATCH_DEFAULT_LOGICAL_QUEUE_FIELD);
    checkForControlChars(errMessages, batchSchedulerProfile, BATCH_SCHEDULER_PROFILE_FIELD);
    // NOTE Use various checkForControlChars* methods to help code readability.
    checkForControlCharsStrArray(errMessages, tags, TAGS_FIELD, null);
    checkForControlCharsEnvVariables(errMessages, jobEnvVariables);
    checkForControlCharsJobRuntimes(errMessages);
    checkForControlCharsBatchLogicalQueues(errMessages);
    checkForControlCharsJobCapabilities(errMessages);
  }

  /**
   * Check for control characters in env variables
   */
  private void checkForControlCharsEnvVariables(List<String> errMessages, List<KeyValuePair> envVariables)
  {
    if (envVariables == null || envVariables.isEmpty()) return;
    for (var envVar : envVariables)
    {
      String name = envVar.getKey(); // NOTE: Previous check ensures name is not empty.
      String value = envVar.getValue();
      checkForControlChars(errMessages, name, JOB_ENV_VARIABLES_FIELD, NAME_FIELD);
      checkForControlChars(errMessages, value, JOB_ENV_VARIABLES_FIELD, VALUE_FIELD);
    }
  }

  /**
   * Check for control characters in JobRuntime attributes
   */
  private void checkForControlCharsJobRuntimes(List<String> errMessages)
  {
    if (jobRuntimes == null || jobRuntimes.isEmpty()) return;
    for (var jobRuntime : jobRuntimes)
    {
      checkForControlChars(errMessages, jobRuntime.getVersion(), JOB_RUNTIMES_FIELD, JOB_RUNTIMES_VERSION_FIELD);
    }
  }

  /**
   * Check for control characters in JobRuntime attributes
   */
  private void checkForControlCharsBatchLogicalQueues(List<String> errMessages)
  {
    if (batchLogicalQueues == null || batchLogicalQueues.isEmpty()) return;
    for (var q : batchLogicalQueues)
    {
      checkForControlChars(errMessages, q.getName(), BATCH_LOGICAL_QUEUES_FIELD, NAME_FIELD);
      checkForControlChars(errMessages, q.getHpcQueueName(), BATCH_LOGICAL_QUEUES_FIELD, HPCQ_NAME_FIELD);
    }
  }

  /**
   * Check for control characters in JobCapabilities
   */
  private void checkForControlCharsJobCapabilities(List<String> errMessages)
  {
    if (jobCapabilities == null || jobCapabilities.isEmpty()) return;
    for (var c : jobCapabilities)
    {
      String name = StringUtils.isBlank(c.getName()) ? UNNAMED : c.getName();
      String value = c.getValue();
      checkForControlChars(errMessages, c.getName(), JOB_CAPABILITIES_FIELD, NAME_FIELD);
      // Since we may use name as a label, we need to replace any control characters in it, so we do not
      // log any strings with control characters.
      name = PathSanitizer.replaceControlChars(name, '?');
      checkForControlChars(errMessages, value, JOB_CAPABILITIES_FIELD, name, VALUE_FIELD);
    }
  }

  /*
   * Check for control characters in an array of strings.
   */
  private void checkForControlCharsStrArray(List<String> errMessages, String[] strArray, String label1, String label2)
  {
    if (strArray == null) return;
    for (String s : strArray)
    {
      checkForControlChars(errMessages, s, label1, label2);
    }
  }

  /**
   * Check for control characters in an attribute value. Use 3 part label.
   * Labels should never be null.
   */
  private void checkForControlChars(List<String> errMessages, String attrValue, String label1, String label2, String label3)
  {
    String label = String.format("%s.%s.%s", label1, label2, label3);
    checkForControlChars(errMessages, attrValue, label);
  }

  /**
   * Check for control characters in an attribute value. Use 1 or 2 part label for message.
   * label2 can be null.
   */
  private void checkForControlChars(List<String> errMessages, String attrValue, String label1, String label2)
  {
    String label = label1;
    if (!StringUtils.isBlank(label2)) label = String.format("%s.%s", label1, label2);
    checkForControlChars(errMessages, attrValue, label);
  }

  /**
   * Check for control characters in an attribute value.
   * If one is found add a message to the error list using provided label.
   */
  private void checkForControlChars(List<String> errMessages, String attrValue, String label)
  {

    try { PathSanitizer.detectControlChars(attrValue); }
    catch (TapisException e)
    {
      String logAttrValue = PathSanitizer.replaceControlChars(attrValue, '?');
      errMessages.add(LibUtils.getMsg(CTL_CHR_ATTR, label, logAttrValue, e.getMessage()));
    }
  }

  /**
   * Validate a port number. Range is 1-65535
   */
  private boolean isValidPort(int p)
  {
    return (p > 0 && p <= 65535);
  }

  /**
   * Strip whitespace as appropriate for string attributes.
   * For: tags
   * Stripping for other handled by the classes: jobRuntime, logicalQueue, capability
   */
  private void stripWhitespace()
  {
    tags = LibUtils.stripWhitespaceStrArray(tags);
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
  public TSystem setOwner(String s) { owner = LibUtils.stripStr(s);  return this;}

  public String getHost() { return host; }
  public TSystem setHost(String s) { host = LibUtils.stripStr(s); return this; }

  public boolean isEnabled() { return enabled; }
  public TSystem setEnabled(boolean b) { enabled = b;  return this; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public TSystem setEffectiveUserId(String s) { effectiveUserId = LibUtils.stripStr(s); return this; }

  public AuthnMethod getDefaultAuthnMethod() { return defaultAuthnMethod; }
  public TSystem setDefaultAuthnMethod(AuthnMethod a) { defaultAuthnMethod = a; return this; }

  public Credential getAuthnCredential() { return authnCredential; }
  public TSystem setAuthnCredential(Credential c) {authnCredential = c; return this; }

  public String getBucketName() { return bucketName; }
  public TSystem setBucketName(String s) { bucketName = LibUtils.stripStr(s); return this; }

  public String getRootDir() { return rootDir; }
  public TSystem setRootDir(String s) { rootDir = LibUtils.stripStr(s); return this; }

  public int getPort() { return port; }
  public TSystem setPort(int i) { port = i; return this; }

  public boolean isUseProxy() { return useProxy; }
  public TSystem setUseProxy(boolean b) { useProxy = b; return this; }

  public String getProxyHost() { return proxyHost; }
  public TSystem setProxyHost(String s) { proxyHost = LibUtils.stripStr(s); return this; }

  public int getProxyPort() { return proxyPort; }
  public TSystem setProxyPort(int i) { proxyPort = i; return this; }

  public String getDtnSystemId() { return dtnSystemId; }
  public TSystem setDtnSystemId(String s) { dtnSystemId = LibUtils.stripStr(s); return this; }

  public boolean getCanExec() { return canExec; }

  public boolean getCanRunBatch() { return canRunBatch; }
  public TSystem setCanRunBatch(boolean b) { canRunBatch = b; return this; }

  public String getMpiCmd() { return mpiCmd; }
  public TSystem setMpiCmd(String s) { mpiCmd = LibUtils.stripStr(s); return this; }

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
  public TSystem setJobWorkingDir(String s) { jobWorkingDir = LibUtils.stripStr(s); return this; }

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
  public TSystem setBatchDefaultLogicalQueue(String s) { batchDefaultLogicalQueue = LibUtils.stripStr(s); return this; }

  public String getBatchSchedulerProfile() { return batchSchedulerProfile; }
  public TSystem setBatchSchedulerProfile(String s) { batchSchedulerProfile = LibUtils.stripStr(s); return this; }

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
  public void setSharedWithUsers(Set<String> s) { sharedWithUsers = (s == null) ? null : new HashSet<>(s); }
  public Set<String> getSharedWithUsers() { return (sharedWithUsers == null) ? null : new HashSet<>(sharedWithUsers); }
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
