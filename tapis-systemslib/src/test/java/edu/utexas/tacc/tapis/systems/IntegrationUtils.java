package edu.utexas.tacc.tapis.systems;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.jooq.tools.StringUtils;
import org.testng.Assert;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.search.parser.ASTNode;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.KeyValuePair;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.ModuleLoadSpec;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_NOTES;
import static edu.utexas.tacc.tapis.systems.model.KeyValuePair.DEFAULT_INPUT_MODE;

/*
 * Utilities and data for integration testing
 */
public final class IntegrationUtils
{
  public static final Gson gson =  TapisGsonUtils.getGson();

  // Test data
  public static final String siteId = "tacc";
  public static final String adminTenantName = "admin";
  public static final String tenantName = "dev";
  public static final String svcName = "systems";
  public static final String filesSvcName = "files";
  public static final String jobsSvcName = "jobs";
  public static final String appsSvcName = "apps";

  // Various usernames
  // NOTE: Continue to use the fake users owner1, owner2 since some operations involve modifying credentials
  //       Although it should not be a problem because credentials are stored for each system it is best to be safe.
  public static final String adminUser = "testadmin";
  public static final String owner1 = "owner1";
  public static final String owner2 = "owner2";
  public static final String owner3 = "owner3";
  public static final String owner4 = "owner4";
  public static final String owner5 = "owner5";
  public static final String owner6 = "owner6";
  public static final String owner7 = "owner7";
  public static final String ownerNull = null;
  public static final String loginUser1 = "loginUser1";
  public static final String testUser0 = "testuser0";
  public static final String testUser1 = "testuser1";
  public static final String testUser2 = "testuser2";
  public static final String testUser3 = "testuser3";
  public static final String testUser4 = "testuser4";
  public static final String testUser5 = "testuser5";
  public static final String parentChild1 = "parentChildUser1";
  public static final String parentChild2 = "parentChildUser2";
  public static final String parentChild3 = "parentChildUser3";
  public static final List<String> childUsers = new ArrayList<>(List.of(parentChild1, parentChild2, parentChild3));
  public static final String testUser4LinuxUser = "testuser4LinuxUser";
  public static final String testUser5LinuxUser = "testuser5LinuxUser";
  public static final String impersonationIdTestUser9 = "testuser9";
  public static final String impersonationIdNull = null;
  public static final String apiUser = "testApiUser";

  // Properties for one of the Tapis v3 test VMs
  public static final String TAPIS_TEST_HOST_LOGIN_USER = testUser3;
  public static final String TAPIS_TEST_NAME_ENV_VAR = "TAPIS_VM_TESTUSER_NAME";
  public static final String TAPIS_TEST_PASSWORD_ENV_VAR = "TAPIS_VM_TESTUSER_PASSWORD";
  public static final String TAPIS_TEST_HOST_IP = "129.114.35.53";
  public static final String TAPIS_TEST_S3_KEY_ENV_VAR = "TAPIS_S3_SCBLACK_KEY";
  public static final String TAPIS_TEST_S3_SECRET_ENV_VAR = "TAPIS_S3_SCBLACK_SECRET";
  public static final String TAPIS_TEST_S3_ROOTDIR = "";
//  public static final String TAPIS_TEST_S3_HOST = "tapisdemotest2.s3.amazonaws.com";
//  public static final String TAPIS_TEST_S3_HOST_LOGIN_USER = "tapisdemo2";
//  public static final String TAPIS_TEST_S3_BUCKET = "tapisdemotest2";
  public static final String TAPIS_TEST_S3_HOST = "cics3.tacc.utexas.edu";
  public static final String TAPIS_TEST_S3_HOST_LOGIN_USER = "scblack";
  public static final String TAPIS_TEST_S3_BUCKET = "smoketest";

  public static final String sysNamePrefix = "TestSys";
  public static final String schedProfileNamePrefix = "TestSchedProfile";
  public static final String description1 = "System description 1";
  public static final String description2 = "System description 2";
  public static final String description3 = "System description 3";
  public static final String hostname2 = "my.system2.org";
  public static final String hostnameNull = null;
  public static final String effectiveUserId1 = "effUsr1";
  public static final String effectiveUserId2 = "effUsr2";
  public static final String effectiveUserIdNull = null;
  public static final Integer portNull = null;
  public static final Boolean userProxyNull = null;
  public static final String proxyHostNull = null;
  public static final Integer proxyPortNull = null;
  public static final boolean isEnabledTrue = true;
  public static final boolean canExecTrue = true;
  public static final boolean canExecFalse = false;
  public static final boolean skipCredCheckTrue = true;
  public static final boolean skipCredCheckFalse = false;
  public static final boolean getCredsTrue = true;
  public static final boolean getCredsFalse = false;
  public static final boolean requireExecPermFalse = false;
  public static final String hostPatchedId = "patched.system.org";
  public static final String hostMinimalId = "minimal.system.org";
  public static final String rootDir1 = "/root/dir1";
  public static final String rootDir2 = "/root/dir2";
  public static TSystem dtnSystem1;
  public static TSystem dtnSystem2;
  public static final String s3SystemId1 = "test-s3-system1";
  public static final String dtnSystemId1 = "test-dtn-system1";
  public static final String dtnSystemId2 = "test-dtn-system2";
  public static final String dtnSystemIdNull = null;
  public static final String dtnSystemValidHostname = "dtn.system.org";
  public static final String dtnSystemFakeHostname = "fakeDTNSystem";
  public static final String jobWorkingDir1 = "/fake/job/working_dir1";
  public static final String jobWorkingDir2 = "/fake/job/working_dir2";
  public static final String jobWorkingDirNull = null;
  public static final SchedulerType batchScheduler1 = SchedulerType.SLURM;
  public static final SchedulerType batchScheduler2 = SchedulerType.PBS;
  public static final String batchDefaultLogicalQueue1 = "lqA1";
  public static final String batchDefaultLogicalQueue2 = "lqA2";
  public static final String batchDefaultLogicalQueueNull = null;
  public static final String batchSchedulerProfile1 = "schedProfile1";
  public static final String batchSchedulerProfile2 = "schedProfile2";
  public static final String batchSchedulerProfileNull = null;
  public static final String noSuchSchedulerProfile = "noSuchSchedulerProfile";
  public static final Object notes1 = TapisGsonUtils.getGson().fromJson("{\"project\": \"my proj1\", \"testdata\": \"abc 1\"}", JsonObject.class);
  public static final Object notes2 = TapisGsonUtils.getGson().fromJson("{\"project\": \"my proj2\", \"testdata\": \"abc 2\"}", JsonObject.class);
  public static final JsonObject notesObj1 = (JsonObject) notes1;
  public static final JsonObject notesObj2 = (JsonObject) notes2;
  public static final Object notesNull = null;

  // Two keyValue pairs for checking defaults. Use default constructor to simulate behavior of jax-rs
//  public static final KeyValuePair kvDefault1 = new KeyValuePair(); TODO invalid, key cannot be empty string
  public static final KeyValuePair kvDefault2 = new KeyValuePair("CHK_DEFAULT",null, null, KeyValuePair.KeyValueInputMode.REQUIRED, null);

  public static final List<KeyValuePair> jobEnvVariables1 =
          new ArrayList<>(List.of(new KeyValuePair("A1","b1", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                                  new KeyValuePair("HOME","/home/testuser1", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                                  new KeyValuePair("TMP","/tmp1", "my keyvalue pair", DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                  new KeyValuePair("TMP1a","/tmp1a", "my keyvalue pair1a", KeyValuePair.KeyValueInputMode.FIXED, notesObj1),
                  kvDefault2));
  public static final List<KeyValuePair> jobEnvVariables2 =
          new ArrayList<>(List.of(new KeyValuePair("a2","b2", "my 2nd key-value pair", DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                                  new KeyValuePair("HOME","/home/testuser2", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                                 new KeyValuePair("TMP","/tmp2", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                  new KeyValuePair("TMP2a",null, "my keyvalue pair2a", KeyValuePair.KeyValueInputMode.REQUIRED, notesObj2)));
  public static final List<KeyValuePair> jobEnvVariables3 =
          new ArrayList<>(List.of(new KeyValuePair("a3","b3", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                  new KeyValuePair("HOME","/home/testuser3", "third one", DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                  new KeyValuePair("TMP","/tmp3",
                          "Send money. Stop. 3rd tmp kv pair with longer description just to test things out. Stop.",
                          DEFAULT_INPUT_MODE, DEFAULT_NOTES),
                  new KeyValuePair("TMP2","/tmp3a", null, DEFAULT_INPUT_MODE, DEFAULT_NOTES)));
  public static final List<KeyValuePair> jobEnvVariablesReject =
          List.of(new KeyValuePair("rejectMe", KeyValuePair.VALUE_NOT_SET, null, KeyValuePair.KeyValueInputMode.FIXED, null));
  public static final List<KeyValuePair> jobEnvVariablesNull = null;
  public static final SchedulerType batchSchedulerNull = null;
  public static final String queueNameNull = null;
  public static final boolean canRunBatchTrue = true;
  public static final boolean canRunBatchFalse = false;
  public static final Boolean canRunBatchNull = null;
  public static final boolean enableCmdPrefixTrue = true;
  public static final boolean enableCmdPrefixFalse = false;
  public static final Boolean enableCmdPrefixNull = null;
  public static final String mpiCmd1 = "mpirun1";
  public static final String mpiCmd2 = "mpirun2";
  public static final String mpiCmdNull = null;
  public static final int jobMaxJobs1 = 1;
  public static final int jobMaxJobs2 = 2;
  public static final int jobMaxJobs3 = 3;
  public static final Integer jobMaxJobsNull = null;
  public static final int jobMaxJobsPerUser1 = 1;
  public static final int jobMaxJobsPerUser2 = 2;
  public static final String tagVal1 = "value1";
  public static final String tagVal2 = "value2";
  public static final String tagVal3Space = "value 3";
  public static final String tagValNotThere = "no such tag value";
  public static final String[] tags1 = {tagVal1, tagVal2, tagVal3Space, "a",
    "Long tag (1 3 2) special chars [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  public static final String[] tags2 = {"value4", "value5"};
  public static final String[] tags3 = {tagVal1};
  public static final String[] tagsNull = null;

  public static final String importRefId1 = "https://exmpale.com/import_ref_id1";
  public static final String importRefId2 = "import_ref_id2";
  public static final String importRefIdNull = null;

  public static final Protocol prot1 = new Protocol(AuthnMethod.PKI_KEYS, 22, false, "", 0);
  public static final Protocol prot2 = new Protocol(AuthnMethod.PASSWORD, 0, true, "localhost",2222);
  public static final Protocol protS3 = new Protocol(AuthnMethod.ACCESS_KEY, 0, false, "localhost",2222);
  public static final String rawDataEmptyJson = "{}";

  // Job Runtimes
  public static final JobRuntime runtimeA1 = new JobRuntime(JobRuntime.RuntimeType.DOCKER, "0.0.1A1");
  public static final JobRuntime runtimeB1 = new JobRuntime(JobRuntime.RuntimeType.SINGULARITY, "0.0.1B1");
  public static final List<JobRuntime> jobRuntimes1 = new ArrayList<>(List.of(runtimeA1, runtimeB1));
  public static final JobRuntime runtimeA2 = new JobRuntime(JobRuntime.RuntimeType.SINGULARITY, "0.0.1A2");
  public static final JobRuntime runtimeB2 = new JobRuntime(JobRuntime.RuntimeType.DOCKER, "0.0.1B2");
  public static final List<JobRuntime> jobRuntimes2 = new ArrayList<>(List.of(runtimeA2, runtimeB2));
  public static final List<JobRuntime> jobRuntimesNull= null;

  // Logical Queues
  public static final LogicalQueue queueA1 = new LogicalQueue("lqA1","hqA1", 1, 1, 0, 1, 0, 1, 0, 1, 0, 1);
  public static final LogicalQueue queueB1 = new LogicalQueue("lqB1","hqB1", 2, 2, 0, 2, 0, 2, 0, 2, 0, 2);
  public static final LogicalQueue queueC1 = new LogicalQueue("lqC1","hqC1", 3, 3, 0, 3, 0, 3, 0, 3, 0, 3);
  public static final List<LogicalQueue> logicalQueueList1 = new ArrayList<>(List.of(queueA1, queueB1, queueC1));
  public static final LogicalQueue queueA2 = new LogicalQueue("lqA2","hqA2", 10, 10, 0, 10, 0, 10, 0, 10, 0, 10);
  public static final LogicalQueue queueB2 = new LogicalQueue("lqB2","hqB1", 20, 20, 0, 20, 0, 20, 0, 20, 0, 20);
  public static final List<LogicalQueue> logicalQueueList2 = new ArrayList<>(List.of(queueA2, queueB2));
  public static final List<LogicalQueue> logicalQueueListNull = null;

  // Job Capabilities
  public static final Capability capA1 = new Capability(Capability.Category.SCHEDULER, "Type",
                                                       Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "Slurm");
  public static final Capability capB1 = new Capability(Capability.Category.HARDWARE, "CoresPerNode",
                                                       Capability.Datatype.INTEGER, Capability.DEFAULT_PRECEDENCE, "4");
  public static final Capability capC1 = new Capability(Capability.Category.SOFTWARE, "OpenMP",
                                                       Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "4.5");
  public static final List<Capability> capList1 = new ArrayList<>(List.of(capA1, capB1, capC1));

  public static final Capability capA2 = new Capability(Capability.Category.SCHEDULER, "Type",
          Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "PBS");
  public static final Capability capB2 = new Capability(Capability.Category.HARDWARE, "CoresPerNode",
          Capability.Datatype.INTEGER, Capability.DEFAULT_PRECEDENCE, "8");
  public static final Capability capC2 = new Capability(Capability.Category.SOFTWARE, "MPI",
          Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "3.1");
  public static final Capability capD2 = new Capability(Capability.Category.CONTAINER, "Docker",
          Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, null);
  public static final List<Capability> capList2 = new ArrayList<>(List.of(capA2, capB2, capC2, capD2));

  public static final Capability capA3 = new Capability(Capability.Category.SCHEDULER, "Type",
          Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "Condor");
  public static final Capability capB3 = new Capability(Capability.Category.HARDWARE, "CoresPerNode",
          Capability.Datatype.INTEGER, Capability.DEFAULT_PRECEDENCE, "128");
  public static final Capability capC3 = new Capability(Capability.Category.SOFTWARE, "OpenMP",
          Capability.Datatype.STRING, Capability.DEFAULT_PRECEDENCE, "3.1");
  public static final List<Capability> capList3 = new ArrayList<>(List.of(capA3, capB3, capC3));
  public static final List<Capability> capListNull = null;

  public static final UUID uuidNull = null;
  public static final boolean isDeletedFalse = false;
  public static final boolean showDeletedFalse = false;
  public static final boolean showDeletedTrue = true;
  public static final boolean resolveEffUserTrue = true;
  public static final boolean resolveEffUserFalse = false;
  public static final boolean fetchShareInfoFalse = false;
  public static final boolean fetchShareInfoTrue = true;
  public static final String sharedCtxOwner = owner1;
  public static final String sharedCtxNull = null;
  public static final String resourceTenantNull = null;
  public static final Boolean allowChildrenFalse = Boolean.FALSE;
  public static final Boolean allowChildrenTrue = Boolean.TRUE;
  public static final String parentIdNull = null;
  public static final Instant createdNull = null;
  public static final Instant updatedNull = null;
  public static final int qMaxJobs = -1;
  public static final int qMaxJobsPerUser = -1;
  public static final int qMaxNodeCount = -1;
  public static final int qMaxCoresPerNode = -1;
  public static final int qMaxMemoryMB = -1;
  public static final int qMaxMinutes = -1;

  public static final String listTypeNull = null;

  public static final List<OrderBy> orderByListNull = null;
  public static final List<OrderBy> orderByListAsc = Collections.singletonList(OrderBy.fromString("id(asc)"));
  public static final List<OrderBy> orderByListDesc = Collections.singletonList(OrderBy.fromString("id(desc)"));
  public static final List<OrderBy> orderByList2Asc = new ArrayList<>(List.of(OrderBy.fromString("system_type(asc)"),
                                                                              OrderBy.fromString("bucket_name(asc)")));
  public static final List<OrderBy> orderByList2Desc = new ArrayList<>(List.of(OrderBy.fromString("system_type(asc)"),
                                                                       OrderBy.fromString("bucket_name(desc)")));
  public static final List<OrderBy> orderByList3Asc = new ArrayList<>(List.of(OrderBy.fromString("id(asc)"),
                                                                              OrderBy.fromString("owner(asc)")));
  public static final List<OrderBy> orderByList3Desc = new ArrayList<>(List.of(OrderBy.fromString("bucket_name(desc)"),
                                                                               OrderBy.fromString("system_type(desc)")));
  public static final String startAfterNull = null;

  public static final String invalidPrivateSshKey = "-----BEGIN OPENSSH PRIVATE KEY-----";
  public static final String invalidPublicSshKey = "testPubSshKey";

  public static final Credential credInvalidPrivateSshKey =
          new Credential(null, null, null, invalidPrivateSshKey, invalidPublicSshKey, null, null, null, null, null);
  public static final Credential credNoLoginUser =
          new Credential(null, null, "fakePassword", null, null, null, null, null, null, null);

  // Permissions
  public static final Set<TSystem.Permission> testPermsREADMODIFY = new HashSet<>(Set.of(TSystem.Permission.READ, TSystem.Permission.MODIFY));
  public static final Set<TSystem.Permission> testPermsREADEXECUTE = new HashSet<>(Set.of(TSystem.Permission.READ, TSystem.Permission.EXECUTE));
  public static final Set<TSystem.Permission> testPermsREAD = new HashSet<>(Set.of(TSystem.Permission.READ));
  public static final Set<TSystem.Permission> testPermsMODIFY = new HashSet<>(Set.of(TSystem.Permission.MODIFY));

  // Search and sort
  public static final List<String> searchListNull = null;
  public static final ASTNode searchASTNull = null;
  public static final Set<String> setOfIDsNull = null;
  public static final int limitNone = -1;
  public static final List<String> orderByAttrEmptyList = Arrays.asList("");
  public static final List<String> orderByDirEmptyList = Arrays.asList("");
  public static final int skipZero = 0;
  public static final String startAferEmpty = "";
  public static final SystemsServiceImpl.AuthListType listTypeOwned = SystemsServiceImpl.AuthListType.OWNED;
  public static final SystemsServiceImpl.AuthListType listTypeAll = SystemsServiceImpl.AuthListType.ALL;
  public static final SystemsServiceImpl.AuthListType listTypePublic = SystemsServiceImpl.AuthListType.SHARED_PUBLIC;

  public static final String stringWithCtrlChar = "Start\u0001Finish"; // String containing a control-A character
  public static final String stringWithNewlineAtEnd = "StartFinish\n"; // String containing a newline at end
  public static final String[] tagsWithCtrlChar = {"Start\u0001Finish", "tag 2"};

  /**
   * Create first DTN System
   * Have a separate method for each because each test suite needs one with a unique name to avoid
   *   concurrency issues when tests are run in parallel through maven.
   */
  public static TSystem makeDtnSystem1(String key)
  {
    // Start ID with a number to validate the update that allows for IDs starting with a number.
    String dtnSystemName1 = "1" +sysNamePrefix+key+dtnSystemId1;
    String dtnRootDir = "/root"+key+"_001"; // testCreateSystem

    // Create DTN systems for other systems to reference. Otherwise, some system definitions are not valid.
    return new TSystem(-1, tenantName, dtnSystemName1 , "DTN System1 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn1", prot1.getAuthnMethod(), "bucketDtn1", dtnRootDir,
            prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
            dtnSystemIdNull,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchFalse, enableCmdPrefixFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull,
            batchSchedulerProfileNull, capListNull, tags1, notes1, importRefId1, uuidNull, isDeletedFalse,
            allowChildrenFalse, parentIdNull, createdNull, updatedNull);
  }

  /**
   * Create second DTN System
   * Have a separate method for each because each test suite needs one with a unique name to avoid
   *   concurrency issues when tests are run in parallel through maven.
   */
  public static TSystem makeDtnSystem2(String key)
  {
    String dtnSystemName2 = sysNamePrefix+key+dtnSystemId2;
    String dtnRootDir = "/root"+key+"_002"; // testGetSystem

    // Create DTN systems for other systems to reference. Otherwise, some system definitions are not valid.
    return new TSystem(-1, tenantName, dtnSystemName2, "DTN System2 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn2", prot2.getAuthnMethod(), "bucketDtn2", dtnRootDir,
            prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemIdNull,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs2, jobMaxJobsPerUser2,
            canRunBatchFalse, enableCmdPrefixFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull,
            batchSchedulerProfileNull, capListNull, tags2, notes2, importRefId2, uuidNull, isDeletedFalse,
            allowChildrenFalse, parentIdNull, createdNull, updatedNull);
  }

  /**
   * Create an array of TSystem objects in memory
   * NOTE: If any DTN systems used, they must be created first and the rootDir values must match.
   * Names will be of format TestSys_K_NNN where K is the key and NNN runs from 000 to 999
   * We need a key because maven runs the tests in parallel so each set of systems created by an integration
   *   test will need its own namespace.
   * @param n number of systems to create
   * @return array of TSystem objects
   */
  public static TSystem[] makeSystems(int n, String key)
  {
    TSystem[] systems = new TSystem[n];
    String dtnSystemName1 = "1" + sysNamePrefix+key+dtnSystemId1;
    String dtnSystemName2 = sysNamePrefix+key+dtnSystemId2;

    // Create DTN systems for other systems to reference. Otherwise, some system definitions are not valid.
    dtnSystem1 = IntegrationUtils.makeDtnSystem1(key);
    dtnSystem2 = IntegrationUtils.makeDtnSystem2(key);

    for (int i = 0; i < n; i++)
    {
      // Suffix which should be unique for each system within each integration test
      String iStr = String.format("%03d", i+1);
      String suffix = key + "_" + iStr;
      String name = getSysName(key, i+1);
      String hostName = "host" + key + iStr + ".test.org";
      // Set dtnSystemId to not be null for a couple of cases (getSys, createSys), so we check dtnSystemId validation.
      // All others have dtnSystemId = null, so we do not have to worry about rootDir values not matching.
      String dtnSystemId = null;
      if (i == 0) dtnSystemId = dtnSystem1.getId(); // testCreateSystem
      if (i == 1) dtnSystemId = dtnSystem2.getId(); // testGetSystem
      // Constructor initializes all attributes except for JobCapabilities and Credential
      systems[i] = new TSystem(-1, tenantName, name, description1+suffix, TSystem.SystemType.LINUX, owner1,
              hostName, isEnabledTrue,effectiveUserId1+suffix, prot1.getAuthnMethod(), "bucket"+suffix, "/root"+suffix,
              prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
              dtnSystemId,
              canExecTrue, jobRuntimes1, "jobWorkDir"+suffix, jobEnvVariables1, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchTrue, enableCmdPrefixTrue, mpiCmd1, batchScheduler1, logicalQueueList1, queueA1.getName(),
              batchSchedulerProfile1, capList1, tags1, notes1, importRefId1, uuidNull, isDeletedFalse,
              allowChildrenFalse, parentIdNull, createdNull, updatedNull);
      systems[i].setJobRuntimes(jobRuntimes1);
      systems[i].setBatchLogicalQueues(logicalQueueList1);
      systems[i].setJobCapabilities(capList1);
    }
    return systems;
  }

  /**
   * Create a TSystem in memory with minimal attributes set based on TSystem given
   *   id, systemType, host, defaultAuthnMethod, canExec
   *   and since sytemType is LINUX must also set rootDir
   * If id is passed in then use it instead of id from tSys
   * NOTE: many args to constructor are primitives so cannot be set to null.
   */
  public static TSystem makeMinimalSystem(TSystem tSys, String id)
  {
    if (!StringUtils.isBlank(id))
    {
      return new TSystem(-1, tenantName, id, null, tSys.getSystemType(), ownerNull,
              hostMinimalId, isEnabledTrue, effectiveUserIdNull, tSys.getDefaultAuthnMethod(), null, rootDir1,
              prot1.getPort(), prot1.isUseProxy(), null, prot1.getProxyPort(), null,
              canExecFalse, jobRuntimesNull, null, null, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchFalse, enableCmdPrefixFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull,
              batchDefaultLogicalQueueNull, batchSchedulerProfileNull, capListNull, tagsNull, notesNull,
              importRefIdNull, uuidNull, isDeletedFalse,
              allowChildrenFalse, parentIdNull, createdNull, updatedNull);
    }
    else
    {
      return new TSystem(-1, tenantName, tSys.getId(), null, tSys.getSystemType(), ownerNull,
              hostMinimalId, isEnabledTrue, effectiveUserIdNull, tSys.getDefaultAuthnMethod(), null, rootDir1,
              prot1.getPort(), prot1.isUseProxy(), null, prot1.getProxyPort(), null,
              canExecFalse, jobRuntimesNull, null, null, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchFalse, enableCmdPrefixFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull,
              batchDefaultLogicalQueueNull, batchSchedulerProfileNull, capListNull,
              tagsNull, notesNull, importRefIdNull, uuidNull, isDeletedFalse, allowChildrenFalse, parentIdNull,
              createdNull, updatedNull);
    }
  }

  /**  public static final Protocol prot1 = new Protocol(AuthnMethod.PKI_KEYS, 22, false, "", 0);
   public static final Protocol prot2 = new Protocol(AuthnMethod.PASSWORD, 0, true, "localhost",2222);

   * Create a TSystem in memory for use in testing the PUT operation.
   * All updatable attributes are modified.
   */
  public static TSystem makePutSystemFull(String key, TSystem system)
  {
    TSystem putSys = new TSystem(system.getSeqId(), tenantName, system.getId(), description2, system.getSystemType(),
                       system.getOwner(), hostname2, system.isEnabled(), system.getEffectiveUserId(),
                       prot2.getAuthnMethod(), system.getBucketName(), system.getRootDir(), prot2.getPort(),
                       prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
                       dtnSystemIdNull,
                       system.getCanExec(), jobRuntimes2, jobWorkingDir2, jobEnvVariables2, jobMaxJobs2,
                       jobMaxJobsPerUser2, canRunBatchTrue, enableCmdPrefixTrue, mpiCmd2, batchScheduler2,
                       logicalQueueList2, batchDefaultLogicalQueue2, batchSchedulerProfile2,
                       capList2, tags2, notes2, importRefId2, null, false,
                       allowChildrenFalse, parentIdNull, null, null);
    putSys.setBatchLogicalQueues(logicalQueueList2);
    putSys.setJobRuntimes(jobRuntimes2);
    putSys.setJobCapabilities(capList2);
    return putSys;
  }

  /**  public static final Protocol prot1 = new Protocol(AuthnMethod.PKI_KEYS, 22, false, "", 0);
  public static final Protocol prot2 = new Protocol(AuthnMethod.PASSWORD, 0, true, "localhost",2222);

   * Create a PatchSystem in memory for use in testing.
   * All attributes are to be updated, except dtnSystemId
   */
  public static PatchSystem makePatchSystemFull(String key, String systemId)
  {
    return new PatchSystem(description2, hostname2, effectiveUserId2,
            prot2.getAuthnMethod(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemIdNull, jobRuntimes2, jobWorkingDir2,
            jobEnvVariables2, jobMaxJobs2, jobMaxJobsPerUser2, canRunBatchTrue, enableCmdPrefixTrue, mpiCmd2, batchScheduler2,
            logicalQueueList2, batchDefaultLogicalQueue2, batchSchedulerProfile2, capList2, tags2, notes2, importRefId2, allowChildrenFalse);
  }

  /**
   * Create a PatchSystem in memory for use in testing.
   * Some attributes are to be updated: description, authnMethod, runtimeList, jobMaxJobsPerUser
   */
  public static PatchSystem makePatchSystemPartial(String key, String systemId)
  {
    return new PatchSystem(description2, hostnameNull, effectiveUserIdNull,
            prot2.getAuthnMethod(), portNull, userProxyNull, proxyHostNull, proxyPortNull,
            dtnSystemIdNull, jobRuntimes2, jobWorkingDirNull, jobEnvVariablesNull,
            jobMaxJobsNull, jobMaxJobsPerUser2, canRunBatchNull, enableCmdPrefixTrue, mpiCmd2, batchSchedulerNull, logicalQueueListNull,
            batchDefaultLogicalQueueNull, batchSchedulerProfileNull, capListNull, tagsNull, notesNull, importRefIdNull, allowChildrenFalse);
  }

  /**
   * Create an S3 system
   */
  public static TSystem makeS3System(String key)
  {
    String s3SystemName1 = sysNamePrefix+key+s3SystemId1;

    return new TSystem(-1, tenantName, s3SystemName1 , "S3 System1 for tests", TSystem.SystemType.S3, owner1,
            TAPIS_TEST_S3_HOST, isEnabledTrue, TAPIS_TEST_S3_HOST_LOGIN_USER, protS3.getAuthnMethod(),
            TAPIS_TEST_S3_BUCKET, TAPIS_TEST_S3_ROOTDIR,
            protS3.getPort(), protS3.isUseProxy(), protS3.getProxyHost(), protS3.getProxyPort(),
            dtnSystemIdNull,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchFalse, enableCmdPrefixFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull, batchSchedulerProfileNull,
            capListNull, tags1, notes1, importRefId1, uuidNull, isDeletedFalse,
            allowChildrenFalse, parentIdNull, createdNull, updatedNull);
  }

  /**
   * Create an array of SchedulerProfile objects in memory
   * Names will be of format TestSchedProfile_K_NNN where K is the key and NNN runs from 000 to 999
   * We need a key because maven runs the tests in parallel so each set of profiles created by an integration
   *   test will need its own namespace.
   * @param n number of objects to create
   * @return array of objects created
   */
  public static SchedulerProfile[] makeSchedulerProfiles(int n, String key)
  {
    SchedulerProfile[] schedulerProfiles = new SchedulerProfile[n];
    List<SchedulerProfile.HiddenOption> hiddenOptions = new ArrayList<>(List.of(SchedulerProfile.HiddenOption.MEM));
    for (int i = 0; i < n; i++)
    {
      // Suffix which should be unique for each profile within each integration test
      String iStr = String.format("%03d", i+1);
      String suffix = key + "_" + iStr;
      String name = getSchedulerProfileName(key, i+1);
      String moduleLoadCmd = "module load" + suffix;
      String[] modulesToLoad = {"value1" + suffix, "value2" + suffix};
      List<ModuleLoadSpec> moduleLoads = List.of(new ModuleLoadSpec(moduleLoadCmd, modulesToLoad));
      schedulerProfiles[i] = new SchedulerProfile(tenantName, name, "Test profile" + suffix, testUser2,
                                                  moduleLoads, hiddenOptions, null, null, null);
    }
    return schedulerProfiles;
  }

  public static String getBucketName(String key, int idx)
  {
    String suffix = key + "_" + String.format("%03d", idx);
    return "bucket" + suffix;
  }

  public static String getSysName(String key, int idx)
  {
    String suffix = key + "_" + String.format("%03d", idx);
    return sysNamePrefix + "_" + suffix;
  }

  public static String getSchedulerProfileName(String key, int idx)
  {
    String suffix = key + "_" + String.format("%03d", idx);
    return schedProfileNamePrefix + "_" + suffix;
  }

  /**
   * Check that KeyValuePair value was set to !tapis_not_set when inputMode was REQUIRED and no value was given.
   * NOTE: This only tests correct behavior of KeyValuePair constructors. Still not a direct test of going through
   *       jax-rs framework
   */
  public static void checkEnvVarDefaults(TSystem sys)
  {
    var jobEnvVars = sys.getJobEnvVariables();
    Assert.assertNotNull(jobEnvVars, "JobEnvVars is null");
    Assert.assertFalse(jobEnvVars.isEmpty(), "JobEnvVars is empty");
    // Look for env var named CHECK
    for (KeyValuePair kv : jobEnvVars)
    {
      if ("CHK_DEFAULT".equals(kv.getKey()))
        Assert.assertEquals(kv.getValue(), "!tapis_not_set");
    }
  }

  // Verify that original list of KeyValuePairs matches the fetched list
  public static void verifyKeyValuePairs(List<KeyValuePair> origKVs, List<KeyValuePair> fetchedKVs)
  {
    Assert.assertNotNull(origKVs, "Orig KVs is null");
    Assert.assertNotNull(fetchedKVs, "Fetched KVs is null");
    Assert.assertEquals(fetchedKVs.size(), origKVs.size());
    // Create hash maps of orig and fetched with KV key as key
    var origMap = new HashMap<String, KeyValuePair>();
    var fetchedMap = new HashMap<String, KeyValuePair>();
    for (KeyValuePair kv : origKVs) origMap.put(kv.getKey(), kv);
    for (KeyValuePair kv : fetchedKVs) fetchedMap.put(kv.getKey(), kv);
    // Go through origMap and check properties
    for (String kvKey : origMap.keySet())
    {
      Assert.assertTrue(fetchedMap.containsKey(kvKey), "Fetched list does not contain original item: " + kvKey);
      KeyValuePair fetchedKV = fetchedMap.get(kvKey);
      System.out.println("Found fetched KeyValuePair: " + fetchedKV);
      Assert.assertEquals(fetchedMap.get(kvKey).toString(), origMap.get(kvKey).toString());
    }
  }
}
