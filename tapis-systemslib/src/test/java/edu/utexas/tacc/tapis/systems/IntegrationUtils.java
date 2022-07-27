package edu.utexas.tacc.tapis.systems;

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
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SchedulerType;
import org.jooq.tools.StringUtils;
import org.testng.Assert;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
  public static final String adminUser = "testadmin";

  // NOTE: Continue to use the fake users owner1, owner2 since some operations involve modifying credentials
  //       Although it should not be a problem because credentials are stored for each system it is best to be safe.
  public static final String owner1 = "owner1";
  public static final String owner2 = "owner2";
  public static final String ownerNull = null;
  public static final String loginUser1 = "loginUser1";
  public static final String testUser0 = "testuser0";
  public static final String testUser1 = "testuser1";
  public static final String testUser2 = "testuser2";
  public static final String testUser3 = "testuser3";
  public static final String testUser4 = "testuser4";
  public static final String testUser5 = "testuser5";
  public static final String testUser4LinuxUser = "testuser4LinuxUser";
  public static final String testUser5LinuxUser = "testuser5LinuxUser";
  public static final String impersonationIdTestUser9 = "testuser9";
  public static final String impersonationIdNull = null;
  public static final String apiUser = "testApiUser";
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
  public static final boolean isDtnTrue = true;
  public static final boolean isDtnFalse = false;
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
  public static final String dtnSystemId1 = "test-dtn-system1";
  public static final String dtnSystemId2 = "test-dtn-system2";
  public static final String dtnSystemIdNull = null;
  public static final String dtnSystemValidHostname = "dtn.system.org";
  public static final String dtnSystemFakeHostname = "fakeDTNSystem";
  public static final String dtnMountPoint1 = "/fake/mountpoint1";
  public static final String dtnMountPoint2 = "/fake/mountpoint2";
  public static final String dtnMountPointNull = null;
  public static final String dtnMountSourcePath1 = "/fake/mountsourcepath1";
  public static final String dtnMountSourcePath2 = "/fake/mountsourcepath2";
  public static final String dtnMountSourcePathNull = null;
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
  public static final KeyValuePair kv1 = new KeyValuePair("a","b");
  public static final KeyValuePair kv2 = new KeyValuePair("HOME","/home/testuser2");
  public static final KeyValuePair kv3 = new KeyValuePair("TMP","/tmp");
  public static final List<KeyValuePair> jobEnvVariables1 =
          new ArrayList<>(List.of(new KeyValuePair("a1","b1"),
                                  new KeyValuePair("HOME","/home/testuser1"),
                                  new KeyValuePair("TMP","/tmp1")));
  public static final List<KeyValuePair> jobEnvVariables2 =
          new ArrayList<>(List.of(new KeyValuePair("a2","b2"),
                                  new KeyValuePair("HOME","/home/testuser2"),
                                 new KeyValuePair("TMP","/tmp2")));
  public static final List<KeyValuePair> jobEnvVariables3 =
          new ArrayList<>(List.of(new KeyValuePair("a3","b3"),
                  new KeyValuePair("HOME","/home/testuser3"),
                  new KeyValuePair("TMP","/tmp3"),
                  new KeyValuePair("TMP2","/tmp3a")));
  public static final List<KeyValuePair> jobEnvVariablesNull = null;
//  public static final String[] jobEnvVariables1 = {"a1=b1", "HOME=/home/testuser1", "TMP=/tmp1"};
//  public static final String[] jobEnvVariables2 = {"a2=b2", "HOME=/home/testuser2", "TMP=/tmp2"};
//  public static final String[] jobEnvVariablesNull = null;
  public static final SchedulerType batchSchedulerNull = null;
  public static final String queueNameNull = null;
  public static final boolean canRunBatchTrue = true;
  public static final boolean canRunBatchFalse = false;
  public static final Boolean canRunBatchNull = null;
  public static final String mpiCmd1 = "mpirun1";
  public static final String mpiCmd2 = "mpirun2";
  public static final String mpiCmdNull = null;
  public static final int jobMaxJobs1 = 1;
  public static final int jobMaxJobs2 = 2;
  public static final int jobMaxJobs3 = 3;
  public static final Integer jobMaxJobsNull = null;
  public static final int jobMaxJobsPerUser1 = 1;
  public static final int jobMaxJobsPerUser2 = 2;
  public static final String[] tags1 = {"value1", "value2", "a",
    "Long tag (1 3 2) special chars [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  public static final String[] tags2 = {"value3", "value4"};
  public static final String[] tags3 = {"value1"};
  public static final String[] tagsNull = null;
  public static final Object notes1 = TapisGsonUtils.getGson().fromJson("{\"project\": \"myproj1\", \"testdata\": \"abc1\"}", JsonObject.class);
  public static final Object notes2 = TapisGsonUtils.getGson().fromJson("{\"project\": \"myproj2\", \"testdata\": \"abc2\"}", JsonObject.class);
  public static final JsonObject notesObj1 = (JsonObject) notes1;
  public static final Object notesNull = null;

  public static final String importRefId1 = "https://exmpale.com/import_ref_id1";
  public static final String importRefId2 = "import_ref_id2";
  public static final String importRefIdNull = null;

  public static final Protocol prot1 = new Protocol(AuthnMethod.PKI_KEYS, 22, false, "", 0);
  public static final Protocol prot2 = new Protocol(AuthnMethod.PASSWORD, 0, true, "localhost",2222);
  public static final String rawDataEmtpyJson = "{}";

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
  public static final boolean sharedAppCtxFalse = false;
  public static final Instant createdNull = null;
  public static final Instant updatedNull = null;
  public static final int qMaxJobs = -1;
  public static final int qMaxJobsPerUser = -1;
  public static final int qMaxNodeCount = -1;
  public static final int qMaxCoresPerNode = -1;
  public static final int qMaxMemoryMB = -1;
  public static final int qMaxMinutes = -1;

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
          new Credential(null, null, null, invalidPrivateSshKey, invalidPublicSshKey, null, null, null);
  public static final Credential credNoLoginUser =
          new Credential(null, null, "fakePassword", null, null, null, null, null);

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

  /**
   * Create first DTN System
   * Have a separate method for each because each test suite needs one with a unique name to avoid
   *   concurrency issues when tests are run in parallel through maven.
   */
  public static TSystem makeDtnSystem1(String key)
  {
    String dtnSystemName1 = sysNamePrefix+key+dtnSystemId1;

    // Create DTN systems for other systems to reference. Otherwise some system definitions are not valid.
    return new TSystem(-1, tenantName, dtnSystemName1 , "DTN System1 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn1", prot1.getAuthnMethod(), "bucketDtn1", "/root/dtn1",
            prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
            dtnSystemIdNull, dtnMountPointNull, dtnMountSourcePathNull, isDtnTrue,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull, batchSchedulerProfileNull,
            capListNull, tags1, notes1, importRefId1, uuidNull, isDeletedFalse, createdNull, updatedNull);
  }

  /**
   * Create second DTN System
   * Have a separate method for each because each test suite needs one with a unique name to avoid
   *   concurrency issues when tests are run in parallel through maven.
   */
  public static TSystem makeDtnSystem2(String key)
  {
    String dtnSystemName2 = sysNamePrefix+key+dtnSystemId2;

    // Create DTN systems for other systems to reference. Otherwise some system definitions are not valid.
    return new TSystem(-1, tenantName, dtnSystemName2, "DTN System2 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn2", prot2.getAuthnMethod(), "bucketDtn2", "/root/dtn2",
            prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemIdNull, dtnMountPointNull, dtnMountSourcePathNull, isDtnTrue,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs2, jobMaxJobsPerUser2,
            canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull, batchSchedulerProfileNull,
            capListNull, tags2, notes2, importRefId2, uuidNull, isDeletedFalse, createdNull, updatedNull);
  }

  /**
   * Create an array of TSystem objects in memory
   * NOTE: DTN systems must be created first.
   * Names will be of format TestSys_K_NNN where K is the key and NNN runs from 000 to 999
   * We need a key because maven runs the tests in parallel so each set of systems created by an integration
   *   test will need its own namespace.
   * @param n number of systems to create
   * @return array of TSystem objects
   */
  public static TSystem[] makeSystems(int n, String key)
  {
    TSystem[] systems = new TSystem[n];
    String dtnSystemName1 = sysNamePrefix+key+dtnSystemId1;
    String dtnSystemName2 = sysNamePrefix+key+dtnSystemId2;

    // Create DTN systems for other systems to reference. Otherwise some system definitions are not valid.
    dtnSystem1 = new TSystem(-1, tenantName, dtnSystemName1 , "DTN System1 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn1", prot1.getAuthnMethod(), "bucketDtn1", "/root/dtn1",
            prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
            dtnSystemIdNull, dtnMountPointNull, dtnMountSourcePathNull, isDtnTrue,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull, batchSchedulerProfileNull,
            capListNull, tags1, notes1, importRefId1, uuidNull, isDeletedFalse, createdNull, updatedNull);
    dtnSystem2 = new TSystem(-1, tenantName, dtnSystemName2, "DTN System2 for tests", TSystem.SystemType.LINUX, owner1,
            dtnSystemValidHostname, isEnabledTrue,"effUserDtn2", prot2.getAuthnMethod(), "bucketDtn2", "/root/dtn2",
            prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemIdNull, dtnMountPointNull, dtnMountSourcePathNull, isDtnTrue,
            canExecFalse, jobRuntimesNull, jobWorkingDirNull, jobEnvVariablesNull, jobMaxJobs2, jobMaxJobsPerUser2,
            canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, queueNameNull, batchSchedulerProfileNull,
            capListNull, tags2, notes2, importRefId2, uuidNull, isDeletedFalse, createdNull, updatedNull);
    for (int i = 0; i < n; i++)
    {
      // Suffix which should be unique for each system within each integration test
      String iStr = String.format("%03d", i+1);
      String suffix = key + "_" + iStr;
      String name = getSysName(key, i+1);
      String hostName = "host" + key + iStr + ".test.org";
      // Constructor initializes all attributes except for JobCapabilities and Credential
      systems[i] = new TSystem(-1, tenantName, name, description1+suffix, TSystem.SystemType.LINUX, owner1,
              hostName, isEnabledTrue,effectiveUserId1+suffix, prot1.getAuthnMethod(), "bucket"+suffix, "/root"+suffix,
              prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
              dtnSystem1.getId(), dtnMountPoint1, dtnMountSourcePath1, isDtnFalse,
              canExecTrue, jobRuntimes1, "jobWorkDir"+suffix, jobEnvVariables1, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchTrue, mpiCmd1, batchScheduler1, logicalQueueList1, queueA1.getName(), batchSchedulerProfile1,
              capList1, tags1, notes1, importRefId1, uuidNull, isDeletedFalse, createdNull, updatedNull);
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
              prot1.getPort(), prot1.isUseProxy(), null, prot1.getProxyPort(), null, null, null, isDtnFalse,
              canExecFalse, jobRuntimesNull, null, null, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, batchDefaultLogicalQueueNull,
              batchSchedulerProfileNull, capListNull,
              tagsNull, notesNull, importRefIdNull, uuidNull, isDeletedFalse, createdNull, updatedNull);
    }
    else
    {
      return new TSystem(-1, tenantName, tSys.getId(), null, tSys.getSystemType(), ownerNull,
              hostMinimalId, isEnabledTrue, effectiveUserIdNull, tSys.getDefaultAuthnMethod(), null, rootDir1,
              prot1.getPort(), prot1.isUseProxy(), null, prot1.getProxyPort(), null, null, null, isDtnFalse,
              canExecFalse, jobRuntimesNull, null, null, jobMaxJobs1, jobMaxJobsPerUser1,
              canRunBatchFalse, mpiCmdNull, batchSchedulerNull, logicalQueueListNull, batchDefaultLogicalQueueNull,
              batchSchedulerProfileNull, capListNull,
              tagsNull, notesNull, importRefIdNull, uuidNull, isDeletedFalse, null, null);
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
                       system.getOwner(), hostname2, system.isEnabled(), effectiveUserId2,
                       prot2.getAuthnMethod(), system.getBucketName(), system.getRootDir(), prot2.getPort(),
                       prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
                       sysNamePrefix+key+dtnSystemId2, dtnMountPoint2, dtnMountSourcePath2, system.isDtn(),
                       system.getCanExec(), jobRuntimes2, jobWorkingDir2, jobEnvVariables2, jobMaxJobs2,
                       jobMaxJobsPerUser2, canRunBatchTrue, mpiCmd2, batchScheduler2, logicalQueueList2,
                       batchDefaultLogicalQueue2, batchSchedulerProfile2,
                       capList2, tags2, notes2, importRefId2, null, false, null, null);
    putSys.setBatchLogicalQueues(logicalQueueList2);
    putSys.setJobRuntimes(jobRuntimes2);
    putSys.setJobCapabilities(capList2);
    return putSys;
  }

  /**  public static final Protocol prot1 = new Protocol(AuthnMethod.PKI_KEYS, 22, false, "", 0);
  public static final Protocol prot2 = new Protocol(AuthnMethod.PASSWORD, 0, true, "localhost",2222);

   * Create a PatchSystem in memory for use in testing.
   * All attributes are to be updated.
   */
  public static PatchSystem makePatchSystemFull(String key, String systemId)
  {
    return new PatchSystem(description2, hostname2, effectiveUserId2,
            prot2.getAuthnMethod(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            sysNamePrefix+key+dtnSystemId2, dtnMountPoint2, dtnMountSourcePath2, jobRuntimes2, jobWorkingDir2,
            jobEnvVariables2, jobMaxJobs2, jobMaxJobsPerUser2, canRunBatchTrue, mpiCmd2, batchScheduler2,
            logicalQueueList2, batchDefaultLogicalQueue2, batchSchedulerProfile2, capList2, tags2, notes2, importRefId2);
  }

  /**
   * Create a PatchSystem in memory for use in testing.
   * Some attributes are to be updated: description, authnMethod, dtnMountPoint, runtimeList, jobMaxJobsPerUser
   */
  public static PatchSystem makePatchSystemPartial(String key, String systemId)
  {
    return new PatchSystem(description2, hostnameNull, effectiveUserIdNull,
            prot2.getAuthnMethod(), portNull, userProxyNull, proxyHostNull, proxyPortNull,
            dtnSystemIdNull, dtnMountPoint2, dtnMountSourcePathNull, jobRuntimes2, jobWorkingDirNull, jobEnvVariablesNull,
            jobMaxJobsNull, jobMaxJobsPerUser2, canRunBatchNull, mpiCmd2, batchSchedulerNull, logicalQueueListNull,
            batchDefaultLogicalQueueNull, batchSchedulerProfileNull, capListNull, tagsNull, notesNull, importRefIdNull);
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
      schedulerProfiles[i] = new SchedulerProfile(tenantName, name, "Test profile" + suffix, testUser2,
                                                  moduleLoadCmd, modulesToLoad, hiddenOptions, null, null, null);
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
