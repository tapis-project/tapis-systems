package edu.utexas.tacc.tapis.systems.dao;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;

import org.jooq.tools.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

import static edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters.*;
import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsDao class against a DB running locally
 */
@Test(groups={"integration"})
public class SystemsDaoTest
{
  private SystemsDaoImpl dao;
  private ResourceRequestUser rOwner1, rOwner2, rOwner3, rOwner4, rOwner5, rOwner6, rOwner7;

  // Create test system definitions and scheduler profiles in memory
  int numSystems = 17; // All in use: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  int numSchedulerProfiles = 5;
  String testKey = "Dao";
  TSystem dtnSystem1 = IntegrationUtils.makeDtnSystem1(testKey);
  TSystem dtnSystem2 = IntegrationUtils.makeDtnSystem2(testKey);
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);
  SchedulerProfile[] schedulerProfiles = IntegrationUtils.makeSchedulerProfiles(numSchedulerProfiles, testKey);

  @BeforeSuite
  public void setup() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsDaoTest.class.getSimpleName());
    dao = new SystemsDaoImpl();
    // Initialize authenticated user
    rOwner1 = new ResourceRequestUser(new AuthenticatedUser(owner1, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner1, tenantName, null, null, null));
    rOwner2 = new ResourceRequestUser(new AuthenticatedUser(owner2, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner2, tenantName, null, null, null));
    rOwner3 = new ResourceRequestUser(new AuthenticatedUser(owner3, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner3, tenantName, null, null, null));
    rOwner4 = new ResourceRequestUser(new AuthenticatedUser(owner4, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner4, tenantName, null, null, null));
    rOwner5 = new ResourceRequestUser(new AuthenticatedUser(owner5, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner5, tenantName, null, null, null));
    rOwner6 = new ResourceRequestUser(new AuthenticatedUser(owner6, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner6, tenantName, null, null, null));
    rOwner7 = new ResourceRequestUser(new AuthenticatedUser(owner7, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner7, tenantName, null, null, null));
    // Cleanup anything leftover from previous failed run
    teardown();
  }

  @AfterSuite
  public void teardown() throws Exception {
    System.out.println("Executing AfterSuite teardown for " + SystemsDaoTest.class.getSimpleName());
    //Remove all objects created by tests
    for (int i = 0; i < numSystems; i++)
    {
      dao.hardDeleteSystem(tenantName, systems[i].getId());
    }

    // Delete scheduler profiles
    for (int i = 0; i < numSchedulerProfiles; i++)
    {
      dao.deleteSchedulerProfile(tenantName, schedulerProfiles[i].getName());
    }

    Assert.assertFalse(dao.checkForSystem(tenantName, systems[0].getId(), true),
                       "System not deleted. System name: " + systems[0].getId());
    Assert.assertFalse(dao.checkForSchedulerProfile(tenantName, schedulerProfiles[0].getName()),
                       "Scheduler Profile not deleted. Profile name: " + schedulerProfiles[0].getName());
  }

  // Test create for a single item
  @Test
  public void testCreateSystem() throws Exception
  {
    TSystem sys0 = systems[0];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
  }

  // ******************************************************************
  //   Systems
  // ******************************************************************

  // Test retrieving a single item
  @Test
  public void testGetSystem() throws Exception
  {
    TSystem sys0 = systems[1];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    TSystem tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod(), sys0.getDefaultAuthnMethod());
    Assert.assertEquals(tmpSys.getBucketName(), sys0.getBucketName());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());

    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
    Assert.assertEquals(tmpSys.getDtnSystemId(), sys0.getDtnSystemId());
    Assert.assertEquals(tmpSys.getCanExec(), sys0.getCanExec());
    Assert.assertEquals(tmpSys.getJobWorkingDir(), sys0.getJobWorkingDir());

    // Verify jobEnvVariables
    verifyKeyValuePairs(sys0.getJobEnvVariables(), tmpSys.getJobEnvVariables());

    Assert.assertEquals(tmpSys.getJobMaxJobs(), sys0.getJobMaxJobs());
    Assert.assertEquals(tmpSys.getJobMaxJobsPerUser(), sys0.getJobMaxJobsPerUser());
    Assert.assertEquals(tmpSys.getCanRunBatch(), sys0.getCanRunBatch());
    Assert.assertEquals(tmpSys.getMpiCmd(), sys0.getMpiCmd());
    Assert.assertEquals(tmpSys.getBatchScheduler(), sys0.getBatchScheduler());
    Assert.assertEquals(tmpSys.getBatchDefaultLogicalQueue(), sys0.getBatchDefaultLogicalQueue());
    Assert.assertEquals(tmpSys.getBatchSchedulerProfile(), sys0.getBatchSchedulerProfile());

    // Verify tags
    String[] tmpTags = tmpSys.getTags();
    Assert.assertNotNull(tmpTags, "Tags value was null");
    var tagsList = Arrays.asList(tmpTags);
    Assert.assertEquals(tmpTags.length, tags1.length, "Wrong number of tags");
    for (String tagStr : tags1)
    {
      Assert.assertTrue(tagsList.contains(tagStr));
      System.out.println("Found tag: " + tagStr);
    }
    // Verify notes
    JsonObject obj = (JsonObject) tmpSys.getNotes();
    Assert.assertNotNull(obj, "Notes object was null");
    Assert.assertTrue(obj.has("project"));
    Assert.assertEquals(obj.get("project").getAsString(), notesObj1.get("project").getAsString());
    Assert.assertTrue(obj.has("testdata"));
    Assert.assertEquals(obj.get("testdata").getAsString(), notesObj1.get("testdata").getAsString());

    // Verify capabilities
    List<Capability> origCaps = sys0.getJobCapabilities();
    List<Capability> jobCaps = tmpSys.getJobCapabilities();
    Assert.assertNotNull(origCaps, "Orig Caps was null");
    Assert.assertNotNull(jobCaps, "Fetched Caps was null");
    Assert.assertEquals(jobCaps.size(), origCaps.size());
    var capNamesFound = new ArrayList<String>();
    for (Capability capFound : jobCaps) {capNamesFound.add(capFound.getName());}
    for (Capability capSeedItem : origCaps)
    {
      Assert.assertTrue(capNamesFound.contains(capSeedItem.getName()),
              "List of capabilities did not contain a capability named: " + capSeedItem.getName());
    }
    // Verify jobRuntimes
    List<JobRuntime> origRuntimes = sys0.getJobRuntimes();
    List<JobRuntime> jobRuntimes = tmpSys.getJobRuntimes();
    Assert.assertNotNull(origRuntimes, "Orig Runtimes was null");
    Assert.assertNotNull(jobRuntimes, "Fetched Runtimes was null");
    Assert.assertEquals(jobRuntimes.size(), origRuntimes.size());
    var runtimeVersionsFound = new ArrayList<String>();
    for (JobRuntime runtimeFound : jobRuntimes) {runtimeVersionsFound.add(runtimeFound.getVersion());}
    for (JobRuntime runtimeSeedItem : origRuntimes)
    {
      Assert.assertTrue(runtimeVersionsFound.contains(runtimeSeedItem.getVersion()),
              "List of jobRuntimes did not contain a runtime with version: " + runtimeSeedItem.getVersion());
    }
    Assert.assertNotNull(tmpSys.getCreated(), "Fetched created timestamp should not be null");
    Assert.assertNotNull(tmpSys.getUpdated(), "Fetched updated timestamp should not be null");
  }

  // Test retrieving all systems
  @Test
  public void testGetSystems() throws Exception
  {
    TSystem sys0 = systems[4];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    List<TSystem> systems = dao.getSystems(rOwner1, null, null, null, DEFAULT_LIMIT, orderByListNull, DEFAULT_SKIP,
                                           startAfterNull, showDeletedFalse, listTypeAll, setOfIDsNull, setOfIDsNull);
    for (TSystem system : systems)
    {
      System.out.println("Found item with id: " + system.getId());
    }
  }

  // Test getSystems using listType parameter
  @Test
  public void testGetSystemsByListType() throws Exception
  {
    var viewableIDs = new HashSet<String>();
    var sharedPublicIDS = new HashSet<String>();
    var sharedIDs = new HashSet<String>();
    // NOTE: User owner7 rather than owner3 because SystemsServiceTest uses owner3 and when tests run in parallel
    //       from command line there is a failure.
    // Create 4 systems.
    // One owned by owner7
    // One owned by owner4 with READ permission granted to owner7
    // One owned by owner5 and shared with owner7
    // One owned by owner6 and shared publicly
    TSystem sys0;
    sys0 = systems[2]; sys0.setOwner(owner7); dao.createSystem(rOwner7, sys0, gson.toJson(sys0), rawDataEmptyJson);
    sys0 = systems[3]; sys0.setOwner(owner4); dao.createSystem(rOwner4, sys0, gson.toJson(sys0), rawDataEmptyJson);
    viewableIDs.add(sys0.getId());
    sys0 = systems[8]; sys0.setOwner(owner5); dao.createSystem(rOwner5, sys0, gson.toJson(sys0), rawDataEmptyJson);
    sharedIDs.add(sys0.getId());
    sys0 = systems[10]; sys0.setOwner(owner6); dao.createSystem(rOwner6, sys0, gson.toJson(sys0), rawDataEmptyJson);
    sharedPublicIDS.add(sys0.getId());
    sharedIDs.add(sys0.getId());
    List<TSystem> systems;
    // Simulate getting just OWNED
    systems = dao.getSystems(rOwner7, null, null, null, DEFAULT_LIMIT, orderByListNull, DEFAULT_SKIP, startAfterNull,
                             showDeletedFalse, listTypeOwned, setOfIDsNull, setOfIDsNull);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypeOwned);
    Assert.assertEquals(systems.size(), 1, "Wrong number of returned systems for listType=" + listTypeOwned);
    // Simulate getting just getting PUBLIC
    systems = dao.getSystems(rOwner7, null, null, null, DEFAULT_LIMIT, orderByListNull, DEFAULT_SKIP, startAfterNull,
                             showDeletedFalse, listTypePublic, setOfIDsNull, sharedPublicIDS);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypePublic);
    Assert.assertEquals(systems.size(), 1, "Wrong number of returned systems for listType=" + listTypePublic);
    // Simulate getting ALL
    systems = dao.getSystems(rOwner7, null, null, null, DEFAULT_LIMIT, orderByListNull, DEFAULT_SKIP, startAfterNull,
                             showDeletedFalse, listTypeAll, viewableIDs, sharedIDs);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypeAll);
    Assert.assertEquals(systems.size(), 4, "Wrong number of returned systems for listType=" + listTypeAll);
  }

  // Test enable/disable/delete/undelete
  @Test
  public void testEnableDisableDeleteUndeleteSystem() throws Exception
  {
    TSystem sys0 = systems[11];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    System.out.println("Created item, id: " + sys0.getId() + " enabled: " + sys0.isEnabled());
    // Enabled should start off true, then become false and finally true again.
    TSystem tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertTrue(tmpSys.isEnabled());
    dao.updateEnabled(rOwner1, tenantName, sys0.getId(), false);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isEnabled());
    dao.updateEnabled(rOwner1, tenantName, sys0.getId(), true);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertTrue(tmpSys.isEnabled());

    // Deleted should start off false, then become true and finally false again.
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isDeleted());
    dao.updateDeleted(rOwner1, tenantName, sys0.getId(), true);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId(), true);
    Assert.assertTrue(tmpSys.isDeleted());
    dao.updateDeleted(rOwner1, tenantName, sys0.getId(), false);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isDeleted());
  }

  // Test change system owner
  @Test
  public void testChangeSystemOwner() throws Exception
  {
    TSystem sys0 = systems[7];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    System.out.println("Created item with systemId: " + sys0.getId());
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    dao.updateSystemOwner(rOwner1, sys0.getId(), apiUser, "newOwner");
    TSystem tmpSystem = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertEquals(tmpSystem.getOwner(), "newOwner");
  }

  // Test hard deleting a single item
  @Test
  public void testHardDeleteSystem() throws Exception
  {
    TSystem sys0 = systems[9];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    System.out.println("Created item with systemId: " + sys0.getId());
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    dao.hardDeleteSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(dao.checkForSystem(sys0.getTenant(), sys0.getId(), true),"System not deleted. System name: " + sys0.getId());
  }

  // Test behavior when system is missing, especially for cases where service layer depends on the behavior.
  //  update - throws not found exception
  //  get - returns null
  //  check - returns false
  //  getOwner - returns null
  @Test
  public void testMissingSystem() throws Exception
  {
    String fakeSystemName = "AMissingSystemName";
    TSystem patchedSystem = new TSystem(1, tenantName, fakeSystemName, "description", SystemType.LINUX, "owner", "host",
            isEnabledTrue, "effUser", prot2.getAuthnMethod(), "bucket", "/root",
            prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemFakeHostname,
            canExecTrue, jobRuntimes1, "jobWorkDir", jobEnvVariables1, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchTrue, enableCmdPrefixTrue, mpiCmd1, batchScheduler1, logicalQueueList1,
            "batchDefaultLogicalQueue", batchSchedulerProfile1, capList1, tags1, notes1,
            importRefIdNull, uuidNull, isDeletedFalse, allowChildrenFalse, parentIdNull, createdNull, updatedNull);
    // Make sure system does not exist
    Assert.assertFalse(dao.checkForSystem(tenantName, fakeSystemName, true));
    Assert.assertFalse(dao.checkForSystem(tenantName, fakeSystemName, false));
    // update should throw not found exception
    boolean pass = false;
    try { dao.patchSystem(rOwner1, fakeSystemName, patchedSystem, rawDataEmptyJson, null); }
    catch (IllegalStateException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);
    Assert.assertNull(dao.getSystem(tenantName, fakeSystemName));
    Assert.assertNull(dao.getSystemOwner(tenantName, fakeSystemName));
  }

  //Test retrieving a system history
  @Test
  public void testGetSystemHistory() throws Exception {
    TSystem sys0 = systems[12];
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    List<SystemHistoryItem> systemHistory = dao.getSystemHistory(tenantName, sys0.getId());

    Assert.assertNotNull(systemHistory, "Failed to create system history for item: " + sys0.getId());
    System.out.println("Found system history item: " + sys0.getId());

    Assert.assertEquals(systemHistory.size(), 1);
    for (SystemHistoryItem item:systemHistory) {
      Assert.assertNotNull(item.getJwtTenant(), "Fetched API Tenant should not be null");
      Assert.assertNotNull(item.getJwtTenant(), "Fetched API User should not be null");
      Assert.assertNotNull(item.getOboTenant(), "Fetched OBO Tenant should not be null");
      Assert.assertNotNull(item.getOboUser(), "Fetched OBO User should not be null");
      Assert.assertEquals(item.getOperation(), SystemOperation.create);
      Assert.assertNotNull(item.getDescription(), "Fetched Json should not be null");
      Assert.assertNotNull(item.getCreated(), "Fetched created timestamp should not be null");
    }
  }
  // ******************************************************************
  //   TapisUser to LoginUser mapping
  // ******************************************************************

  @Test
  public void testCreateGetDeleteLoginMapping() throws Exception
  {
    // Create a new system - we need id as foreign key
    TSystem sys0 = systems[13];
    String sysId = sys0.getId();
    String tapisUser = owner1;
    boolean itemCreated = dao.createSystem(rOwner1, sys0, gson.toJson(sys0), rawDataEmptyJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sysId);
    dao.createOrUpdateLoginUserMapping(tenantName, sysId, tapisUser, loginUser1, isStaticTrue);
    System.out.println("Login map entry created");
    String loginUser = dao.getLoginUser(tenantName, sysId, tapisUser);
    Assert.assertEquals(loginUser, loginUser1);
    dao.deleteLoginUserMapping(rOwner1, tenantName, sysId, tapisUser);
    loginUser = dao.getLoginUser(tenantName, sysId, tapisUser);
    Assert.assertNull(loginUser);
  }

  // ******************************************************************
  //   Scheduler Profiles
  // ******************************************************************

  @Test
  public void testCreateSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[0];
    dao.createSchedulerProfile(rOwner1, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[1];
    dao.createSchedulerProfile(rOwner1, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());

    SchedulerProfile tmpProfile = dao.getSchedulerProfile(tenantName, p0.getName());
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    Assert.assertEquals(tmpProfile.getTenant(), p0.getTenant());
    Assert.assertEquals(tmpProfile.getName(), p0.getName());
    Assert.assertEquals(tmpProfile.getDescription(), p0.getDescription());
    Assert.assertEquals(tmpProfile.getOwner(), p0.getOwner());

    var tmpModLoads = tmpProfile.getModuleLoads();
    var p0ModLoads = p0.getModuleLoads();
    Assert.assertNotNull(p0ModLoads);
    Assert.assertNotNull(p0ModLoads.get(0));
    Assert.assertNotNull(p0ModLoads.get(0).getModulesToLoad());
    Assert.assertNotNull(tmpModLoads, "tmpModuleLoads was null");
    Assert.assertEquals(tmpModLoads.get(0).getModuleLoadCommand(), p0ModLoads.get(0).getModuleLoadCommand());
    Assert.assertNotNull(tmpModLoads.get(0).getModulesToLoad());
    Assert.assertEquals(tmpModLoads.get(0).getModulesToLoad().length, p0ModLoads.get(0).getModulesToLoad().length);

    Assert.assertNotNull(tmpProfile.getHiddenOptions());
    Assert.assertFalse(tmpProfile.getHiddenOptions().isEmpty());
    Assert.assertEquals(tmpProfile.getHiddenOptions().size(), p0.getHiddenOptions().size());
    Assert.assertTrue(tmpProfile.getHiddenOptions().contains(SchedulerProfile.HiddenOption.MEM));

    Assert.assertNotNull(tmpProfile.getUuid());
    Assert.assertNotNull(tmpProfile.getCreated());
    Assert.assertNotNull(tmpProfile.getUpdated());
    Assert.assertFalse(StringUtils.isBlank(tmpProfile.getUuid().toString()));
    Assert.assertFalse(StringUtils.isBlank(tmpProfile.getCreated().toString()));
    Assert.assertFalse(StringUtils.isBlank(tmpProfile.getUpdated().toString()));
  }

  @Test
  public void testDeleteSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[2];
    dao.createSchedulerProfile(rOwner1, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
    dao.deleteSchedulerProfile(rOwner1.getOboTenantId(), p0.getName());
    Assert.assertFalse(dao.checkForSchedulerProfile(tenantName, p0.getName()),
                       "Scheduler Profile not deleted. Profile name: " + p0.getName());
    System.out.println("Scheduler Profile deleted: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfiles() throws Exception {
    var profileNameList = new HashSet<String>();
    SchedulerProfile p0 = schedulerProfiles[3];
    dao.createSchedulerProfile(rOwner1, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
    profileNameList.add(p0.getName());
    p0 = schedulerProfiles[4];
    dao.createSchedulerProfile(rOwner1, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
    profileNameList.add(p0.getName());

    List<SchedulerProfile> profiles = dao.getSchedulerProfiles(tenantName);
    Assert.assertNotNull(profiles, "getSchedulerProfiles returned null");
    Assert.assertFalse(profiles.isEmpty(), "getSchedulerProfiles returned empty list");
    for (SchedulerProfile profile : profiles)
    {
      System.out.println("Found item with name: " + profile.getName());
      Assert.assertNotNull(profile.getHiddenOptions());
      Assert.assertFalse(profile.getHiddenOptions().isEmpty());
      Assert.assertTrue(profile.getHiddenOptions().contains(SchedulerProfile.HiddenOption.MEM));
    }
    Assert.assertTrue(profileNameList.contains(schedulerProfiles[3].getName()),
                      "getSchedulerProfiles did not return item with name: " + schedulerProfiles[3].getName());
    Assert.assertTrue(profileNameList.contains(schedulerProfiles[4].getName()),
                      "getSchedulerProfiles did not return item with name: " + schedulerProfiles[4].getName());
  }

  @Test
  public void testHasChildrenAndGetParent() throws Exception {
    TSystem hasNoChildrenSys = systems[14];
    TSystem childSys = systems[15];
    TSystem hasChildrenSys = systems[16];
    hasChildrenSys.setAllowChildren(true);

    // parent id is not exposed in a a setter - use reflaction to set it.  This could be
    // have used a special child constructor, but this is the easiest way to fit in with the
    // existing test framework.
    Field field = TSystem.class.getDeclaredField("parentId");
    field.setAccessible(true);
    field.set(childSys, hasChildrenSys.getId());

    Assert.assertTrue(dao.createSystem(rOwner1, hasNoChildrenSys, gson.toJson(hasNoChildrenSys), rawDataEmptyJson));
    Assert.assertTrue(dao.createSystem(rOwner1, hasChildrenSys, gson.toJson(hasChildrenSys), rawDataEmptyJson));
    Assert.assertTrue(dao.createSystem(rOwner1, childSys, gson.toJson(childSys), rawDataEmptyJson));

    Assert.assertFalse(dao.hasChildren(rOwner1.getOboTenantId(), hasNoChildrenSys.getId()));
    Assert.assertTrue(dao.hasChildren(rOwner1.getOboTenantId(), hasChildrenSys.getId()));

    String parentId = dao.getParent(rOwner1.getOboTenantId(), childSys.getId());
    Assert.assertEquals(hasChildrenSys.getId(), parentId);

    Assert.assertNull(dao.getParent(rOwner1.getOboTenantId(), hasChildrenSys.getId()));
    Assert.assertNull(dao.getParent(rOwner1.getOboTenantId(), hasNoChildrenSys.getId()));
  }
}
