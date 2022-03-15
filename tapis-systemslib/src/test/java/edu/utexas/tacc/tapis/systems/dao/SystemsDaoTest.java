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
  private ResourceRequestUser rUser;

  // Create test system definitions and scheduler profiles in memory
  int numSystems = 13;
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
    rUser = new ResourceRequestUser(new AuthenticatedUser(apiUser, tenantName, TapisThreadContext.AccountType.user.name(),
                                                          null, apiUser, tenantName, null, null, null));
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
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
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
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
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
    Assert.assertEquals(tmpSys.getDtnMountSourcePath(), sys0.getDtnMountSourcePath());
    Assert.assertEquals(tmpSys.getDtnMountPoint(), sys0.getDtnMountPoint());
    Assert.assertEquals(tmpSys.isDtn(), sys0.isDtn());
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
  public void testGetSystems() throws Exception {
    TSystem sys0 = systems[4];
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    List<TSystem> systems = dao.getSystems(tenantName, null, null, null, DEFAULT_LIMIT, orderByListNull,
                                            DEFAULT_SKIP, startAfterNull, showDeletedFalse);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId());
    }
  }

  // Test retrieving all systems in a list of IDs
  @Test
  public void testGetSystemsInIDList() throws Exception
  {
    var sysIdList = new HashSet<String>();
    // Create 2 systems
    TSystem sys0 = systems[5];
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    sysIdList.add(sys0.getId());
    sys0 = systems[6];
    itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    sysIdList.add(sys0.getId());
    // Get all systems in list of seqIDs
    List<TSystem> systems = dao.getSystems(tenantName, null, null, sysIdList, DEFAULT_LIMIT, orderByListNull,
                                            DEFAULT_SKIP, startAfterNull, showDeletedFalse);
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId());
      Assert.assertTrue(sysIdList.contains(system.getId()));
    }
    Assert.assertEquals(sysIdList.size(), systems.size());
  }

  // Test enable/disable/delete/undelete
  @Test
  public void testEnableDisableDeleteUndeleteSystem() throws Exception
  {
    TSystem sys0 = systems[11];
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    System.out.println("Created item, id: " + sys0.getId() + " enabled: " + sys0.isEnabled());
    // Enabled should start off true, then become false and finally true again.
    TSystem tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertTrue(tmpSys.isEnabled());
    dao.updateEnabled(rUser, tenantName, sys0.getId(), false);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isEnabled());
    dao.updateEnabled(rUser, tenantName, sys0.getId(), true);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertTrue(tmpSys.isEnabled());

    // Deleted should start off false, then become true and finally false again.
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isDeleted());
    dao.updateDeleted(rUser, tenantName, sys0.getId(), true);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId(), true);
    Assert.assertTrue(tmpSys.isDeleted());
    dao.updateDeleted(rUser, tenantName, sys0.getId(), false);
    tmpSys = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertFalse(tmpSys.isDeleted());
  }

  // Test change system owner
  @Test
  public void testChangeSystemOwner() throws Exception
  {
    TSystem sys0 = systems[7];
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
    System.out.println("Created item with systemId: " + sys0.getId());
    Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
    dao.updateSystemOwner(rUser, tenantName, sys0.getId(), "newOwner");
    TSystem tmpSystem = dao.getSystem(sys0.getTenant(), sys0.getId());
    Assert.assertEquals(tmpSystem.getOwner(), "newOwner");
  }

  // Test hard deleting a single item
  @Test
  public void testHardDeleteSystem() throws Exception
  {
    TSystem sys0 = systems[9];
    boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
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
            dtnSystemFakeHostname, dtnMountPoint1, dtnMountSourcePath1, isDtnFalse,
            canExecTrue, jobRuntimes1, "jobWorkDir", jobEnvVariables1, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchTrue, mpiCmd1, batchScheduler1, logicalQueueList1, "batchDefaultLogicalQueue", batchSchedulerProfile1,
            capList1, tags1, notes1, importRefIdNull, uuidNull, isDeletedFalse, createdNull, updatedNull);
    // Make sure system does not exist
    Assert.assertFalse(dao.checkForSystem(tenantName, fakeSystemName, true));
    Assert.assertFalse(dao.checkForSystem(tenantName, fakeSystemName, false));
    // update should throw not found exception
    boolean pass = false;
    try { dao.patchSystem(rUser, fakeSystemName, patchedSystem, scrubbedJson, null); }
    catch (IllegalStateException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);
    Assert.assertNull(dao.getSystem(tenantName, fakeSystemName));
    Assert.assertNull(dao.getSystemOwner(tenantName, fakeSystemName));
  }

  // ******************************************************************
  //   Scheduler Profiles
  // ******************************************************************

  @Test
  public void testCreateSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[0];
    dao.createSchedulerProfile(rUser, p0, gson.toJson(p0), scrubbedJson);
    System.out.println("Scheduler Profile created: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[1];
    dao.createSchedulerProfile(rUser, p0, gson.toJson(p0), scrubbedJson);
    System.out.println("Scheduler Profile created: " + p0.getName());

    SchedulerProfile tmpProfile = dao.getSchedulerProfile(tenantName, p0.getName());
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    Assert.assertEquals(tmpProfile.getTenant(), p0.getTenant());
    Assert.assertEquals(tmpProfile.getName(), p0.getName());
    Assert.assertEquals(tmpProfile.getDescription(), p0.getDescription());
    Assert.assertEquals(tmpProfile.getOwner(), p0.getOwner());
    Assert.assertEquals(tmpProfile.getModuleLoadCommand(), p0.getModuleLoadCommand());

    Assert.assertNotNull(tmpProfile.getModulesToLoad());
    Assert.assertEquals(tmpProfile.getModulesToLoad().length, p0.getModulesToLoad().length);

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
    dao.createSchedulerProfile(rUser, p0, gson.toJson(p0), scrubbedJson);
    System.out.println("Scheduler Profile created: " + p0.getName());
    dao.deleteSchedulerProfile(rUser.getOboTenantId(), p0.getName());
    Assert.assertFalse(dao.checkForSchedulerProfile(tenantName, p0.getName()),
                       "Scheduler Profile not deleted. Profile name: " + p0.getName());
    System.out.println("Scheduler Profile deleted: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfiles() throws Exception {
    var profileNameList = new HashSet<String>();
    SchedulerProfile p0 = schedulerProfiles[3];
    dao.createSchedulerProfile(rUser, p0, gson.toJson(p0), scrubbedJson);
    System.out.println("Scheduler Profile created: " + p0.getName());
    profileNameList.add(p0.getName());
    p0 = schedulerProfiles[4];
    dao.createSchedulerProfile(rUser, p0, gson.toJson(p0), scrubbedJson);
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
  
 //Test retrieving a system history
 @Test
 public void testGetHistory() throws Exception {
   TSystem sys0 = systems[12];
   boolean itemCreated = dao.createSystem(rUser, sys0, gson.toJson(sys0), scrubbedJson);
   Assert.assertTrue(itemCreated, "Item not created, id: " + sys0.getId());
   List<SystemHistoryItem> systemHistory = dao.getSystemHistory(sys0.getId());
   
   Assert.assertNotNull(systemHistory, "Failed to create system history for item: " + sys0.getId());
   System.out.println("Found system history item: " + sys0.getId());
   
   Assert.assertEquals(systemHistory.size(), 1);
   for (SystemHistoryItem item:systemHistory) {     
     Assert.assertEquals(item.getOperation(), SystemOperation.create);
     Assert.assertNotNull(item.getCreated(), "Fetched created timestamp should not be null"); 
   }
 }
}
