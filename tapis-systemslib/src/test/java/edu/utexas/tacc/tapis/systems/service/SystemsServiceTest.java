package edu.utexas.tacc.tapis.systems.service;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.IntegrationUtils;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.JobRuntime;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.jooq.tools.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.utexas.tacc.tapis.systems.IntegrationUtils.*;

/**
 * Test the SystemsService implementation class against a DB running locally
 * Note that this test has the following dependencies running locally or in dev
 *    Database - typically local
 *    Tenants service - typically dev
 *    Tokens service - typically dev and obtained from tenants service
 *    Security Kernel service - typically dev and obtained from tenants service
 *
 */
@Test(groups={"integration"})
public class SystemsServiceTest
{
  private SystemsService svc;
  private SystemsServiceImpl svcImpl;
  private ResourceRequestUser rOwner1, rTestUser0, rTestUser1, rTestUser2,
          rTestUser3, rTestUser4, rAdminUser, rSystemsSvc,
          rFilesSvcOwner1, rFilesSvcTestUser3, rFilesSvcTestUser4;

  // Create test system definitions and scheduler profiles in memory
  String testKey = "Svc";
  int numSystems = 28; // UNUSED SYSTEMS: systems[3]
  int numSchedulerProfiles = 7;
  TSystem dtnSystem1 = IntegrationUtils.makeDtnSystem1(testKey);
  TSystem dtnSystem2 = IntegrationUtils.makeDtnSystem2(testKey);
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);
  SchedulerProfile[] schedulerProfiles = IntegrationUtils.makeSchedulerProfiles(numSchedulerProfiles, testKey);

  @BeforeSuite
  public void setUp() throws Exception
  {
    System.out.println("Executing BeforeSuite setup method: " + SystemsServiceTest.class.getSimpleName());
    // Setup for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure() {
        bind(SystemsServiceImpl.class).to(SystemsService.class);
        bind(SystemsServiceImpl.class).to(SystemsServiceImpl.class);
        bind(SystemsDaoImpl.class).to(SystemsDao.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class);
      }
    });
    locator.inject(this);

    // Initialize TenantManager and services
    String url = RuntimeParameters.getInstance().getTenantsSvcURL();
    TenantManager.getInstance(url).getTenants();

    // Initialize services
    svc = locator.getService(SystemsService.class);
    svcImpl = locator.getService(SystemsServiceImpl.class);
    svcImpl.initService(siteId, adminTenantName, RuntimeParameters.getInstance().getServicePassword());

    // Initialize users and service
    rAdminUser = new ResourceRequestUser(new AuthenticatedUser(adminUser, tenantName, TapisThreadContext.AccountType.user.name(),
                                                    null, adminUser, tenantName, null, null, null));
    rOwner1 = new ResourceRequestUser(new AuthenticatedUser(owner1, tenantName, TapisThreadContext.AccountType.user.name(),
                                                null, owner1, tenantName, null, null, null));
    rTestUser0 = new ResourceRequestUser(new AuthenticatedUser(testUser0, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser0, tenantName, null, null, null));
    rTestUser1 = new ResourceRequestUser(new AuthenticatedUser(testUser1, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser1, tenantName, null, null, null));
    rTestUser2 = new ResourceRequestUser(new AuthenticatedUser(testUser2, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser2, tenantName, null, null, null));
    rTestUser3 = new ResourceRequestUser(new AuthenticatedUser(testUser3, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser3, tenantName, null, null, null));
    rTestUser4 = new ResourceRequestUser(new AuthenticatedUser(testUser4, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser4, tenantName, null, null, null));
    rSystemsSvc = new ResourceRequestUser(new AuthenticatedUser(svcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                    null, svcName, adminTenantName, null, null, null));
    rFilesSvcOwner1 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, owner1, tenantName, null, null, null));
    rFilesSvcTestUser3 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, testUser3, tenantName, null, null, null));
    rFilesSvcTestUser4 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, testUser4, tenantName, null, null, null));

    // Cleanup anything leftover from previous failed run
    tearDown();

    // Create DTN systems for other systems to reference. Otherwise some system definitions are not valid.
    svc.createSystem(rOwner1, dtnSystem1, skipCredCheckTrue, rawDataEmtpyJson);
    svc.createSystem(rOwner1, dtnSystem2, skipCredCheckTrue, rawDataEmtpyJson);

    // Create a scheduler profile for systems to reference
    System.out.println("Creating scheduler profile with name: " + batchSchedulerProfile1);
    List<SchedulerProfile.HiddenOption> hiddenOptions = Arrays.asList(SchedulerProfile.HiddenOption.MEM);
    SchedulerProfile sp = new SchedulerProfile(tenantName, batchSchedulerProfile1, "test profile1",  owner1, "module load1",
                                               null, hiddenOptions, null, null, null);
    svc.createSchedulerProfile(rOwner1, sp);
    System.out.println("Creating scheduler profile with name: " + batchSchedulerProfile2);
    sp = new SchedulerProfile(tenantName, batchSchedulerProfile2, "test profile2",  owner1, "module load2",
                              null, hiddenOptions, null, null, null);
    svc.createSchedulerProfile(rOwner1, sp);
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown for " + SystemsServiceTest.class.getSimpleName());
    // Remove non-owner permissions granted during the tests
    try { svc.revokeUserPermissions(rOwner1, systems[9].getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[12].getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[12].getId(), testUser2, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[14].getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[14].getId(), testUser2, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (Exception e) { }

    // Remove all objects created by tests
    for (int i = 0; i < numSystems; i++)
    {
      svcImpl.hardDeleteSystem(rAdminUser, tenantName, systems[i].getId());
    }
    svcImpl.hardDeleteSystem(rAdminUser, tenantName, dtnSystem2.getId());
    svcImpl.hardDeleteSystem(rAdminUser, tenantName, dtnSystem1.getId());

    for (int i = 0; i < numSchedulerProfiles; i++)
    {
      svc.deleteSchedulerProfile(rTestUser2, schedulerProfiles[i].getName());
    }
    svc.deleteSchedulerProfile(rOwner1, batchSchedulerProfile1);
    svc.deleteSchedulerProfile(rOwner1, batchSchedulerProfile2);

    Assert.assertFalse(svc.checkForSystem(rAdminUser, systems[0].getId()),
            "System not deleted. System name: " + systems[0].getId());
    Assert.assertFalse(svc.checkForSchedulerProfile(rAdminUser, schedulerProfiles[0].getName()),
            "SchedulerProfile not deleted. Profile name: " + schedulerProfiles[0].getName());
  }

  @Test
  public void testCreateSystem() throws Exception
  {
    TSystem sys0 = systems[0];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
  }

  // Create a system using minimal attributes:
  //   id, systemType, host, defaultAuthnMethod, canExec
  @Test
  public void testCreateSystemMinimal() throws Exception
  {
    TSystem sys0 = makeMinimalSystem(systems[11], null);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
  }

  // TODO: cred check fails when it should not when run from laptop
  //   Error is:
  //   TAPIS_SSH_CONNECT_ERROR Unable to establish SSH session on host 129.114.17.113 (port 22) for user testuser3
  //   using PUBLICKEY_AUTH authentication: java.lang.IllegalArgumentException: Invalid host pattern char in (129.114.60.128)
//TODO  @Test
//  public void testCreateSystemCredCheck() throws Exception
//  {
//    TSystem sys0 = systems[2];
//    sys0.setEffectiveUserId("testuser2"); //TODO
//// TODO    sys0.setEffectiveUserId("testuser99"); //TODO
//    Credential cred0 = new Credential(null, "fakePassword", "fakePrivateKey", "fakePublicKey",
//            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
//    sys0.setAuthnCredential(cred0);
//
//    // Using invalid credentials should fail with exception
//    try {
//      svc.createSystem(rOwner1, sys0, skipCredCheckFalse, scrubbedJson);
//      Assert.fail("System create call should have thrown an exception when credentials are invalid");
//    } catch (Exception e) {
//      Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID"));
//    }
//
//    // Using valid credentials should succeed. TODO:
//    String password = "testuser99";
//    cred0 = new Credential(null, password, null, null, null, null, null);
//    sys0.setAuthnCredential(cred0);
//    svc.createSystem(rOwner1, sys0, skipCredCheckFalse, scrubbedJson);
//  }
//
  // Test retrieving a system including default authn method
  //   and test retrieving for specified authn method.
  @Test
  public void testGetSystem() throws Exception
  {
    TSystem sys0 = systems[1];
    sys0.setJobCapabilities(capList1);
    Credential cred0 = new Credential(null, null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Retrieve system as owner, without and with requireExecPerm
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
    checkCommonSysAttrs(sys0, tmpSys);
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, true, false, false);
    checkCommonSysAttrs(sys0, tmpSys);
    // Retrieve the system including the credential using the default authn method defined for the system
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), null, false, true, false);
    checkCommonSysAttrs(sys0, tmpSys);
    // Verify credentials. Only cred for default authnMethod is returned. In this case PKI_KEYS.
    Credential cred = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred, "AuthnCredential should not be null");
    Assert.assertEquals(cred.getAuthnMethod(), AuthnMethod.PKI_KEYS);
    Assert.assertEquals(cred.getPrivateKey(), cred0.getPrivateKey());
    Assert.assertEquals(cred.getPublicKey(), cred0.getPublicKey());
    Assert.assertNull(cred.getPassword(), "AuthnCredential password should be null");
    Assert.assertNull(cred.getAccessKey(), "AuthnCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AuthnCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AuthnCredential certificate should be null");

    // Test retrieval using specified authn method
    tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), AuthnMethod.PASSWORD, false, true, false);
    System.out.println("Found item: " + sys0.getId());
    // Verify credentials. Only cred for default authnMethod is returned. In this case PASSWORD.
    cred = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred, "AuthnCredential should not be null");
    Assert.assertEquals(cred.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertEquals(cred.getPassword(), cred0.getPassword());
    Assert.assertNull(cred.getPrivateKey(), "AuthnCredential private key should be null");
    Assert.assertNull(cred.getPublicKey(), "AuthnCredential public key should be null");
    Assert.assertNull(cred.getAccessKey(), "AuthnCredential access key should be null");
    Assert.assertNull(cred.getAccessSecret(), "AuthnCredential access secret should be null");
    Assert.assertNull(cred.getCertificate(), "AuthnCredential certificate should be null");
  }

  // Test updating a system using put
  // Both update of all possible attributes and only some attributes
  @Test
  public void testPutSystem() throws Exception
  {
    TSystem sys0 = systems[25];
    String systemId = sys0.getId();
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    String rawDataCreate = "{\"testPut\": \"0-create1\"}";
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataCreate);
    TSystem tmpSys = svc.getSystem(rOwner1, systemId, null, false, false, false);
    // Get last updated timestamp
    LocalDateTime updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr1 = TapisUtils.getSQLStringFromUTCTime(updated);
    Thread.sleep(300);

    // Create putSystem where all updatable attributes are changed
    String rawDataPut = "{\"testPut\": \"1-put1\"}";
    TSystem putSystem = IntegrationUtils.makePutSystemFull(testKey, tmpSys);

    // Update using PUT
    svc.putSystem(rOwner1, putSystem, skipCredCheckTrue, rawDataPut);
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);

    // Get last updated timestamp
    updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr2 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Make sure update timestamp has been modified
    System.out.println("Updated timestamp before: " + updatedStr1 + " after: " + updatedStr2);
    Assert.assertNotEquals(updatedStr1, updatedStr2, "Update timestamp was not updated. Both are: " + updatedStr1);

    // Update original definition with modified values so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setHost(hostname2);
    sys0.setEffectiveUserId(effectiveUserId2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setPort(prot2.getPort());
    sys0.setUseProxy(prot2.isUseProxy());
    sys0.setProxyHost(prot2.getProxyHost());
    sys0.setProxyPort(prot2.getProxyPort());
    sys0.setDtnSystemId(sysNamePrefix+ testKey +dtnSystemId2);
    sys0.setDtnMountPoint(dtnMountPoint2);
    sys0.setDtnMountSourcePath(dtnMountSourcePath2);
    sys0.setMpiCmd(mpiCmd2);
    sys0.setJobWorkingDir(jobWorkingDir2);
    sys0.setJobEnvVariables(jobEnvVariables2);
    sys0.setJobMaxJobs(jobMaxJobs2);
    sys0.setJobMaxJobsPerUser(jobMaxJobsPerUser2);
    sys0.setBatchScheduler(batchScheduler2);
    sys0.setBatchDefaultLogicalQueue(batchDefaultLogicalQueue2);
    sys0.setBatchSchedulerProfile(batchSchedulerProfile2);
    sys0.setTags(tags2);
    sys0.setNotes(notes2);
    sys0.setImportRefId(importRefId2);
    sys0.setJobRuntimes(jobRuntimes2);
    sys0.setBatchLogicalQueues(logicalQueueList2);
    sys0.setJobCapabilities(capList2);
    // Check common system attributes:
    checkCommonSysAttrs(sys0, tmpSys);
  }

  // Test updating a system using patch
  // Both update of all possible attributes and only some attributes
  @Test
  public void testPatchSystem() throws Exception
  {
    TSystem sys0 = systems[13];
    String systemId = sys0.getId();
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    String rawDataCreate = "{\"testUpdate\": \"0-create1\"}";
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataCreate);
    TSystem tmpSys = svc.getSystem(rOwner1, systemId, null, false, false, false);
    // Get last updated timestamp
    LocalDateTime updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr1 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Pause briefly to make sure updated timestamp changes for patch
    Thread.sleep(300);

    // Create patchSystem where all updatable attributes are changed
    String rawDataPatch = "{\"testUpdate\": \"1-patch1\"}";
    PatchSystem patchSystemFull = IntegrationUtils.makePatchSystemFull(testKey, systemId);

    // Update using patchSys
    svc.patchSystem(rOwner1, systemId, patchSystemFull, rawDataPatch);
    TSystem tmpSysFull = svc.getSystem(rOwner1, systemId, null, false, false, false);

    // Get last updated timestamp
    updated = LocalDateTime.ofInstant(tmpSysFull.getUpdated(), ZoneOffset.UTC);
    String updatedStr2 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Make sure update timestamp has been modified
    System.out.println("Updated timestamp before: " + updatedStr1 + " after: " + updatedStr2);
    Assert.assertNotEquals(updatedStr1, updatedStr2, "Update timestamp was not updated. Both are: " + updatedStr1);

    // Update original definition with patched values so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setHost(hostname2);
    sys0.setEffectiveUserId(effectiveUserId2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setPort(prot2.getPort());
    sys0.setUseProxy(prot2.isUseProxy());
    sys0.setProxyHost(prot2.getProxyHost());
    sys0.setProxyPort(prot2.getProxyPort());
    sys0.setDtnSystemId(sysNamePrefix+ testKey +dtnSystemId2);
    sys0.setDtnMountPoint(dtnMountPoint2);
    sys0.setDtnMountSourcePath(dtnMountSourcePath2);
    sys0.setMpiCmd(mpiCmd2);
    sys0.setJobWorkingDir(jobWorkingDir2);
    sys0.setJobEnvVariables(jobEnvVariables2);
    sys0.setJobMaxJobs(jobMaxJobs2);
    sys0.setJobMaxJobsPerUser(jobMaxJobsPerUser2);
    sys0.setBatchScheduler(batchScheduler2);
    sys0.setBatchDefaultLogicalQueue(batchDefaultLogicalQueue2);
    sys0.setBatchSchedulerProfile(batchSchedulerProfile2);
    sys0.setTags(tags2);
    sys0.setNotes(notes2);
    sys0.setImportRefId(importRefId2);
    sys0.setJobRuntimes(jobRuntimes2);
    sys0.setBatchLogicalQueues(logicalQueueList2);
    sys0.setJobCapabilities(capList2);
    // Check common system attributes:
    checkCommonSysAttrs(sys0, tmpSysFull);

    // Test updating just a few attributes
    sys0 = systems[22];
    systemId = sys0.getId();
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    rawDataCreate = "{\"testUpdate\": \"0-create2\"}";
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataCreate);
    // Create patchSystem where some attributes are changed
    //   * Some attributes are to be updated: description, authnMethod, dtnMountPoint, runtimeList, jobMaxJobsPerUser
    String rawDataPatch2 = "{\"testUpdate\": \"1-patch2\"}";
    PatchSystem patchSystemPartial = IntegrationUtils.makePatchSystemPartial(testKey, systemId);

    // Update using patchSys
    svc.patchSystem(rOwner1, systemId, patchSystemPartial, rawDataPatch2);
    TSystem tmpSysPartial = svc.getSystem(rOwner1, systemId, null, false, false, false);

    // Update original definition with patched values so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setDtnMountPoint(dtnMountPoint2);
    sys0.setMpiCmd(mpiCmd2);
    sys0.setJobMaxJobsPerUser(jobMaxJobsPerUser2);
    sys0.setJobRuntimes(jobRuntimes2);
    // Check common system attributes:
    checkCommonSysAttrs(sys0, tmpSysPartial);
  }

  // Test changing system owner
  @Test
  public void testChangeSystemOwner() throws Exception
  {
    TSystem sys0 = systems[15];
    sys0.setJobCapabilities(capList1);
    String rawDataCreate = "{\"testChangeOwner\": \"0-create\"}";
    String newOwnerName = testUser2;
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataCreate);
    // Change owner using api
    svc.changeSystemOwner(rOwner1, sys0.getId(), newOwnerName);
    TSystem tmpSys = svc.getSystem(rTestUser2, sys0.getId(), null, false, false, false);
    Assert.assertEquals(tmpSys.getOwner(), newOwnerName);
    // Check expected auxiliary updates have happened
    // New owner should be able to retrieve permissions and have all permissions
    Set<Permission> userPerms = svc.getUserPermissions(rTestUser2, sys0.getId(), newOwnerName);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    for (Permission perm : Permission.values())
    {
      Assert.assertTrue(userPerms.contains(perm));
    }
    // Original owner should no longer have the modify or execute permission
    userPerms = svc.getUserPermissions(rTestUser2, sys0.getId(), owner1);
    Assert.assertFalse(userPerms.contains(Permission.READ));
    Assert.assertFalse(userPerms.contains(Permission.MODIFY));
    Assert.assertFalse(userPerms.contains(Permission.EXECUTE));
    Assert.assertTrue(userPerms.isEmpty());
    // Original owner should not be able to modify system
    try {
      svc.deleteSystem(rOwner1, sys0.getId());
      Assert.fail("Original owner should not have permission to update system after change of ownership. System name: " + sys0.getId() +
              " Old owner: " + rOwner1.getOboUserId() + " New Owner: " + newOwnerName);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
    }
    // Original owner should not be able to read system
    try {
      svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
      Assert.fail("Original owner should not have permission to read system after change of ownership. System name: " + sys0.getId() +
              " Old owner: " + rOwner1.getOboUserId() + " New Owner: " + newOwnerName);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
    }
  }

  // Check that when a system is created variable substitution is correct for:
  //   owner, effectiveUser, bucketName, rootDir, jobWorkingDir
  // And when system is retrieved effectiveUserId is resolved
  @Test
  public void testGetSystemWithVariables() throws Exception
  {
    TSystem sys0 = systems[7];
    sys0.setOwner("${apiUserId}");
    sys0.setEffectiveUserId("${owner}");
    sys0.setBucketName("bucket8-${tenant}-${apiUserId}");
    sys0.setRootDir("/root8/${tenant}");
    sys0.setJobWorkingDir("jobWorkDir8/${owner}/${tenant}/${apiUserId}");
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    String effectiveUserId = owner1;
    String bucketName = "bucket8-" + tenantName + "-" + effectiveUserId;
    String rootDir = "/root8/" + tenantName;
    String jobWorkingDir = "jobWorkDir8/" + owner1 + "/" + tenantName + "/" + effectiveUserId;
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), owner1);
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), effectiveUserId);
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod().name(), sys0.getDefaultAuthnMethod().name());
    Assert.assertEquals(tmpSys.isEnabled(), sys0.isEnabled());
    Assert.assertEquals(tmpSys.getBucketName(), bucketName);
    Assert.assertEquals(tmpSys.getRootDir(), rootDir);
    Assert.assertEquals(tmpSys.getJobWorkingDir(), jobWorkingDir);
    Assert.assertEquals(tmpSys.getPort(), sys0.getPort());
    Assert.assertEquals(tmpSys.isUseProxy(), sys0.isUseProxy());
    Assert.assertEquals(tmpSys.getProxyHost(), sys0.getProxyHost());
    Assert.assertEquals(tmpSys.getProxyPort(), sys0.getProxyPort());
  }

  @Test
  public void testGetSystems() throws Exception
  {
    TSystem sys0 = systems[4];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    List<TSystem> systems = svc.getSystems(rOwner1, searchListNull, limitNone, orderByListNull, skipZero,
                                           startAferEmpty, showDeletedFalse);
    Assert.assertNotNull(systems, "getSystems returned null");
    Assert.assertFalse(systems.isEmpty(), "getSystems returned empty list");
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
    }
  }

  // Check that user only sees systems they are authorized to see.
  //   and same for a service when it is calling with oboUser (i.e. not as itself).
  @Test
  public void testGetSystemsAuth() throws Exception
  {
    // Create 3 systems, 2 of which are owned by testUser4.
    TSystem sys0 = systems[16];
    String sys1Name = sys0.getId();
    sys0.setOwner(rTestUser4.getOboUserId());
    svc.createSystem(rTestUser4, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    sys0 = systems[17];
    String sys2Name = sys0.getId();
    sys0.setOwner(rTestUser4.getOboUserId());
    svc.createSystem(rTestUser4, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    sys0 = systems[18];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // When retrieving systems as testUser4 only 2 should be returned
    List<TSystem> systems = svc.getSystems(rTestUser4, searchListNull, limitNone, orderByListNull, skipZero,
                                           startAferEmpty, showDeletedFalse);
    Assert.assertNotNull(systems, "getSystems returned null");
    Assert.assertFalse(systems.isEmpty(), "getSystems returned empty list");
    System.out.println("Total number of systems retrieved by testuser4: " + systems.size());
    for (TSystem system : systems)
    {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
      Assert.assertTrue(system.getId().equals(sys1Name) || system.getId().equalsIgnoreCase(sys2Name));
    }
    Assert.assertEquals(systems.size(), 2);

    // When retrieving systems as a service with oboUser = testuser4 only 2 should be returned.
    systems = svc.getSystems(rFilesSvcTestUser4, searchListNull, limitNone, orderByListNull, skipZero,
                             startAferEmpty, showDeletedFalse);
    System.out.println("Total number of systems retrieved by Files svc calling with oboUser=testuser4: " + systems.size());
    Assert.assertNotNull(systems, "getSystems returned null");
    Assert.assertFalse(systems.isEmpty(), "getSystems returned empty list");
    for (TSystem system : systems)
    {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
      Assert.assertTrue(system.getId().equals(sys1Name) || system.getId().equalsIgnoreCase(sys2Name));
    }
    Assert.assertEquals(systems.size(), 2);
  }

  // Check enable/disable/delete/undelete as well as isEnabled
  // When resource deleted isEnabled should throw a NotFound exception
  @Test
  public void testEnableDisableDeleteUndelete() throws Exception
  {
    // Create the resource
    TSystem sys0 = systems[21];
    String sysId = sys0.getId();
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Enabled should start off true, then become false and finally true again.
    TSystem tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertTrue(tmpSys.isEnabled());
    Assert.assertTrue(svc.isEnabled(rOwner1, sysId));
    int changeCount = svc.disableSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertFalse(tmpSys.isEnabled());
    Assert.assertFalse(svc.isEnabled(rOwner1, sysId));
    changeCount = svc.enableSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertTrue(tmpSys.isEnabled());
    Assert.assertTrue(svc.isEnabled(rOwner1, sysId));

    // Deleted should start off false, then become true and finally false again.
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertFalse(tmpSys.isDeleted());
    changeCount = svc.deleteSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertNull(tmpSys);
    changeCount = svc.undeleteSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, false);
    Assert.assertFalse(tmpSys.isDeleted());

    // When deleted isEnabled should throw NotFound exception
    svc.deleteSystem(rOwner1, sysId);
    boolean pass = false;
    try { svc.isEnabled(rOwner1, sysId); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);
  }

  @Test
  public void testDelete() throws Exception
  {
    // Create a system with no credentials
    TSystem sys0 = systems[5];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Delete the system
    int changeCount = svc.deleteSystem(rOwner1, sys0.getId());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
    Assert.assertNull(tmpSys, "System without credentials not deleted. System name: " + sys0.getId());

    // Create a system with credentials for owner and another user
    sys0 = systems[23];
    Credential cred0 = new Credential(null, null, null, null, "fakePrivateKey", "fakePublicKey", null, null, null);
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);

    // Delete the system
    changeCount = svc.deleteSystem(rOwner1, sys0.getId());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
    Assert.assertNull(tmpSys, "System with credentials not deleted. System name: " + sys0.getId());
  }

  @Test
  public void testSystemExists() throws Exception
  {
    // If system not there we should get false
    Assert.assertFalse(svc.checkForSystem(rOwner1, systems[6].getId()));
    // After creating system we should get true
    TSystem sys0 = systems[6];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    Assert.assertTrue(svc.checkForSystem(rOwner1, systems[6].getId()));
  }

  // Check that if systems already exists we get an IllegalStateException when attempting to create
  @Test(expectedExceptions = {IllegalStateException.class},  expectedExceptionsMessageRegExp = "^SYSLIB_SYS_EXISTS.*")
  public void testCreateSystemAlreadyExists() throws Exception
  {
    // Create the system
    TSystem sys0 = systems[8];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    Assert.assertTrue(svc.checkForSystem(rOwner1, sys0.getId()));
    // Now attempt to create again, should get IllegalStateException with msg SYSLIB_SYS_EXISTS
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
  }

  // Check that reserved names are honored.
  // Because of endpoints certain IDs should not be allowed: healthcheck, readycheck, search
  @Test
  public void testReservedNames() throws Exception
  {
    TSystem sys0 = systems[20];
    for (String id : TSystem.RESERVED_ID_SET)
    {
      System.out.println("Testing create fail for reserved ID: " + id);
      TSystem tmpSys = IntegrationUtils.makeMinimalSystem(sys0, id);
      System.out.println("  - Created in-memory system object with ID: " + tmpSys.getId());
      try
      {
        svc.createSystem(rOwner1, tmpSys, skipCredCheckTrue, rawDataEmtpyJson);
        Assert.fail("System create call should have thrown an exception when using a reserved ID. Id: " + id);
      } catch (IllegalStateException e)
      {
        Assert.assertTrue(e.getMessage().contains("SYSLIB_CREATE_RESERVED"));
      }
    }
  }

  // Check that if credential contains invalid private key then create/update fails.
  @Test
  public void testInvalidPrivateSshKey() throws Exception
  {
    TSystem sys0 = systems[19];
    sys0.setAuthnCredential(credInvalidPrivateSshKey);
    // Test system create
    try {
      svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
      Assert.fail("System create call should have thrown an exception when private ssh key is invalid");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY1"));
    }

    // Test credential update
    sys0.setAuthnCredential(null);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    try {
      svc.createUserCredential(rOwner1, sys0.getId(), sys0.getOwner(), credInvalidPrivateSshKey, skipCredCheckTrue, rawDataEmtpyJson);
      Assert.fail("Credential update call should have thrown an exception when private ssh key is invalid");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2"));
    }
  }

  // Test that attempting to create a system with invalid attribute combinations fails
  // Note that these checks are in addition to other similar tests such as: testReservedNames, testInvalidPrivateSshKey
  // NOTE: Not all combinations are checked.
  // - If canExec is true then jobWorkingDir must be set and jobRuntimes must have at least one entry.
  // - If isDtn is true then canExec must be false and the following attributes may not be set:
  //       dtnSystemId, dtnMountSourcePath, dtnMountPoint, all job execution related attributes.
  // - If canRunBatch is true
  //     batchScheduler must be specified
  //     batchLogicalQueues must not be empty
  //     batchLogicalDefaultQueue must be set
  //     batchLogicalDefaultQueue must be in the list of queues
  //     If batchLogicalQueues has more then one item then batchDefaultLogicalQueue must be set
  //     batchDefaultLogicalQueue must be in the list of logical queues.
  // - If type is OBJECT_STORE then bucketName must be set, isExec and isDtn must be false.
  // - If systemType is LINUX then rootDir is required.
  // - effectiveUserId is restricted.
  // - If effectiveUserId is dynamic then providing credentials is disallowed
  // - If credential is provided and contains ssh keys then validate them
  // - SchedulerProfile must exist
  @Test
  public void testCreateInvalidMiscFail()
  {
    TSystem sys0 = systems[24];
    // If canExec is true then jobWorkingDir must be set and jobRuntimes must have at least one entry.
    String tmpJobWorkingDir = sys0.getJobWorkingDir();
    sys0.setJobWorkingDir(null);
    boolean pass = false;
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CANEXEC_NO_JOBWORKINGDIR_INPUT"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setJobWorkingDir(tmpJobWorkingDir);
    pass = false;
    sys0.setJobRuntimes(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CANEXEC_NO_JOBRUNTIME_INPUT"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setJobRuntimes(jobRuntimes1);

    // If canRunBatch is true
    //     batchScheduler must be specified
    pass = false;
    sys0.setBatchScheduler(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_ISBATCH_NOSCHED"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setBatchScheduler(batchScheduler1);

    // If systemType is LINUX then rootDir is required.
    pass = false;
    sys0.setRootDir(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_NOROOTDIR"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setRootDir(rootDir1);

    // SchedulerProfile must exist.
    pass = false;
    sys0.setBatchSchedulerProfile(noSuchSchedulerProfile);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_PRF_NO_PROFILE"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setBatchSchedulerProfile(batchSchedulerProfile1);
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    TSystem sys0 = systems[9];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Create user perms for the system
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);
    // Get the system perms for the user and make sure permissions are there
    Set<Permission> userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPermsREADMODIFY.size(), "Incorrect number of perms returned.");
    for (Permission perm: testPermsREADMODIFY) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm.name()); }
    // Remove perms for the user. Should return a change count of 2
    int changeCount = svc.revokeUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }

    // Owner should not be able to update perms. It would be confusing since owner always authorized. Perms not checked.
    try {
      svc.grantUserPermissions(rOwner1, sys0.getId(), sys0.getOwner(), testPermsREAD, rawDataEmtpyJson);
      Assert.fail("Update of perms by owner for owner should have thrown an exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_PERM_OWNER_UPDATE"));
    }
    try {
      svc.revokeUserPermissions(rOwner1, sys0.getId(), sys0.getOwner(), testPermsREAD, rawDataEmtpyJson);
      Assert.fail("Update of perms by owner for owner should have thrown an exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_PERM_OWNER_UPDATE"));
    }

    // Give testuser3 back some perms so we can test revokePerms auth when user is not the owner and is target user
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);

    // Have testuser3 remove their own perms. Should return a change count of 2
    changeCount = svc.revokeUserPermissions(rTestUser3, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions as user - not owner.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }

    // Give testuser3 back some perms so we can test revokePerms auth when user is not the owner and is not target user
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);
    try {
      svc.revokeUserPermissions(rTestUser2, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmtpyJson);
      Assert.fail("Update of perms by non-owner user who is not target user should have thrown an exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_UNAUTH"));
    }
  }

  // Test creating, reading and deleting user credentials for a system
  // Including retrieving credentials with a system when effectiveUserId=apiUserId for a system.
  @Test
  public void testUserCredentials() throws Exception
  {
    // Create a system with effUsr = apiUserId
    TSystem sys0 = systems[10];
    sys0.setEffectiveUserId("${apiUserId}");
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    Credential cred1 = new Credential(null, null, null, "fakePassword1", "fakePrivateKey1", "fakePublicKey1",
            "fakeAccessKey1", "fakeAccessSecret1", "fakeCert1");
    Credential cred3 = new Credential(null, null, null, "fakePassword3", "fakePrivateKey3", "fakePublicKey3",
            "fakeAccessKey3", "fakeAccessSecret3", "fakeCert3");
    Credential cred3a = new Credential(null, null, null, null, null, null, "fakeAccessKey3a", "fakeAccessSecret3a", null);

    // Make the separate calls required to store credentials for each user.
    // In this case for owner1 and testUser3
    svc.createUserCredential(rOwner1, sys0.getId(), owner1, cred1, skipCredCheckTrue, rawDataEmtpyJson);
    svc.createUserCredential(rOwner1, sys0.getId(), testUser3, cred3, skipCredCheckTrue, rawDataEmtpyJson);
    // Get system as owner1 using files service and should get cred for owner1
    TSystem tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), AuthnMethod.PASSWORD, false, true, false);
    Credential cred0 = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + owner1);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null for user: " + owner1);
    Assert.assertEquals(cred0.getPassword(), cred1.getPassword());

    // Get system as testUser3 using files service and should get cred for testUser3
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sys0.getId(), AuthnMethod.PASSWORD, false, true, false);
    cred0 = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getPassword(), cred3.getPassword());

    // Get credentials for testUser3 and validate
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.PASSWORD);
    // Verify credentials
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getPassword(), cred3.getPassword());
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PKI_KEYS);
    Assert.assertEquals(cred0.getPublicKey(), cred3.getPublicKey());
    Assert.assertEquals(cred0.getPrivateKey(), cred3.getPrivateKey());
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred0.getAccessKey(), cred3.getAccessKey());
    Assert.assertEquals(cred0.getAccessSecret(), cred3.getAccessSecret());

    // Delete credentials and verify they were destroyed
    int changeCount = svc.deleteUserCredential(rOwner1, sys0.getId(), owner1);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing credential for user: " + owner1);
    changeCount = svc.deleteUserCredential(rOwner1, sys0.getId(), testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing credential for user: " + testUser3);

    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), owner1, AuthnMethod.PASSWORD);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sys0.getId() + " User name: " + owner1);
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.PASSWORD);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sys0.getId() + " User name: " + testUser3);

    // Attempt to delete again, should return 0 for change count
    changeCount = svc.deleteUserCredential(rOwner1, sys0.getId(), testUser3);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when removing a credential already removed.");

    // Set just ACCESS_KEY only and test
    svc.createUserCredential(rOwner1, sys0.getId(), testUser3, cred3a, skipCredCheckTrue, rawDataEmtpyJson);
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred0.getAccessKey(), cred3a.getAccessKey());
    Assert.assertEquals(cred0.getAccessSecret(), cred3a.getAccessSecret());
    // Attempt to retrieve secret that has not been set
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertNull(cred0, "Credential was non-null for missing secret. System name: " + sys0.getId() + " User name: " + testUser3);
    // Delete credentials and verify they were destroyed
    changeCount = svc.deleteUserCredential(rOwner1, sys0.getId(), testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sys0.getId() + " User name: " + testUser3);
  }

  // Test various cases when system is missing
  //  - get system
  //  - isEnabled
  //  - get owner with no system
  //  - get perm with no system
  //  - grant perm with no system
  //  - revoke perm with no system
  //  - get credential with no system
  //  - create credential with no system
  //  - delete credential with no system
  @Test
  public void testMissingSystem() throws Exception
  {
    String fakeSystemName = "AMissingSystemName";
    String fakeUserName = "AMissingUserName";
    int changeCount;
    boolean pass;
    // Make sure system does not exist
    Assert.assertFalse(svc.checkForSystem(rOwner1, fakeSystemName, true));

    // Get TSystem with no system should return null
    TSystem tmpSys = svc.getSystem(rOwner1, fakeSystemName, null, false, false, false);
    Assert.assertNull(tmpSys, "TSystem not null for non-existent system");

    // Delete system with no system should throw a NotFound exception
    pass = false;
    try { svc.deleteSystem(rOwner1, fakeSystemName); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);

    // isEnabled check with no resource should throw a NotFound exception
    pass = false;
    try { svc.isEnabled(rOwner1, fakeSystemName); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);

    // Get owner with no system should return null
    String owner = svc.getSystemOwner(rOwner1, fakeSystemName);
    Assert.assertNull(owner, "Owner not null for non-existent system.");

    // Get perms with no system should return null
    Set<Permission> perms = svc.getUserPermissions(rOwner1, fakeSystemName, fakeUserName);
    Assert.assertNull(perms, "Perms list was not null for non-existent system");

    // Revoke perm with no system should return 0 changes
    changeCount = svc.revokeUserPermissions(rOwner1, fakeSystemName, fakeUserName, testPermsREADMODIFY, rawDataEmtpyJson);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when revoking perms for non-existent system.");

    // Grant perm with no system should throw an exception
    pass = false;
    try { svc.grantUserPermissions(rOwner1, fakeSystemName, fakeUserName, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);

    //Get credential with no system should return null
    Credential cred = svc.getUserCredential(rOwner1, fakeSystemName, fakeUserName, AuthnMethod.PKI_KEYS);
    Assert.assertNull(cred, "Credential was not null for non-existent system");

    // Create credential with no system should throw an exception
    pass = false;
    cred = new Credential(null, null, null, null, null, null, null,"fakeAccessKey2", "fakeAccessSecret2");
    try { svc.createUserCredential(rOwner1, fakeSystemName, fakeUserName, cred, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);

    // Delete credential with no system should 0 changes
    changeCount = svc.deleteUserCredential(rOwner1, fakeSystemName, fakeUserName);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when deleting a user credential for non-existent system.");
  }

  // Test Auth denials
  // testUser0 - no perms, not owner
  // testUser3 - READ perm
  // testUser2 - MODIFY perm
  // NOTE: owner1 is owner - all perms
  @Test
  public void testAuthDeny() throws Exception
  {
    // NOTE: By default seed data has owner as owner1 == "owner1"
    TSystem sys0 = systems[12];
    String systemId = sys0.getId();
    PatchSystem patchSys = new PatchSystem("description PATCHED", "hostPATCHED", "effUserPATCHED",
            prot2.getAuthnMethod(), prot2.getPort(), prot2.isUseProxy(), prot2.getProxyHost(), prot2.getProxyPort(),
            dtnSystemFakeHostname, dtnMountPoint1, dtnMountSourcePath1, jobRuntimes1, jobWorkingDir1, jobEnvVariables1, jobMaxJobs1,
            jobMaxJobsPerUser1, canRunBatchTrue, mpiCmd1, batchScheduler1, logicalQueueList1,
            batchDefaultLogicalQueue1, batchSchedulerProfile1, capList2, tags2, notes2, importRefId2);
    // CREATE - Deny user not owner/admin, deny service
    boolean pass = false;
    try { svc.createSystem(rTestUser0, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createSystem(rFilesSvcOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Create system for remaining auth access tests
    Credential cred0 = new Credential(null, null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Grant testUesr3 - READ and testUser2 - MODIFY
    svc.grantUserPermissions(rOwner1, systemId, testUser3, testPermsREAD, rawDataEmtpyJson);
    svc.grantUserPermissions(rOwner1, systemId, testUser2, testPermsMODIFY, rawDataEmtpyJson);

    // READ - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getSystem(rTestUser0, systemId, null, false, false, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // EXECUTE - deny user not owner/admin with READ but not EXECUTE
    pass = false;
    try { svc.getSystem(rTestUser3, systemId, null, true, false, false); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // MODIFY Deny user with no READ or MODIFY, deny user with only READ, deny service
    pass = false;
    try { svc.patchSystem(rTestUser0, systemId, patchSys, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.patchSystem(rTestUser3, systemId, patchSys, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.patchSystem(rFilesSvcOwner1, systemId, patchSys, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // DELETE - deny user not owner/admin, deny service
    pass = false;
    try { svc.deleteSystem(rTestUser3, systemId); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteSystem(rFilesSvcOwner1, systemId); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // CHANGE_OWNER - deny user not owner/admin, deny service
    pass = false;
    try { svc.changeSystemOwner(rTestUser3, systemId, testUser2); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.changeSystemOwner(rFilesSvcOwner1, systemId, testUser2); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GET_PERMS - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getUserPermissions(rTestUser0, systemId, owner1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GRANT_PERMS - deny user not owner/admin, deny service
    pass = false;
    try { svc.grantUserPermissions(rTestUser3, systemId, testUser0, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.grantUserPermissions(rFilesSvcOwner1, systemId, testUser0, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // REVOKE_PERMS - deny user not owner/admin, deny service
    pass = false;
    try { svc.revokeUserPermissions(rTestUser3, systemId, owner1, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.revokeUserPermissions(rFilesSvcOwner1, systemId, owner1, testPermsREADMODIFY, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // SET_CRED - deny user not owner/admin and not target user, deny service
    pass = false;
    try { svc.createUserCredential(rTestUser3, systemId, owner1, cred0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createUserCredential(rFilesSvcOwner1, systemId, owner1, cred0, skipCredCheckTrue, rawDataEmtpyJson); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // REMOVE_CRED - deny user not owner/admin and not target user, deny service
    pass = false;
    try { svc.deleteUserCredential(rTestUser3, systemId, owner1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteUserCredential(rFilesSvcOwner1, systemId, owner1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GET_CRED - deny user not owner/admin, deny owner - with special message
    pass = false;
    try { svc.getUserCredential(rTestUser3, systemId, owner1, null); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_AUTH_GETCRED"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.getUserCredential(rOwner1, systemId, owner1, null); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_AUTH_GETCRED"));
      pass = true;
    }
    Assert.assertTrue(pass);
  }

  // Test Auth allow
  // Many cases covered during other tests
  // Test special cases here:
  //    MODIFY implies READ
  // testUser0 - no perms
  // testUser3 - READ,EXECUTE perm
  // testUser2 - MODIFY perm
  // NOTE: testUser1 is owner - all perms
  @Test
  public void testAuthAllow() throws Exception
  {
    // NOTE: By default seed data has owner as testUser1
    TSystem sys0 = systems[14];
    // Create system for remaining auth access tests
    Credential cred0 = new Credential(null, null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    // Grant User1 - READ and User2 - MODIFY
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADEXECUTE, rawDataEmtpyJson);
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser2, testPermsMODIFY, rawDataEmtpyJson);

    // READ - allow owner, service, with READ only, with MODIFY only
    svc.getSystem(rOwner1, sys0.getId(), null, false, false, false);
    svc.getSystem(rOwner1, sys0.getId(), null, true, false, false);
    svc.getSystem(rFilesSvcOwner1, sys0.getId(), null, false, false, false);
    svc.getSystem(rTestUser3, sys0.getId(), null, false, false, false);
    svc.getSystem(rTestUser3, sys0.getId(), null, true, false, false);
    svc.getSystem(rTestUser2, sys0.getId(), null, false, false, false);
  }

  // ******************************************************************
  //   Scheduler Profiles
  // ******************************************************************

  @Test
  public void testCreateSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[0];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[1];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());

    SchedulerProfile tmpProfile = svc.getSchedulerProfile(rTestUser2, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
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

    // Anyone should be able to get
    tmpProfile = svc.getSchedulerProfile(rTestUser1, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    tmpProfile = svc.getSchedulerProfile(rTestUser2, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    tmpProfile = svc.getSchedulerProfile(rAdminUser, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    tmpProfile = svc.getSchedulerProfile(rFilesSvcOwner1, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
    tmpProfile = svc.getSchedulerProfile(rFilesSvcTestUser3, p0.getName());
    Assert.assertNotNull(tmpProfile, "Retrieving scheduler profile resulted in null");
    System.out.println("Scheduler Profile retrieved: " + tmpProfile.getName());
  }

  @Test
  public void testDeleteSchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[2];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
    svc.deleteSchedulerProfile(rTestUser2, p0.getName());
    Assert.assertFalse(svc.checkForSchedulerProfile(rTestUser2, p0.getName()),
                       "Scheduler Profile not deleted. Profile name: " + p0.getName());
    System.out.println("Scheduler Profile deleted: " + p0.getName());
  }

  @Test
  public void testGetSchedulerProfiles() throws Exception {
    SchedulerProfile p0 = schedulerProfiles[3];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());
    p0 = schedulerProfiles[4];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());

    List<SchedulerProfile> profiles = svc.getSchedulerProfiles(rTestUser2);
    Assert.assertNotNull(profiles, "getSchedulerProfiles returned null");
    Assert.assertFalse(profiles.isEmpty(), "getSchedulerProfiles returned empty list");
    var profileNamesFound = new HashSet<String>();
    for (SchedulerProfile profile : profiles)
    {
      profileNamesFound.add(profile.getName());
      System.out.println("Found item with name: " + profile.getName());
    }
    Assert.assertTrue(profileNamesFound.contains(schedulerProfiles[3].getName()),
                      "getSchedulerProfiles did not return item with name: " + schedulerProfiles[3].getName());
    Assert.assertTrue(profileNamesFound.contains(schedulerProfiles[4].getName()),
                      "getSchedulerProfiles did not return item with name: " + schedulerProfiles[4].getName());
  }

  // Test Auth denials for Scheduler Profile
  // testUser1 - not owner
  // testUser2 - owner
  @Test
  public void testAuthDenySchedulerProfile() throws Exception
  {
    SchedulerProfile p0 = schedulerProfiles[5];
    SchedulerProfile p1 = schedulerProfiles[6];
    svc.createSchedulerProfile(rTestUser2, p0);
    System.out.println("Scheduler Profile created: " + p0.getName());

    // CREATE - Deny user not owner/admin, deny service
    boolean pass = false;
    try { svc.createSchedulerProfile(rTestUser1, p1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createSchedulerProfile(rFilesSvcOwner1, p1); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // DELETE - deny user not owner/admin, deny service
    pass = false;
    try { svc.deleteSchedulerProfile(rTestUser1, p0.getName()); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteSchedulerProfile(rFilesSvcOwner1, p0.getName()); }
    catch (NotAuthorizedException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check common attributes after creating and retrieving a system
   * @param sys0 - Test system
   * @param tmpSys - Retrieved system
   */
  private static void checkCommonSysAttrs(TSystem sys0, TSystem tmpSys)
  {
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getDescription(), sys0.getDescription());
    Assert.assertEquals(tmpSys.getSystemType().name(), sys0.getSystemType().name());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    Assert.assertEquals(tmpSys.getHost(), sys0.getHost());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getDefaultAuthnMethod().name(), sys0.getDefaultAuthnMethod().name());
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

    Assert.assertEquals(tmpSys.getImportRefId(), sys0.getImportRefId());

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
    String[] origTags = sys0.getTags();
    String[] tmpTags = tmpSys.getTags();
    Assert.assertNotNull(origTags, "Orig Tags should not be null");
    Assert.assertNotNull(tmpTags, "Fetched Tags value should not be null");
    var tagsList = Arrays.asList(tmpTags);
    Assert.assertEquals(tmpTags.length, origTags.length, "Wrong number of tags.");
    for (String tagStr : origTags)
    {
      Assert.assertTrue(tagsList.contains(tagStr));
      System.out.println("Found tag: " + tagStr);
    }
    // Verify notes
    Assert.assertNotNull(sys0.getNotes(), "Orig Notes should not be null");
    Assert.assertNotNull(tmpSys.getNotes(), "Fetched Notes should not be null");
    System.out.println("Found notes: " + sys0.getNotes());
    JsonObject tmpObj = (JsonObject) tmpSys.getNotes();
    JsonObject origNotes = (JsonObject) sys0.getNotes();
    Assert.assertTrue(tmpObj.has("project"));
    String projStr = origNotes.get("project").getAsString();
    Assert.assertEquals(tmpObj.get("project").getAsString(), projStr);
    Assert.assertTrue(tmpObj.has("testdata"));
    String testdataStr = origNotes.get("testdata").getAsString();
    Assert.assertEquals(tmpObj.get("testdata").getAsString(), testdataStr);

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

    // Verify logicalQueues
    List<LogicalQueue> origLogicalQueues = sys0.getBatchLogicalQueues();
    List<LogicalQueue> logicalQueues = tmpSys.getBatchLogicalQueues();
    Assert.assertNotNull(origLogicalQueues, "Orig LogicalQueues was null");
    Assert.assertNotNull(logicalQueues, "Fetched LogicalQueues was null");
    Assert.assertEquals(logicalQueues.size(), origLogicalQueues.size());
    var logicalQueueNamesFound = new ArrayList<String>();
    for (LogicalQueue logicalQueueFound : logicalQueues) {logicalQueueNamesFound.add(logicalQueueFound.getName());}
    for (LogicalQueue logicalQueueSeedItem : origLogicalQueues)
    {
      Assert.assertTrue(logicalQueueNamesFound.contains(logicalQueueSeedItem.getName()),
              "List of logicalQueues did not contain a logicalQueue with name: " + logicalQueueSeedItem.getName());
    }

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

    Assert.assertNotNull(tmpSys.getCreated(), "Fetched created timestamp should not be null");
    Assert.assertNotNull(tmpSys.getUpdated(), "Fetched updated timestamp should not be null");
  }
  
  // Test retrieving a system history
  @Test
  public void testGetSystemHistory() throws Exception
  {
    TSystem sys0 = systems[26];
    Credential cred0 = new Credential(null, null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
        "fakeAccessKey", "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    
    // Test retrieval using specified authn method
    List<SystemHistoryItem> systemHistory = svc.getSystemHistory(rOwner1, sys0.getId());
    
    System.out.println("Found item: " + sys0.getId());
    // Verify system history fields
    Assert.assertEquals(systemHistory.size(), 1);
    for (SystemHistoryItem item:systemHistory)
    {
      Assert.assertNotNull(item.getJwtTenant(), "Fetched API Tenant should not be null");
      Assert.assertNotNull(item.getJwtTenant(), "Fetched API User should not be null");
      Assert.assertNotNull(item.getOboTenant(), "Fetched OBO Tenant should not be null");
      Assert.assertNotNull(item.getOboUser(), "Fetched OBO User should not be null");
      Assert.assertEquals(item.getOperation(), SystemOperation.create);
      Assert.assertNotNull(item.getDescription(), "Fetched Json should not be null");
      Assert.assertNotNull(item.getCreated(), "Fetched created timestamp should not be null");
    }
  }
  // Test system history when there are multiple updates
  @Test
  public void testGetSystemHistoryMultipleUpdates() throws Exception
  {
    TSystem sys0 = systems[27];
    String sysId = sys0.getId();
    ResourceRequestUser ownerUser = rTestUser2;
    Credential cred0 = new Credential(null, null, null, "fakePassword", "fakePrivateKey", "fakePublicKey", "fakeAccessKey",
                                      "fakeAccessSecret", "fakeCert");
    sys0.setAuthnCredential(cred0);
    sys0.setOwner(testUser2);
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    // Create systems - history record 1
    svc.createSystem(ownerUser, sys0, skipCredCheckTrue, rawDataEmtpyJson);
    TSystem tmpSys = svc.getSystem(ownerUser, sysId, null, false, false, false);
    // Get last updated timestamp
    LocalDateTime updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr1 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Pause briefly to make sure updated timestamp changes for patch
    Thread.sleep(300);
    // Disable - record 2
    svc.disableSystem(ownerUser, sysId);
    // Enable - record 3
    svc.enableSystem(ownerUser, sysId);
    // Patch - record 4
    // Create patchSystem where all updatable attributes are changed
    String rawDataPatch = "{\"testUpdate\": \"1-patch1\"}";
    PatchSystem patchSystemFull = IntegrationUtils.makePatchSystemFull(testKey, sysId);
    svc.patchSystem(ownerUser, sysId, patchSystemFull, rawDataPatch);
    // Put - record 5
    String rawDataPut = "{\"testPut\": \"1-put1\"}";
    TSystem putSystem = IntegrationUtils.makePutSystemFull(testKey, tmpSys);
    // Update a few values to make sure we have changes
    putSystem.setDescription(description3);
    putSystem.setJobMaxJobs(jobMaxJobs3);
    putSystem.setTags(tags3);
    putSystem.setJobEnvVariables(jobEnvVariables3);
    svc.putSystem(ownerUser, putSystem, skipCredCheckTrue, rawDataPut);
    // Delete - record 6
    svc.deleteSystem(ownerUser, sysId);

    // Test retrieval of history
    List<SystemHistoryItem> systemHistory = svc.getSystemHistory(ownerUser, sysId);
    Assert.assertNotNull(systemHistory);

    System.out.println("Found item: " + sysId);
    // Verify system history fields
    System.out.printf("History system: %s Number of history records: %d%n", sysId, systemHistory.size());
    for (int i=0; i < systemHistory.size(); i++)
    {
      System.out.println("-------------------------------------------");
      System.out.printf("Record # %d%n", i+1);
      System.out.println("-------------------------------------------");
      SystemHistoryItem item = systemHistory.get(i);
      System.out.printf("apiTenant: %s%n", item.getJwtTenant());
      System.out.printf("apiUser: %s%n", item.getJwtUser());
      System.out.printf("oboTenant: %s%n", item.getOboTenant());
      System.out.printf("oboUser: %s%n", item.getOboUser());
      System.out.printf("operation: %s%n", item.getOperation());
      System.out.printf("created: %s%n", item.getCreated());
      System.out.printf("Description:%n%s%n", item.getDescription());
    }
    svc.undeleteSystem(ownerUser, sysId);
    Assert.assertEquals(systemHistory.size(), 6);
  }
}
