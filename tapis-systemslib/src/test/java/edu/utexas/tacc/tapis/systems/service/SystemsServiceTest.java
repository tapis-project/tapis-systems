package edu.utexas.tacc.tapis.systems.service;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
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
import edu.utexas.tacc.tapis.systems.model.ModuleLoadSpec;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.SystemShare;

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

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
  private ResourceRequestUser rOwner1, rOwner3, rOwner4, rOwner5, rOwner6,
          rTestUser0, rTestUser1, rTestUser2, rTestUser3, rTestUser4, rTestUser5,
          rParentChild1, rParentChild2, rParentChild3,
          rAdminUser, rSystemsSvc, rAppsSvcTestUser1, rFilesSvcAsFiles,
          rFilesSvcOwner1, rFilesSvcTestUser3, rFilesSvcTestUser4, rFilesSvcTestUser5,
          rJobsSvcTestUser1, rJobsSvcOwner1;

  // Create test system definitions and scheduler profiles in memory
  String testKey = "Svc";
  int numSystems = 35; // UNUSED SYSTEMS: None
  int numSchedulerProfiles = 7;
  TSystem dtnSystem1 = IntegrationUtils.makeDtnSystem1(testKey);
  TSystem dtnSystem2 = IntegrationUtils.makeDtnSystem2(testKey);
  TSystem s3System1 = IntegrationUtils.makeS3System(testKey);
  TSystem[] systems = IntegrationUtils.makeSystems(numSystems, testKey);

  // used for cleanup
  private static int MAX_PARENT_SYSTEMS=13;
  List<TSystem> parentSystems = new ArrayList<>();
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
    rOwner3 = new ResourceRequestUser(new AuthenticatedUser(owner3, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner3, tenantName, null, null, null));
    rOwner4 = new ResourceRequestUser(new AuthenticatedUser(owner4, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner4, tenantName, null, null, null));
    rOwner5 = new ResourceRequestUser(new AuthenticatedUser(owner5, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner5, tenantName, null, null, null));
    rOwner6 = new ResourceRequestUser(new AuthenticatedUser(owner6, tenantName, TapisThreadContext.AccountType.user.name(),
                                                            null, owner6, tenantName, null, null, null));
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
    rTestUser5 = new ResourceRequestUser(new AuthenticatedUser(testUser5, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, testUser5, tenantName, null, null, null));
    rParentChild1 = new ResourceRequestUser(new AuthenticatedUser(parentChild1, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, parentChild1, tenantName, null, null, null));
    rParentChild2 = new ResourceRequestUser(new AuthenticatedUser(parentChild2, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, parentChild2, tenantName, null, null, null));
    rParentChild3 = new ResourceRequestUser(new AuthenticatedUser(parentChild3, tenantName, TapisThreadContext.AccountType.user.name(),
                                                   null, parentChild3, tenantName, null, null, null));
    rSystemsSvc = new ResourceRequestUser(new AuthenticatedUser(svcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                    null, svcName, adminTenantName, null, null, null));
    rFilesSvcAsFiles = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                    null, filesSvcName, adminTenantName, null, null, null));
    rFilesSvcOwner1 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, owner1, tenantName, null, null, null));
    rFilesSvcTestUser3 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, testUser3, tenantName, null, null, null));
    rFilesSvcTestUser4 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, testUser4, tenantName, null, null, null));
    rFilesSvcTestUser5 = new ResourceRequestUser(new AuthenticatedUser(filesSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                   null, testUser5, tenantName, null, null, null));
    rJobsSvcTestUser1 = new ResourceRequestUser(new AuthenticatedUser(jobsSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                null, testUser1, tenantName, null, null, null));
    rJobsSvcOwner1 = new ResourceRequestUser(new AuthenticatedUser(jobsSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                             null, owner1, tenantName, null, null, null));
    rAppsSvcTestUser1 = new ResourceRequestUser(new AuthenticatedUser(appsSvcName, adminTenantName, TapisThreadContext.AccountType.service.name(),
                                                null, testUser1, tenantName, null, null, null));

    // Cleanup anything leftover from previous failed run
    tearDown();

    // Create DTN systems for other systems to reference. Otherwise, some system definitions are not valid.
    svc.createSystem(rOwner1, dtnSystem1, skipCredCheckTrue, rawDataEmptyJson);
    svc.createSystem(rOwner1, dtnSystem2, skipCredCheckTrue, rawDataEmptyJson);

    // Create a scheduler profile for systems to reference
    System.out.println("Creating scheduler profile with name: " + batchSchedulerProfile1);
    List<SchedulerProfile.HiddenOption> hiddenOptions = Arrays.asList(SchedulerProfile.HiddenOption.MEM);
    String moduleLoadCmd = "modload1";
    String[] modulesToLoad = {"value1"};
    List<ModuleLoadSpec> moduleLoads = List.of(new ModuleLoadSpec(moduleLoadCmd, modulesToLoad));
    SchedulerProfile sp = new SchedulerProfile(tenantName, batchSchedulerProfile1, "test profile1",  owner1,
                                               moduleLoads, hiddenOptions, null, null, null);
    svc.createSchedulerProfile(rOwner1, sp);
    System.out.println("Creating scheduler profile with name: " + batchSchedulerProfile2);
    sp = new SchedulerProfile(tenantName, batchSchedulerProfile2, "test profile2",  owner1, moduleLoads, hiddenOptions,
                              null, null, null);
    svc.createSchedulerProfile(rOwner1, sp);
  }

  @AfterSuite
  public void tearDown() throws Exception
  {
    System.out.println("Executing AfterSuite teardown for " + SystemsServiceTest.class.getSimpleName());
    // Remove non-owner permissions granted during the tests
    try { svc.revokeUserPermissions(rOwner1, systems[9].getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[12].getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[12].getId(), testUser2, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[14].getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (Exception e) { }
    try { svc.revokeUserPermissions(rOwner1, systems[14].getId(), testUser2, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (Exception e) { }

    // Remove all objects created by tests
    for (int i = 0; i < numSystems; i++)
    {
      svcImpl.hardDeleteSystem(rAdminUser, tenantName, systems[i].getId());
    }

    // if this assertion fails, it means that tests have been added that create more parent systems.  We need to
    // know what the max number is so that when this method is called at the begining of the test, we know how many
    // parent/child systems to look for to clean up.  At the end of the tests we always know which ones to clean up.
    // Just make MAX_PARENT_SYSTEMS larger - it's not a "real" error.
    Assert.assertTrue(parentSystems.size() <= MAX_PARENT_SYSTEMS);
    List<String> parentIds = new ArrayList<>();
    if(parentSystems.size() == 0) {
      for(int i=0;i < MAX_PARENT_SYSTEMS; i++) {
        parentIds.add(getParentSysName(i));
      }
    } else {
      for(TSystem parentSystem : parentSystems) {
        parentIds.add(parentSystem.getId());
      }
    }
    SystemsDaoImpl dao = new SystemsDaoImpl();
    for(String pSystemId : parentIds) {
      List<TSystem> childSystems = dao.getSystems(rAdminUser, Arrays.asList("parentId.eq." + pSystemId),
              null, -1, null, 0, null, true, null, null, null);
      for(TSystem cSystem : childSystems) {
        svcImpl.hardDeleteSystem(rAdminUser, tenantName, cSystem.getId());
      }
      svcImpl.hardDeleteSystem(rAdminUser, tenantName, pSystemId);
    }

    svcImpl.hardDeleteSystem(rAdminUser, tenantName, s3System1.getId());
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
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
  }

  // Create a system using minimal attributes:
  //   id, systemType, host, defaultAuthnMethod, canExec
  @Test
  public void testCreateSystemMinimal() throws Exception
  {
    TSystem sys0 = makeMinimalSystem(systems[11], null);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
  }

  // Test credential verification for linux
  @Test
  public void testCredCheckLinux() throws Exception
  {
    TSystem sys0 = systems[34];
    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR);
    sys0.setDefaultAuthnMethod(AuthnMethod.PASSWORD);
    sys0.setHost(TAPIS_TEST_HOST_IP);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false,
                      null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());

    String targetUser = owner1;
    String loginUser = TAPIS_TEST_HOST_LOGIN_USER;
    // Get password from environment - TAPIS_VM_TESTUSER3_PASSWORD
    String testUser3P = System.getenv(TAPIS_TEST_PASSWORD_ENV_VAR);
    if (StringUtils.isBlank(testUser3P))
    {
      Assert.fail("Missing environment variable. Please set env var: " + TAPIS_TEST_PASSWORD_ENV_VAR);
    }
    Credential credFake = new Credential(AuthnMethod.PASSWORD, loginUser, "fakePassword", null, null, null, null, null, null, null);

    // Cleanup any previous credentials
    svc.deleteUserCredential(rOwner1, sys0.getId(), targetUser);

    // Test create with invalid credentials
    Credential checkedCred = svc.createUserCredential(rOwner1, sys0.getId(), targetUser, credFake, skipCredCheckFalse, rawDataEmptyJson);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.FALSE);

    // Using valid credentials should succeed.
    Credential credGood = new Credential(null, loginUser, testUser3P, null, null, null, null, null, null, null);
    sys0.setAuthnCredential(credGood);

    // Test create and check with valid credentials
    checkedCred = svc.createUserCredential(rOwner1, sys0.getId(), targetUser, credGood, skipCredCheckFalse, rawDataEmptyJson);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);
    checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, null);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);
    checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, AuthnMethod.PASSWORD);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);

    // Negative tests
    // Check with different authnMethod. Should throw NotAuthorized
    boolean pass = false;
    try
    {
      checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, AuthnMethod.PKI_KEYS);
      Assert.fail("System checkUserCredential call should have thrown an exception when credentials do not exist");
    }
    catch (Exception e)
    {
      String msg = e.getMessage();
      Assert.assertTrue(msg.contains("SYSLIB_CRED_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Check that check for non-existent user with no credentials fails.
    pass = false;
    try
    {
      checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), "testuser_99999999_no_such_user", null);
      Assert.fail("System checkUserCredential call should have thrown an exception when user and credentials do not exist");
    }
    catch (Exception e)
    {
      String msg = e.getMessage();
      Assert.assertTrue(msg.contains("SYSLIB_CRED_NOT_FOUND"));
      pass = true;
    }
    Assert.assertTrue(pass);
  }

  // Test credential verification for S3 - local ceph server
  @Test
  public void testCredCheckS3() throws Exception
  {
    TSystem sys0 = s3System1;
    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR);
    sys0.setDefaultAuthnMethod(AuthnMethod.ACCESS_KEY);
    sys0.setHost(TAPIS_TEST_S3_HOST);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
    System.out.println("Found item: " + sys0.getId());

    String targetUser = owner1;
    String loginUser = TAPIS_TEST_S3_HOST_LOGIN_USER;
    // Get cred from environment -
    String testS3Key = System.getenv(TAPIS_TEST_S3_KEY_ENV_VAR);
    String testS3Secret = System.getenv(TAPIS_TEST_S3_SECRET_ENV_VAR);
    if (StringUtils.isBlank(testS3Key) || StringUtils.isBlank(testS3Secret))
    {
      Assert.fail("Missing cred environment variable. Please set env variables: " + TAPIS_TEST_S3_KEY_ENV_VAR + " and " + TAPIS_TEST_S3_SECRET_ENV_VAR);
    }
    Credential credFake = new Credential(AuthnMethod.ACCESS_KEY, loginUser, null, null, null, "fakeAccessKey", "fakeAccessSecret", null, null, null);

    // Cleanup any previous credentials
    svc.deleteUserCredential(rOwner1, sys0.getId(), targetUser);

    // Test create with invalid credentials
    Credential checkedCred = svc.createUserCredential(rOwner1, sys0.getId(), targetUser, credFake, skipCredCheckFalse, rawDataEmptyJson);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.FALSE);

    // Using valid credentials should succeed.
    Credential credGood = new Credential(null, loginUser, null, null, null, testS3Key, testS3Secret, null, null, null);
    sys0.setAuthnCredential(credGood);

    // Test create and check with valid credentials
    checkedCred = svc.createUserCredential(rOwner1, sys0.getId(), targetUser, credGood, skipCredCheckFalse, rawDataEmptyJson);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);
    checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, null);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);
    checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(checkedCred.getValidationResult(), Boolean.TRUE);

    // Check with different authnMethod. Should throw NotAuthorized
    try
    {
      checkedCred = svc.checkUserCredential(rOwner1, sys0.getId(), targetUser, AuthnMethod.PASSWORD);
      Assert.fail("System checkUserCredential call should have thrown an exception when credentials do not exist");
    }
    catch (Exception e)
    {
      String msg = e.getMessage();
      Assert.assertTrue(msg.contains("SYSLIB_CRED_NOT_FOUND"));
    }
  }

  // Test retrieving a system including default authn method
  //   and test retrieving for specified authn method.
  @Test
  public void testGetSystem() throws Exception
  {
    TSystem sys0 = systems[1];
    sys0.setJobCapabilities(capList1);
    Credential cred0 = new Credential(null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeAccessToken", "fakeRefreshToken", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Retrieve system as owner, without and with requireExecPerm
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false,
                      null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCommonSysAttrs(sys0, tmpSys);
    checkEnvVarDefaults(tmpSys);
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, true, false, null,
                           sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCommonSysAttrs(sys0, tmpSys);
    // Retrieve the system including the credential using the default authn method defined for the system
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), null, false, true,
              null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), AuthnMethod.PASSWORD, false, true,
              null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    TSystem tmpSys = svc.getSystem(rOwner1, systemId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    // Get last updated timestamp
    LocalDateTime updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr1 = TapisUtils.getSQLStringFromUTCTime(updated);
    Thread.sleep(300);

    // Create putSystem where all updatable attributes are changed
    String rawDataPut = "{\"testPut\": \"1-put1\"}";
    TSystem putSystem = IntegrationUtils.makePutSystemFull(testKey, tmpSys);

    // Update using PUT
    svc.putSystem(rOwner1, putSystem, skipCredCheckTrue, rawDataPut);
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);

    // Get last updated timestamp
    updated = LocalDateTime.ofInstant(tmpSys.getUpdated(), ZoneOffset.UTC);
    String updatedStr2 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Make sure update timestamp has been modified
    System.out.println("Updated timestamp before: " + updatedStr1 + " after: " + updatedStr2);
    Assert.assertNotEquals(updatedStr1, updatedStr2, "Update timestamp was not updated. Both are: " + updatedStr1);

    // Update original definition with modified values, so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setHost(hostname2);
    sys0.setEffectiveUserId(effectiveUserId2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setPort(prot2.getPort());
    sys0.setUseProxy(prot2.isUseProxy());
    sys0.setProxyHost(prot2.getProxyHost());
    sys0.setProxyPort(prot2.getProxyPort());
    sys0.setDtnSystemId(sysNamePrefix + testKey + dtnSystemId2);
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
    // Retrieve and display history for manual checking of system history records
    displaySystemHistory(rOwner1, sys0.getId());
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
    TSystem tmpSys = svc.getSystem(rOwner1, systemId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    TSystem tmpSysFull = svc.getSystem(rOwner1, systemId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);

    // Get last updated timestamp
    updated = LocalDateTime.ofInstant(tmpSysFull.getUpdated(), ZoneOffset.UTC);
    String updatedStr2 = TapisUtils.getSQLStringFromUTCTime(updated);
    // Make sure update timestamp has been modified
    System.out.println("Updated timestamp before: " + updatedStr1 + " after: " + updatedStr2);
    Assert.assertNotEquals(updatedStr1, updatedStr2, "Update timestamp was not updated. Both are: " + updatedStr1);

    // Update original definition with patched values, so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setHost(hostname2);
    sys0.setEffectiveUserId(effectiveUserId2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setPort(prot2.getPort());
    sys0.setUseProxy(prot2.isUseProxy());
    sys0.setProxyHost(prot2.getProxyHost());
    sys0.setProxyPort(prot2.getProxyPort());
    sys0.setDtnSystemId(sysNamePrefix+ testKey +dtnSystemId2);
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

    // Retrieve and display history for manual checking of system history records
    displaySystemHistory(rOwner1, systemId);

    // Test updating just a few attributes
    sys0 = systems[22];
    systemId = sys0.getId();
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    rawDataCreate = "{\"testUpdate\": \"0-create2\"}";
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataCreate);
    // Create patchSystem where some attributes are changed
    //   * Some attributes are to be updated: description, authnMethod, runtimeList, jobMaxJobsPerUser
    String rawDataPatch2 = "{\"testUpdate\": \"1-patch2\"}";
    PatchSystem patchSystemPartial = IntegrationUtils.makePatchSystemPartial(testKey, systemId);

    // Update using patchSys
    svc.patchSystem(rOwner1, systemId, patchSystemPartial, rawDataPatch2);
    TSystem tmpSysPartial = svc.getSystem(rOwner1, systemId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);

    // Update original definition with patched values, so we can use the checkCommon method.
    sys0.setDescription(description2);
    sys0.setDefaultAuthnMethod(prot2.getAuthnMethod());
    sys0.setMpiCmd(mpiCmd2);
    sys0.setJobMaxJobsPerUser(jobMaxJobsPerUser2);
    sys0.setJobRuntimes(jobRuntimes2);
    // Check common system attributes:
    checkCommonSysAttrs(sys0, tmpSysPartial);
    // Retrieve and display history for manual checking of system history records
    displaySystemHistory(rOwner1, systemId);
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
    TSystem tmpSys = svc.getSystem(rTestUser2, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertEquals(tmpSys.getOwner(), newOwnerName);
    // Check expected auxiliary updates have happened
    // New owner should be able to retrieve permissions
    Set<Permission> userPerms = svc.getUserPermissions(rTestUser2, sys0.getId(), newOwnerName);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
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
      svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    sys0.setOwner(TSystem.APIUSERID_VAR);
    sys0.setEffectiveUserId("${owner}");
    sys0.setBucketName("bucket8-${tenant}-${apiUserId}");
    sys0.setRootDir("/root8/${tenant}");
    sys0.setJobWorkingDir("jobWorkDir8/${owner}/${tenant}/${apiUserId}");
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    // Set effectiveUserId to dynamic user and verify effUsr is resolved
    tmpSys.setEffectiveUserId(TSystem.APIUSERID_VAR);
    svc.putSystem(rOwner1, tmpSys, true, rawDataEmptyJson);
    // Get with resolve
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNotNull(tmpSys, "Failed to get item: " + sys0.getId());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), rOwner1.getJwtUserId());
  }

//  // Check resolving of dynamic rootDir
//  @Test
//  public void testGetSystemResolveRootDir() throws Exception
//  {
//    String rootDir = "HOST_EVAL($HOME)/test/${effectiveUserId}";
//    TSystem sys0 = systems[33];
//    sys0.setHost(TAPIS_TEST_HOST_IP);
//    sys0.setRootDir(rootDir);
//    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR);
//    sys0.setDefaultAuthnMethod(AuthnMethod.PASSWORD);
//    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
//
//    // Fetch system without resolving rootDir. Returned rootDir should match original.
//    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
//    Assert.assertNotNull(tmpSys, "Failed to create item: " + sys0.getId());
//    System.out.println("Found item: " + sys0.getId());
//    checkCommonSysAttrs(sys0, tmpSys);
//
//    // Set up for evaluating HOST_EVAL. Tapis will need to ssh to the host.
//    String targetUser = owner1;
//    String loginUser = TAPIS_TEST_HOST_LOGIN_USER;
//    // Get password from environment - TAPIS_VM_TESTUSER3_PASSWORD
//    String testUser3P = System.getenv(TAPIS_TEST_PASSWORD_ENV_VAR);
//    if (StringUtils.isBlank(testUser3P))
//    {
//      Assert.fail("Missing environment variable. Please set env var: " + TAPIS_TEST_PASSWORD_ENV_VAR);
//    }
//    Credential cred0 = new Credential(null, loginUser, testUser3P, null, null, null, null, null);
//    sys0.setAuthnCredential(cred0);
//    svc.createUserCredential(rOwner1, sys0.getId(), targetUser, cred0, skipCredCheckFalse, rawDataEmptyJson);
//
//    // Fetch system with resolving rootDir. Returned rootDir should have been updated.
//    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, resolveTypeALL, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
//    // Update original definition, so we can use the checkCommon method.
//    String resolvedRootDir = String.format("/home/%s/test/%s", loginUser, loginUser);
//    sys0.setRootDir(resolvedRootDir);
//    sys0.setEffectiveUserId(loginUser);
//    checkCommonSysAttrs(sys0, tmpSys);
//    // Reset rootDir
//    sys0.setRootDir(rootDir);
//  }

  @Test
  public void testGetSystems() throws Exception
  {
    TSystem sys0 = systems[4];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    List<TSystem> systems = svc.getSystems(rOwner1, searchListNull, limitNone, orderByListNull, skipZero,
                                           startAferEmpty, showDeletedFalse, listTypeNull, fetchShareInfoFalse);
    Assert.assertNotNull(systems, "getSystems returned null");
    Assert.assertFalse(systems.isEmpty(), "getSystems returned empty list");
    for (TSystem system : systems) {
      System.out.println("Found item with id: " + system.getId() + " and name: " + system.getId());
    }
  }

  // Test getSystems using listType parameter
  @Test
  public void testGetSystemsByListType() throws Exception
  {
    var sharedIDs = new HashSet<String>();
    // Create 4 systems.
    // One owned by owner3
    // One owned by owner4 with READ permission granted to owner3
    // One owned by owner5 and shared with owner3
    // One owned by owner6 and shared publicly
    TSystem sys0;
    sys0 = systems[3]; sys0.setOwner(owner3); svc.createSystem(rOwner3, sys0, skipCredCheckTrue, rawDataEmptyJson);
    sys0 = systems[30]; sys0.setOwner(owner4); svc.createSystem(rOwner4, sys0, skipCredCheckTrue, rawDataEmptyJson);
    sys0 = systems[31]; sys0.setOwner(owner5); svc.createSystem(rOwner5, sys0, skipCredCheckTrue, rawDataEmptyJson);
    sharedIDs.add(sys0.getId());
    sys0 = systems[32]; sys0.setOwner(owner6); svc.createSystem(rOwner6, sys0, skipCredCheckTrue, rawDataEmptyJson);

    // owner4 grants READ permission to owner3
    svc.grantUserPermissions(rOwner4, systems[30].getId(), owner3, testPermsREAD, rawDataEmptyJson);
    // owner5 shares with owner3
    String rawDataShare = "{\"users\": [\"" + owner3 + "\"]}";
    SystemShare systemShare = TapisGsonUtils.getGson().fromJson(rawDataShare, SystemShare.class);
    svc.shareSystem(rOwner5, systems[31].getId(), systemShare);
    // owner6 makes system public
    svc.shareSystemPublicly(rOwner6, systems[32].getId());

    List<TSystem> systems;
    // OWNED - should return 1
    systems = svc.getSystems(rOwner3, searchListNull, limitNone, orderByListNull, skipZero, startAferEmpty,
            showDeletedFalse, listTypeOwned.name(), fetchShareInfoFalse);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypeOwned);
    Assert.assertEquals(systems.size(), 1, "Wrong number of returned systems for listType=" + listTypeOwned);
    // PUBLIC - should return 1
    systems = svc.getSystems(rOwner3, searchListNull, limitNone, orderByListNull, skipZero, startAferEmpty,
            showDeletedFalse, listTypePublic.name(), fetchShareInfoFalse);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypePublic);
    Assert.assertEquals(systems.size(), 1, "Wrong number of returned systems for listType=" + listTypePublic);
    // ALL - should return 4
    systems = svc.getSystems(rOwner3, searchListNull, limitNone, orderByListNull, skipZero, startAferEmpty,
            showDeletedFalse, listTypeAll.name(), fetchShareInfoFalse);
    Assert.assertNotNull(systems, "Returned list of systems should not be null");
    System.out.printf("getSystems returned %d items using listType = %s%n", systems.size(), listTypeAll);
    Assert.assertEquals(systems.size(), 4, "Wrong number of returned systems for listType=" + listTypeAll);
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
    svc.createSystem(rTestUser4, sys0, skipCredCheckTrue, rawDataEmptyJson);
    sys0 = systems[17];
    String sys2Name = sys0.getId();
    sys0.setOwner(rTestUser4.getOboUserId());
    svc.createSystem(rTestUser4, sys0, skipCredCheckTrue, rawDataEmptyJson);
    sys0 = systems[18];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // When retrieving systems as testUser4 only 2 should be returned
    List<TSystem> systems = svc.getSystems(rTestUser4, searchListNull, limitNone, orderByListNull, skipZero,
                                           startAferEmpty, showDeletedFalse, listTypeNull, fetchShareInfoFalse);
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
                             startAferEmpty, showDeletedFalse, listTypeNull, fetchShareInfoFalse);
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
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Enabled should start off true, then become false and finally true again.
    TSystem tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertTrue(tmpSys.isEnabled());
    Assert.assertTrue(svc.isEnabled(rOwner1, sysId));
    int changeCount = svc.disableSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertFalse(tmpSys.isEnabled());
    Assert.assertFalse(svc.isEnabled(rOwner1, sysId));
    changeCount = svc.enableSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertTrue(tmpSys.isEnabled());
    Assert.assertTrue(svc.isEnabled(rOwner1, sysId));

    // Deleted should start off false, then become true and finally false again.
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertFalse(tmpSys.isDeleted());
    changeCount = svc.deleteSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNull(tmpSys);
    changeCount = svc.undeleteSystem(rOwner1, sysId);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when updating the system.");
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Delete the system
    int changeCount = svc.deleteSystem(rOwner1, sys0.getId());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNull(tmpSys, "System without credentials not deleted. System name: " + sys0.getId());

    // Create a system with credentials for owner and another user
    sys0 = systems[23];
    Credential cred0 = new Credential(null, null, null, "fakePrivateKey", "fakePublicKey", null, null, null, null, null);
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);

    // Delete the system
    changeCount = svc.deleteSystem(rOwner1, sys0.getId());
    Assert.assertEquals(changeCount, 1, "Change count incorrect when deleting a system.");
    tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertNull(tmpSys, "System with credentials not deleted. System name: " + sys0.getId());
  }

  @Test
  public void testSystemExists() throws Exception
  {
    // If system not there we should get false
    Assert.assertFalse(svc.checkForSystem(rOwner1, systems[6].getId()));
    // After creating system we should get true
    TSystem sys0 = systems[6];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(svc.checkForSystem(rOwner1, systems[6].getId()));
  }

  // Check that if systems already exists we get an IllegalStateException when attempting to create
  @Test(expectedExceptions = {IllegalStateException.class},  expectedExceptionsMessageRegExp = "^SYSLIB_SYS_EXISTS.*")
  public void testCreateSystemAlreadyExists() throws Exception
  {
    // Create the system
    TSystem sys0 = systems[8];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(svc.checkForSystem(rOwner1, sys0.getId()));
    // Now attempt to create again, should get IllegalStateException with msg SYSLIB_SYS_EXISTS
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
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
        svc.createSystem(rOwner1, tmpSys, skipCredCheckTrue, rawDataEmptyJson);
        Assert.fail("System create call should have thrown an exception when using a reserved ID. Id: " + id);
      } catch (IllegalStateException e)
      {
        Assert.assertTrue(e.getMessage().contains("SYSLIB_CREATE_RESERVED"));
      }
    }
  }

  // Test various cases where create/update should get rejected due to invalid credential input
  //   - credential contains invalid private key - create sys
  //   - credential contains invalid private key - create cred
  //   - credential contains invalid private key - update sys
  //   - system has dynamic effectiveUserId and cred provided - create sys
  //   - system has dynamic effectiveUserId and cred provided - update sys
  @Test
  public void testSystemCreateUpdateBadCred() throws Exception
  {
    TSystem sys0 = systems[19];
    // Update effectiveUserId to be dynamic since some cases require it and others are not invalidated by it
    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR);

    // Test create cases first since we will need to create a system to test the update cases
    sys0.setAuthnCredential(credInvalidPrivateSshKey);
    // Test system create with invalid private key
    try
    {
      svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
      Assert.fail("System create call should have thrown an exception when private ssh key is invalid");
    }
    catch (Exception e) { Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY1")); }
    // Test system create with dynamic effectiveUserId
    sys0.setAuthnCredential(credNoLoginUser);
    try
    {
      svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
      Assert.fail("System create call should have thrown an exception when effectiveUserId is dynamic");
    }
    catch (Exception e) { Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_DISALLOWED_INPUT")); }

    // Now create a system so we can test update cases
    sys0.setAuthnCredential(null);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    TSystem tmpSys = svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);

    // Test credential update with invalid private key
    try
    {
      svc.createUserCredential(rOwner1, sys0.getId(), sys0.getOwner(), credInvalidPrivateSshKey, skipCredCheckTrue, rawDataEmptyJson);
      Assert.fail("Credential update call should have thrown an exception when private ssh key is invalid");
    }
    catch (Exception e) { Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY2")); }

    // Test system update with invalid private key
    tmpSys.setAuthnCredential(credInvalidPrivateSshKey);
    try
    {
      svc.putSystem(rOwner1, tmpSys, skipCredCheckTrue, rawDataEmptyJson);
      Assert.fail("Credential update call should have thrown an exception when private ssh key is invalid");
    }
    catch (Exception e) { Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_INVALID_PRIVATE_SSHKEY1")); }

    // Test system update with dynamic effectiveUserId
    tmpSys.setAuthnCredential(credNoLoginUser);
    tmpSys.setEffectiveUserId(TSystem.APIUSERID_VAR);
    try
    {
      svc.putSystem(rOwner1, tmpSys, skipCredCheckTrue, rawDataEmptyJson);
      Assert.fail("System create call should have thrown an exception when effectiveUserId is dynamic");
    }
    catch (Exception e) { Assert.assertTrue(e.getMessage().contains("SYSLIB_CRED_DISALLOWED_INPUT")); }
  }

  // Test that attempting to create a system with invalid attribute combinations fails
  // Note that these checks are in addition to other similar tests such as: testReservedNames, testInvalidPrivateSshKey
  // NOTE: Not all combinations are checked.
  // - If canExec is true then jobWorkingDir must be set and jobRuntimes must have at least one entry.
  // - If canRunBatch is true
  //     batchScheduler must be specified
  //     batchLogicalQueues must not be empty
  //     batchLogicalDefaultQueue must be set
  //     batchLogicalDefaultQueue must be in the list of queues
  //     If batchLogicalQueues has more then one item then batchDefaultLogicalQueue must be set
  //     batchDefaultLogicalQueue must be in the list of logical queues.
  // - If type is OBJECT_STORE then bucketName must be set, isExec must be false.
  // - If systemType is LINUX then rootDir is required.
  // - effectiveUserId is restricted.
  // - If effectiveUserId is dynamic then providing credentials is disallowed
  // - If credential is provided and contains ssh keys then validate them
  // - SchedulerProfile must exist
  // - envVariables contains a FIXED entry then value cannot be !tapis_not_set
  @Test
  public void testCreateInvalidMiscFail()
  {
    TSystem sys0 = systems[24];
    // If canExec is true then jobWorkingDir must be set and jobRuntimes must have at least one entry.
    String tmpJobWorkingDir = sys0.getJobWorkingDir();
    sys0.setJobWorkingDir(null);
    boolean pass = false;
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CANEXEC_NO_JOBWORKINGDIR_INPUT"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setJobWorkingDir(tmpJobWorkingDir);
    pass = false;
    sys0.setJobRuntimes(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CANEXEC_NO_JOBRUNTIME_INPUT"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setJobRuntimes(jobRuntimes1);

    // If canRunBatch is true
    //     batchScheduler must be specified
    pass = false;
    sys0.setBatchScheduler(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_ISBATCH_NOSCHED"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setBatchScheduler(batchScheduler1);

    // If systemType is LINUX then rootDir is required.
    pass = false;
    sys0.setRootDir(null);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_NOROOTDIR"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setRootDir(rootDir1);
    pass = false;
    sys0.setRootDir("");
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_NOROOTDIR"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setRootDir(rootDir1);

    // SchedulerProfile must exist.
    pass = false;
    sys0.setBatchSchedulerProfile(noSuchSchedulerProfile);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_PRF_NO_PROFILE"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setBatchSchedulerProfile(batchSchedulerProfile1);

    // If envVariables contains a FIXED entry then value cannot be !tapis_not_set
    var tmpJobEnvVars = sys0.getJobEnvVariables();
    sys0.setJobEnvVariables(jobEnvVariablesReject);
    pass = false;
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_ENV_VAR_FIXED_UNSET"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setJobEnvVariables(tmpJobEnvVars);

  }

  // Test that attempting to create a system with control characters in attributes
  @Test
  public void testCreateInvalidControlCharactersFail()
  {
    TSystem sys0 = systems[24];
    // If bucketName has a control char then create should fail.
    String tmpStr = sys0.getBucketName();
    sys0.setBucketName(stringWithCtrlChar);
    boolean pass = false;
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      // Check that error message contains expected strings
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CTL_CHR_ATTR"));
      Assert.assertTrue(e.getMessage().contains("Attribute Name: bucketName"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Reset in prep for continued checking
    sys0.setBucketName(tmpStr);
    pass = false;

    // Do similar checks for a tag
    sys0.setTags(tagsWithCtrlChar);
    try { svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (Exception e)
    {
      // Check that error message contains expected strings
      Assert.assertTrue(e.getMessage().contains("SYSLIB_CTL_CHR_ATTR"));
      Assert.assertTrue(e.getMessage().contains("Attribute Name: tags"));
      pass = true;
    }
    Assert.assertTrue(pass);
    sys0.setTags(tags1);
    pass = false;
  }

  // Test creating, reading and deleting user permissions for a system
  @Test
  public void testUserPerms() throws Exception
  {
    // Create a system
    TSystem sys0 = systems[9];
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);

    // Owner should be able to grant/revoke for themselves in preparation for changeSystemOwner.
    svc.grantUserPermissions(rOwner1, sys0.getId(), owner1, testPermsREADMODIFY, rawDataEmptyJson);
    Set<Permission> userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), owner1);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPermsREADMODIFY.size(), "Incorrect number of perms returned.");
    for (Permission perm: testPermsREADMODIFY) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm.name()); }
    svc.revokeUserPermissions(rOwner1, sys0.getId(), owner1, testPermsREADMODIFY, rawDataEmptyJson);
    int changeCount = svc.revokeUserPermissions(rOwner1, sys0.getId(), owner1, testPermsREADMODIFY, rawDataEmptyJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions.");
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), owner1);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }

    // Create non-owner user perms for the system
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);
    // Get the system perms for the user and make sure permissions are there
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    Assert.assertNotNull(userPerms, "Null returned when retrieving perms.");
    Assert.assertEquals(userPerms.size(), testPermsREADMODIFY.size(), "Incorrect number of perms returned.");
    for (Permission perm: testPermsREADMODIFY) { if (!userPerms.contains(perm)) Assert.fail("User perms should contain permission: " + perm.name()); }
    // Remove perms for the user. Should return a change count of 2
    changeCount = svc.revokeUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }

    // Give testuser3 back some perms, so we can test revokePerms auth when user is not the owner and is target user
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);

    // Have testuser3 remove their own perms. Should return a change count of 2
    changeCount = svc.revokeUserPermissions(rTestUser3, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);
    Assert.assertEquals(changeCount, 2, "Change count incorrect when revoking permissions as user - not owner.");
    // Get the system perms for the user and make sure permissions are gone.
    userPerms = svc.getUserPermissions(rOwner1, sys0.getId(), testUser3);
    for (Permission perm: testPermsREADMODIFY) { if (userPerms.contains(perm)) Assert.fail("User perms should not contain permission: " + perm.name()); }

    // Give testuser3 back some perms, so we can test revokePerms auth when user is not the owner and is not target user
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);
    try {
      svc.revokeUserPermissions(rTestUser2, sys0.getId(), testUser3, testPermsREADMODIFY, rawDataEmptyJson);
      Assert.fail("Update of perms by non-owner user who is not target user should have thrown an exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("SYSLIB_UNAUTH"));
    }
  }

  // Test creating, reading and deleting user credentials for a system
  // Initial system is dynamic, effectiveUserId = ${apiUserId}
  //   - Test 1 - create and get cred as owner1, testuser3
  //            - create and get should always use Tapis user (owner1, testuser3) in method arguments
  // Also test loginUser mapping functionality.
  //   - Test 2 - basic loginUser mapping with dynamic TSystem. Create and get cred as owner1, testuser3, testuser4
  // Test switching system from dynamic to static
  //   - Test 3 - patch system to have static effectiveUserId = "testuser5LinuxUser", get cred as owner1, testuser3
  //            - create and get should use static user in method arguments
  // Test switching system back from static to dynamic
  //   - Test 4 - patch system to revert to dynamic effectiveUserId = ${apiUserId}, get cred as owner1, testuser3, testuser4
  //            - create and get should always use Tapis user (owner1, testuser3, testuser4) in method arguments
  @Test
  public void testUserCredentials() throws Exception
  {
    // Create dynamic system with effUsr = apiUserId
    TSystem sys0 = systems[10];
    String sysId = sys0.getId();
    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR); // "${apiUserId}"
    // Create the system
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Create in-memory objects for all credentials we will use.
    Credential cred1NoLoginUser = new Credential(null, null, "fakePassword1", "fakePrivateKey1", "fakePublicKey1",
            "fakeAccessKey1", "fakeAccessSecret1", "fakeAccessToken1", "fakeRefreshToken1", "fakeCert1");
    Credential cred3NoLoginUser = new Credential(null, null, "fakePassword3", "fakePrivateKey3", "fakePublicKey3",
            "fakeAccessKey3", "fakeAccessSecret3", "fakeAccessToken3", "fakeRefreshToken3", "fakeCert3");
    Credential cred3NoLoginUserAccessAuthn = new Credential(null, null, null, null, null, "fakeAccessKey3a", "fakeAccessSecret3a", null, null, null);
    Credential cred4LoginUser = new Credential(null, testUser4LinuxUser, "fakePassword4", null, null, null, null, null, null, null);
    Credential cred5A_NoLoginUser = new Credential(null, null, "fakePassword5a", null, null, null, null, null, null, null);
    Credential cred5NoLoginLinuxUser = new Credential(null, null, "fakePassword5LinuxUser", null, null, null, null, null, null, null);
    Credential cred5NoLoginStatic = new Credential(null, null, "fakePassword5Static", null, null, null, null, null, null, null);
    Credential cred5B_LoginUser = new Credential(null, testUser5LinuxUser, "fakePassword5b", null, null, null, null, null, null, null);

    // We will be updating credentials for testUser3, 5 so allow them READ access to system.
    svc.grantUserPermissions(rOwner1, sysId, testUser3, testPermsREAD, rawDataEmptyJson);
    svc.grantUserPermissions(rOwner1, sysId, testUser5, testPermsREAD, rawDataEmptyJson);

    // Make the separate calls required to store credentials for each user.
    // In this case for owner1, testUser3, testUser5
    // These should all go under the dynamic secret path in SK
    svc.createUserCredential(rOwner1, sysId, owner1, cred1NoLoginUser, skipCredCheckTrue, rawDataEmptyJson);
    svc.createUserCredential(rOwner1, sysId, testUser3, cred3NoLoginUser, skipCredCheckTrue, rawDataEmptyJson);
    svc.createUserCredential(rOwner1, sysId, testUser5, cred5A_NoLoginUser, skipCredCheckTrue, rawDataEmptyJson);

    // ------------------------
    // Test 1 - basic cred retrieve/delete for owner1, testuser3
    //        - fetch creds for specific authnMethod
    // -------------------------
    // Get system as owner using files service, should get cred for owner
    TSystem tmpSys = svc.getSystem(rFilesSvcOwner1, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred1NoLoginUser.getPassword(), owner1, owner1);

    // Get system as testUser3 using files service and should get cred for testUser3
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred3NoLoginUser.getPassword(), testUser3, testUser3);

    // Get credentials for testUser3 and validate
    // Use files service AuthenticatedUser since only certain services can retrieve the cred.
    Credential cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.PASSWORD);
    // Verify credentials
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getPassword(), cred3NoLoginUser.getPassword());
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PKI_KEYS);
    Assert.assertEquals(cred0.getPublicKey(), cred3NoLoginUser.getPublicKey());
    Assert.assertEquals(cred0.getPrivateKey(), cred3NoLoginUser.getPrivateKey());
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser3);
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred0.getAccessKey(), cred3NoLoginUser.getAccessKey());
    Assert.assertEquals(cred0.getAccessSecret(), cred3NoLoginUser.getAccessSecret());

    // Delete credentials and verify they were destroyed
    int changeCount = svc.deleteUserCredential(rOwner1, sysId, owner1);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing credential for user: " + owner1);
    changeCount = svc.deleteUserCredential(rOwner1, sysId, testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing credential for user: " + testUser3);

    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, owner1, AuthnMethod.PASSWORD);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sysId + " User name: " + owner1);
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.PASSWORD);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sysId + " User name: " + testUser3);

    // Attempt to delete again, should return 0 for change count
    changeCount = svc.deleteUserCredential(rOwner1, sysId, testUser3);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when removing a credential already removed.");

    // Update cred to set just ACCESS_KEY and test
    // This should go under the dynamic secret path in SK
    svc.createUserCredential(rOwner1, sysId, testUser3, cred3NoLoginUserAccessAuthn, skipCredCheckTrue, rawDataEmptyJson);
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertEquals(cred0.getAccessKey(), cred3NoLoginUserAccessAuthn.getAccessKey());
    Assert.assertEquals(cred0.getAccessSecret(), cred3NoLoginUserAccessAuthn.getAccessSecret());
    // Attempt to retrieve secret that has not been set
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.PKI_KEYS);
    Assert.assertNull(cred0, "Credential was non-null for missing secret. System name: " + sysId + " User name: " + testUser3);
    // Delete credentials and verify they were destroyed
    changeCount = svc.deleteUserCredential(rOwner1, sysId, testUser3);
    Assert.assertEquals(changeCount, 1, "Change count incorrect when removing a credential.");
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser3, AuthnMethod.ACCESS_KEY);
    Assert.assertNull(cred0, "Credential not deleted. System name: " + sysId + " User name: " + testUser3);

    // ============================================
    // Tests for tapis user to loginUser mapping.
    // ============================================
    // ------------------------
    // Test 2 - basic loginUser mapping with dynamic TSystem. Create and get cred for testuser3, testuser4
    // -------------------------
    // Create a credential for Tapis user testUser4 with a loginUser so that a mapping should be created.
    // owner should be permitted to update their own credential
    // This should go under the dynamic secret path in SK
    svc.createUserCredential(rOwner1, sysId, testUser4, cred4LoginUser, skipCredCheckTrue, rawDataEmptyJson);
    // Give testUser4 READ access to the system. Normally this would be done through sharing and call would
    // be made with impersonationId set to the system owner but here we are testing loginUser mapping, not impersonation.
    svc.grantUserPermissions(rOwner1, sysId, testUser4, testPermsREAD, rawDataEmptyJson);

    // Now when fetching System as Files with oboUser=testUser4 and impersonationId=null
    //   we should find effectiveUserId=testUser4LinuxUser and password=fakePassword4
    tmpSys = svc.getSystem(rFilesSvcTestUser4, sysId, AuthnMethod.PASSWORD, requireExecPermFalse, getCredsTrue,
                           impersonationIdNull, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred4LoginUser.getPassword(), testUser4, testUser4LinuxUser);

    // When Files gets as system as itself using impersonationId=testuser4 and resourceTenant=dev
    //   we should get back the mapped login user, i.e. effectiveUserId=testUser4LinuxUser and password=fakePassword4
    tmpSys = svc.getSystem(rFilesSvcAsFiles, sysId, AuthnMethod.PASSWORD, requireExecPermFalse, getCredsTrue,
                           testUser4, sharedCtxNull, tenantName, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred4LoginUser.getPassword(), testUser4, testUser4LinuxUser);

    // when fetching System as Files with oboUser=testUser3 and impersonationId=testUser4
    //   we should also find effectiveUserId=testUser4LinuxUser and password=fakePassword4
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sysId, AuthnMethod.PASSWORD, requireExecPermFalse, getCredsTrue,
                           testUser4, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred4LoginUser.getPassword(), testUser4, testUser4LinuxUser);

    // ------------------------
    // Test 3: patch system to have static effectiveUserId = "testuser5LinuxUser", get cred
    // -------------------------
    // Patch the system
    String rawDataPatch = "{\"effectiveUserId\": \"testuser5LinuxUser\"}";
    PatchSystem patchSystem = new PatchSystem(null, null, testUser5LinuxUser, null, null, null, null, null, null,
                              null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    svc.patchSystem(rOwner1, sysId, patchSystem, rawDataPatch);
    // Retrieve with resolve of effUser, effUser should be static value
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, getCredsFalse, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), testUser5LinuxUser);
    // Create "static" cred for testuser5LinuxUser and testuser5
    // These should go under the static secret path in SK
    // Note that we create a static cred for testuser5 to make sure it does not get mixed up with the dynamic cred for same user name
    svc.createUserCredential(rOwner1, sysId, testUser5LinuxUser, cred5NoLoginLinuxUser, skipCredCheckTrue, rawDataEmptyJson);
    svc.createUserCredential(rOwner1, sysId, testUser5, cred5NoLoginStatic, skipCredCheckTrue, rawDataEmptyJson);
    // Get cred and verify for testUser5LinuxUser
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser5LinuxUser, AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser5LinuxUser);
    Assert.assertEquals(cred0.getPassword(), cred5NoLoginLinuxUser.getPassword());
    // Get cred and verify for testUser5
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sysId, testUser5, AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null for user: " + testUser5);
    Assert.assertEquals(cred0.getPassword(), cred5NoLoginStatic.getPassword());

    // Get sys as owner and check cred. Since it is static should always get back cred for testUser5LinuxUser
    tmpSys = svc.getSystem(rFilesSvcOwner1, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred5NoLoginLinuxUser.getPassword(), testUser5, testUser5LinuxUser);

    // Get as testUser3 and check cred. Since it is static should always get back cred for testUser5
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred5NoLoginLinuxUser.getPassword(), testUser5, testUser5LinuxUser);

    // ------------------------
    // Test 4: patch system to revert to dynamic effectiveUserId = ${apiUserId}, get cred
    // -------------------------
    // Patch the system
    rawDataPatch = "{\"effectiveUserId\": \"${apiUserId}\"}";
    patchSystem = new PatchSystem(null, null, TSystem.APIUSERID_VAR, null, null, null, null, null, null,
                                  null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    svc.patchSystem(rOwner1, sysId, patchSystem, rawDataPatch);
    // Retrieve with resolve, check effUser
    tmpSys = svc.getSystem(rOwner1, sysId, null, false, getCredsFalse, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Assert.assertEquals(tmpSys.getEffectiveUserId(), owner1);
    // Re-create creds for owner1, testuser3. Recall we deleted them above as part of the test
    svc.createUserCredential(rOwner1, sysId, owner1, cred1NoLoginUser, skipCredCheckTrue, rawDataEmptyJson);
    svc.createUserCredential(rOwner1, sysId, testUser3, cred3NoLoginUser, skipCredCheckTrue, rawDataEmptyJson);
    // Create "dynamic" cred for testuser5
    // This should go under the dynamic secret path in SK
    svc.createUserCredential(rOwner1, sysId, testUser5, cred5B_LoginUser, skipCredCheckTrue, rawDataEmptyJson);

    // Get system as owner and check cred, should be same as before for "dynamic" use case.
    tmpSys = svc.getSystem(rFilesSvcOwner1, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred1NoLoginUser.getPassword(), owner1, owner1);
    // Get system as testUser3 using files service and should get cred for testUser3
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred3NoLoginUser.getPassword(), testUser3, testUser3);

    // Get system as testUser5 and check cred.
    tmpSys = svc.getSystem(rFilesSvcTestUser5, sysId, AuthnMethod.PASSWORD, false, getCredsTrue, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    checkCredPasswordAndEffectiveUser(tmpSys, cred5B_LoginUser.getPassword(), testUser5, testUser5LinuxUser);
  }

  // Test creating, reading and deleting user credentials for a system
  //  for the case of a static effectiveUserId
  @Test
  public void testUserCredentialsStaticEffUser() throws Exception
  {
    // Create a system where effectiveUserId is static and credentials are provided with system definition.
    TSystem sys0 = systems[28];
    Credential cred1 = new Credential(null, null, "fakePassword1", "fakePrivateKey1", "fakePublicKey1",
                                      "fakeAccessKey1", "fakeAccessSecret1", "fakeAccessToken1", "fakeRefreshToken1", "fakeCert1");
    sys0.setEffectiveUserId(effectiveUserId1);
    sys0.setAuthnCredential(cred1);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);

    // Get system as owner using files service, should get cred for static effUser
    TSystem tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), AuthnMethod.PASSWORD, requireExecPermFalse,
                                   getCredsTrue, impersonationIdNull, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Credential cred0 = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred0, "AuthnCredential should not be null");
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null");
    Assert.assertEquals(cred0.getPassword(), cred1.getPassword());
    // Make sure cred has correct loginUser, the static value
    Assert.assertEquals(cred0.getLoginUser(), effectiveUserId1, "Incorrect loginUser. Should be static effUser");

    // Fetch credentials directly using targetUser=<static effUser>
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), effectiveUserId1, AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0, "AuthnCredential should not be null");
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null");
    Assert.assertEquals(cred0.getPassword(), cred1.getPassword());
    Assert.assertEquals(cred0.getLoginUser(), effectiveUserId1, "Incorrect loginUser. Should be static effUser");

    // Owner should have no credentials
    cred0 = svc.getUserCredential(rFilesSvcOwner1, sys0.getId(), owner1, AuthnMethod.PASSWORD);
    Assert.assertNull(cred0, "AuthnCredential should be null for owner");

    // When impersonating should still get back same cred and loginUser
    // Give testUser4 READ access to the system. Normally this would be done through sharing and call would
    // be made with impersonationId set to the system owner but here we are testing loginUser mapping, not impersonation.
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser4, testPermsREAD, rawDataEmptyJson);
    tmpSys = svc.getSystem(rFilesSvcTestUser3, sys0.getId(), AuthnMethod.PASSWORD, requireExecPermFalse, getCredsTrue,
                           testUser4, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    cred0 = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred0, "AuthnCredential should not be null");
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null");
    Assert.assertEquals(cred0.getPassword(), cred1.getPassword());
    Assert.assertEquals(cred0.getLoginUser(), effectiveUserId1, "Incorrect loginUser. Should be static effUser");
  }

  // Test creating, reading and deleting user credentials when user is not the owner, dynamic effectiveUserId
  // Test that user can create and remove credentials when:
  //    - user only has READ perm
  //    - user only has MODIFY perm
  //    - user only has share access
  @Test
  public void testUserCredentialsNotOwner() throws Exception
  {
    // Create a system where effectiveUserId is dynamic.
    TSystem sys0 = systems[2];
    String sysId = sys0.getId();
    sys0.setEffectiveUserId(TSystem.APIUSERID_VAR);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);

    Credential cred1 = new Credential(null, null, "fakePassword1", "fakePrivateKey1", "fakePublicKey1",
                                      "fakeAccessKey1", "fakeAccessSecret1", "fakeAccessToken1", "fakeRefreshToken1", "fakeCert1");
    svc.createUserCredential(rOwner1, sysId, owner1, cred1, skipCredCheckTrue, rawDataEmptyJson);

    String rawDataShare = "{\"users\": [\"" + testUser5 + "\"]}";
    SystemShare systemShare = TapisGsonUtils.getGson().fromJson(rawDataShare, SystemShare.class);

    // Cleanup from any previous runs
    svc.unshareSystemPublicly(rOwner1, sysId);
    svc.unshareSystem(rOwner1, sysId, systemShare);
    svc.revokeUserPermissions(rOwner1, sys0.getId(), testUser5, testPermsREADMODIFY, rawDataEmptyJson);

    // Get system as owner using files service, should get cred for static effUser
    TSystem tmpSys = svc.getSystem(rFilesSvcOwner1, sys0.getId(), AuthnMethod.PASSWORD, requireExecPermFalse,
            getCredsTrue, impersonationIdNull, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    Credential cred0 = tmpSys.getAuthnCredential();
    Assert.assertNotNull(cred0, "AuthnCredential should not be null");
    Assert.assertEquals(cred0.getAuthnMethod(), AuthnMethod.PASSWORD);
    Assert.assertNotNull(cred0.getPassword(), "AuthnCredential password should not be null");
    Assert.assertEquals(cred0.getPassword(), cred1.getPassword());

    // Initially user testUser5 should not be able to set a cred.
    boolean pass = false;
    try { svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteUserCredential(rTestUser5, sysId, testUser5); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Grant READ perm, now user should be able to set cred
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser5, testPermsREAD, rawDataEmptyJson);
    svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson);
    svc.deleteUserCredential(rTestUser5, sysId, testUser5);

    // Revoke READ perm and grant MODIFY perm. User should be able to set cred.
    svc.revokeUserPermissions(rOwner1, sys0.getId(), testUser5, testPermsREAD, rawDataEmptyJson);
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser5, testPermsMODIFY, rawDataEmptyJson);
    svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson);
    svc.deleteUserCredential(rTestUser5, sysId, testUser5);

    // Revoke MODIFY perm and share system. User should be able to set cred.
    svc.revokeUserPermissions(rOwner1, sys0.getId(), testUser5, testPermsMODIFY, rawDataEmptyJson);
    svc.shareSystem(rOwner1, sysId, systemShare);
    svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson);
    svc.deleteUserCredential(rTestUser5, sysId, testUser5);

    // Unshare and then share publicly. User should be able to set cred
    svc.unshareSystem(rOwner1, sysId, systemShare);
    svc.shareSystemPublicly(rOwner1, sysId);
    svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson);
    svc.deleteUserCredential(rTestUser5, sysId, testUser5);

    // Unshare public and now testUser5 should again be denied
    svc.unshareSystemPublicly(rOwner1, sysId);
    pass = false;
    try { svc.createUserCredential(rTestUser5, sysId, testUser5, cred1, skipCredCheckTrue, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteUserCredential(rTestUser5, sysId, testUser5); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
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
    TSystem tmpSys = svc.getSystem(rOwner1, fakeSystemName, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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

    // Get perms with no system should throw exception
    pass = false;
    try { svc.getUserPermissions(rOwner1, fakeSystemName, fakeUserName); }
    catch (NotFoundException nfe)
    {
      pass = true;
    }
    Assert.assertTrue(pass);

    // Revoke perm with no system should return 0 changes
    changeCount = svc.revokeUserPermissions(rOwner1, fakeSystemName, fakeUserName, testPermsREADMODIFY, rawDataEmptyJson);
    Assert.assertEquals(changeCount, 0, "Change count incorrect when revoking perms for non-existent system.");

    // Grant perm with no system should throw an exception
    pass = false;
    try { svc.grantUserPermissions(rOwner1, fakeSystemName, fakeUserName, testPermsREADMODIFY, rawDataEmptyJson); }
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
    cred = new Credential(null, null, null, null, null, "fakeAccessKey2", "fakeAccessSecret2", null, null, null);
    try { svc.createUserCredential(rOwner1, fakeSystemName, fakeUserName, cred, skipCredCheckTrue, rawDataEmptyJson); }
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
            dtnSystemFakeHostname, jobRuntimes1, jobWorkingDir1, jobEnvVariables1, jobMaxJobs1,
            jobMaxJobsPerUser1, canRunBatchTrue, enableCmdPrefixTrue, mpiCmd1, batchScheduler1, logicalQueueList1,
            batchDefaultLogicalQueue1, batchSchedulerProfile1, capList2, tags2, notes2, importRefId2, allowChildrenFalse);

    // CREATE - Deny user not owner/admin, deny service calling as itself
    boolean pass = false;
    try { svc.createSystem(rTestUser0, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createSystem(rFilesSvcAsFiles, sys0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Create system for remaining auth access tests
    Credential cred0 = new Credential(null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
                                      "fakeAccessKey", "fakeAccessSecret", "fakeAccessToken", "fakeRefreshToken", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Grant testUesr3 - READ
    svc.grantUserPermissions(rOwner1, systemId, testUser3, testPermsREAD, rawDataEmptyJson);
    // Grant testUser2 - MODIFY
    svc.grantUserPermissions(rOwner1, systemId, testUser2, testPermsMODIFY, rawDataEmptyJson);

    // READ - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getSystem(rTestUser0, systemId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // EXECUTE - deny user not owner/admin with READ but not EXECUTE
    pass = false;
    try { svc.getSystem(rTestUser3, systemId, null, true, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // MODIFY Deny user with no READ or MODIFY, deny user with only READ
    pass = false;
    try { svc.patchSystem(rTestUser0, systemId, patchSys, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.patchSystem(rTestUser3, systemId, patchSys, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // DELETE - deny user not owner/admin
    pass = false;
    try { svc.deleteSystem(rTestUser3, systemId); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // CHANGE_OWNER - deny user not owner/admin
    pass = false;
    try { svc.changeSystemOwner(rTestUser3, systemId, testUser2); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GET_PERMS - deny user not owner/admin and no READ or MODIFY access
    pass = false;
    try { svc.getUserPermissions(rTestUser0, systemId, owner1); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // GRANT_PERMS - deny user not owner/admin
    pass = false;
    try { svc.grantUserPermissions(rTestUser3, systemId, testUser0, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // REVOKE_PERMS - deny user not owner/admin
    pass = false;
    try { svc.revokeUserPermissions(rTestUser3, systemId, owner1, testPermsREADMODIFY, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // SET_CRED - deny user not owner/admin and not target user
    pass = false;
    try { svc.createUserCredential(rTestUser3, systemId, owner1, cred0, skipCredCheckTrue, rawDataEmptyJson); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // Services now allowed to modify, etc obo a user
//    pass = false;
//    try { svc.createUserCredential(rFilesSvcOwner1, systemId, owner1, cred0, skipCredCheckTrue, rawDataEmtpyJson); }
//    catch (ForbiddenException e)
//    {
//      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
//      pass = true;
//    }
//    Assert.assertTrue(pass);

    // REMOVE_CRED - deny user not owner/admin and not target user
    pass = false;
    try { svc.deleteUserCredential(rTestUser3, systemId, owner1); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
//    pass = false;
//    try { svc.deleteUserCredential(rFilesSvcOwner1, systemId, owner1); }
//    catch (ForbiddenException e)
//    {
//      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
//      pass = true;
//    }
//    Assert.assertTrue(pass);

    // GET_CRED - deny user not owner/admin, deny owner - with special message
    pass = false;
    try { svc.getUserCredential(rTestUser3, systemId, owner1, null); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH_GETCRED"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.getUserCredential(rOwner1, systemId, owner1, null); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH_GETCRED"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // User should not be able to impersonate another user.
    pass = false;
    try { svc.getSystem(rTestUser1, systemId, null, false, false, owner1, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH_IMPERSONATE"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // When a service impersonates another user they should be denied if that user cannot read the system.
    pass = false;
    try { svc.getSystem(rFilesSvcTestUser3, systemId, null, false, false, impersonationIdTestUser9, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // User should not be able to set sharedAppCtx
    pass = false;
    try { svc.getSystem(rTestUser1, systemId, null, false, false, impersonationIdNull, sharedCtxOwner, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH_SHAREDAPPCTX"));
      pass = true;
    }
    Assert.assertTrue(pass);
    // Apps service should not be able to set sharedAppCtx
    pass = false;
    try { svc.getSystem(rAppsSvcTestUser1, systemId, null, false, false, impersonationIdNull, sharedCtxOwner, resourceTenantNull, fetchShareInfoFalse); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_UNAUTH_SHAREDAPPCTX"));
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
    Credential cred0 = new Credential(null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
            "fakeAccessKey", "fakeAccessSecret", "fakeAccessToken", "fakeRefreshToken", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    // Grant User1 - READ and User2 - MODIFY
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser3, testPermsREADEXECUTE, rawDataEmptyJson);
    svc.grantUserPermissions(rOwner1, sys0.getId(), testUser2, testPermsMODIFY, rawDataEmptyJson);

    // READ - allow owner, service, with READ only, with MODIFY only
    svc.getSystem(rOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rOwner1, sys0.getId(), null, true, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rFilesSvcOwner1, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rTestUser3, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rTestUser3, sys0.getId(), null, true, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rTestUser2, sys0.getId(), null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    // Files should be allowed to impersonate another user
    svc.getSystem(rFilesSvcTestUser3, sys0.getId(), null, false, false, owner1, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
    // Jobs and Files should be allowed to set sharedAppCtx
    svc.getSystem(rJobsSvcOwner1, sys0.getId(), null, false, false, impersonationIdNull, sharedCtxOwner, resourceTenantNull, fetchShareInfoFalse);
    svc.getSystem(rFilesSvcTestUser3, sys0.getId(), null, false, false, impersonationIdNull, sharedCtxOwner, resourceTenantNull, fetchShareInfoFalse);

    // When a service impersonates another user it should be allowed if sharedAppCtx set to true even if normally denied.
    svc.getSystem(rFilesSvcTestUser3, sys0.getId(), null, false, false, impersonationIdTestUser9, sharedCtxOwner, resourceTenantNull, fetchShareInfoFalse);
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
    var tmpModLoads = tmpProfile.getModuleLoads();
    var p0ModLoads = p0.getModuleLoads();
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
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.createSchedulerProfile(rFilesSvcOwner1, p1); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);

    // DELETE - deny user not owner/admin, deny service
    pass = false;
    try { svc.deleteSchedulerProfile(rTestUser1, p0.getName()); }
    catch (ForbiddenException e)
    {
      Assert.assertTrue(e.getMessage().startsWith("SYSLIB_PRF_UNAUTH"));
      pass = true;
    }
    Assert.assertTrue(pass);
    pass = false;
    try { svc.deleteSchedulerProfile(rFilesSvcOwner1, p0.getName()); }
    catch (ForbiddenException e)
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
    Assert.assertEquals(tmpSys.getId(), sys0.getId());
    Assert.assertEquals(tmpSys.getEffectiveUserId(), sys0.getEffectiveUserId());
    Assert.assertEquals(tmpSys.getRootDir(), sys0.getRootDir());
    Assert.assertEquals(tmpSys.getOwner(), sys0.getOwner());
    System.out.println("Found item: " + sys0.getId());
    compareSystems(sys0, tmpSys);
  }
  private static void checkCommonParentChildAttrs(TSystem parentSystem, TSystem childSystem) {
    Assert.assertEquals(parentSystem.getId(), childSystem.getParentId());
    compareSystems(parentSystem, childSystem);
  }

  private static void compareSystems(TSystem sys1, TSystem sys2)
  {
    Assert.assertEquals(sys2.getDescription(), sys1.getDescription());
    Assert.assertEquals(sys2.getSystemType().name(), sys1.getSystemType().name());
    Assert.assertEquals(sys2.getHost(), sys1.getHost());
    Assert.assertEquals(sys2.getDefaultAuthnMethod().name(), sys1.getDefaultAuthnMethod().name());
    Assert.assertEquals(sys2.getBucketName(), sys1.getBucketName());

    Assert.assertEquals(sys2.getPort(), sys1.getPort());
    Assert.assertEquals(sys2.isUseProxy(), sys1.isUseProxy());
    Assert.assertEquals(sys2.getProxyHost(), sys1.getProxyHost());
    Assert.assertEquals(sys2.getProxyPort(), sys1.getProxyPort());
    Assert.assertEquals(sys2.getDtnSystemId(), sys1.getDtnSystemId());
    Assert.assertEquals(sys2.getCanExec(), sys1.getCanExec());
    Assert.assertEquals(sys2.getJobWorkingDir(), sys1.getJobWorkingDir());

    Assert.assertEquals(sys2.getImportRefId(), sys1.getImportRefId());

    // Verify jobEnvVariables
    verifyKeyValuePairs(sys1.getJobEnvVariables(), sys2.getJobEnvVariables());

    Assert.assertEquals(sys2.getJobMaxJobs(), sys1.getJobMaxJobs());
    Assert.assertEquals(sys2.getJobMaxJobsPerUser(), sys1.getJobMaxJobsPerUser());
    Assert.assertEquals(sys2.getCanRunBatch(), sys1.getCanRunBatch());
    Assert.assertEquals(sys2.getMpiCmd(), sys1.getMpiCmd());
    Assert.assertEquals(sys2.getBatchScheduler(), sys1.getBatchScheduler());
    Assert.assertEquals(sys2.getBatchDefaultLogicalQueue(), sys1.getBatchDefaultLogicalQueue());
    Assert.assertEquals(sys2.getBatchSchedulerProfile(), sys1.getBatchSchedulerProfile());

    // Verify tags
    String[] origTags = sys1.getTags();
    String[] tmpTags = sys2.getTags();
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
    Assert.assertNotNull(sys1.getNotes(), "Orig Notes should not be null");
    Assert.assertNotNull(sys2.getNotes(), "Fetched Notes should not be null");
    System.out.println("Found notes: " + sys1.getNotes());
    JsonObject tmpObj = (JsonObject) sys2.getNotes();
    JsonObject origNotes = (JsonObject) sys1.getNotes();
    Assert.assertTrue(tmpObj.has("project"));
    String projStr = origNotes.get("project").getAsString();
    Assert.assertEquals(tmpObj.get("project").getAsString(), projStr);
    Assert.assertTrue(tmpObj.has("testdata"));
    String testdataStr = origNotes.get("testdata").getAsString();
    Assert.assertEquals(tmpObj.get("testdata").getAsString(), testdataStr);

    // Verify jobRuntimes
    List<JobRuntime> origRuntimes = sys1.getJobRuntimes();
    List<JobRuntime> jobRuntimes = sys2.getJobRuntimes();
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
    List<LogicalQueue> origLogicalQueues = sys1.getBatchLogicalQueues();
    List<LogicalQueue> logicalQueues = sys2.getBatchLogicalQueues();
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
    List<Capability> origCaps = sys1.getJobCapabilities();
    List<Capability> jobCaps = sys2.getJobCapabilities();
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

    Assert.assertNotNull(sys2.getCreated(), "Fetched created timestamp should not be null");
    Assert.assertNotNull(sys2.getUpdated(), "Fetched updated timestamp should not be null");
  }

  // Test retrieving a system history
  @Test
  public void testGetSystemHistory() throws Exception
  {
    TSystem sys0 = systems[26];
    Credential cred0 = new Credential(null, null, "fakePassword", "fakePrivateKey", "fakePublicKey",
                                      "fakeAccessKey", "fakeAccessSecret", "fakeAccessToken", "fakeRefreshToken", "fakeCert");
    sys0.setAuthnCredential(cred0);
    svc.createSystem(rOwner1, sys0, skipCredCheckTrue, rawDataEmptyJson);
    
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
    Credential cred0 = new Credential(null, null, "fakePassword", "fakePrivateKey", "fakePublicKey", "fakeAccessKey",
                                      "fakeAccessSecret", "fakeAccessToken1", "fakeRefreshToken1", "fakeCert");
    sys0.setAuthnCredential(cred0);
    sys0.setOwner(testUser2);
    sys0.setJobRuntimes(jobRuntimes1);
    sys0.setBatchLogicalQueues(logicalQueueList1);
    sys0.setJobCapabilities(capList1);
    // Create systems - history record 1
    svc.createSystem(ownerUser, sys0, skipCredCheckTrue, rawDataEmptyJson);
    TSystem tmpSys = svc.getSystem(ownerUser, sysId, null, false, false, null, sharedCtxNull, resourceTenantNull, fetchShareInfoFalse);
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
    // Retrieve and display history for manual checking of system history records
    displaySystemHistory(ownerUser, sysId);

    svc.undeleteSystem(ownerUser, sysId);
    Assert.assertEquals(systemHistory.size(), 6);
  }
  
  // Test retrieving system sharing information
  // System owned by testuser5, shared with testuser4
  @Test
  public void testShareSystem() throws Exception
  {
    TSystem sys0 = systems[29];
    String sysId = sys0.getId();
    sys0.setOwner(testUser5);
    svc.createSystem(rTestUser5, sys0, skipCredCheckTrue, rawDataEmptyJson);
    
    // **************************  Create and share system  ***************************
    //  Create a SystemShare from the json 
   SystemShare systemShare;
   String testUserName = testUser4;
   String rawDataShare = "{\"users\": [\"" + testUserName + "\"]}";
   Set<String> testUserList = new HashSet<String>(1);
   testUserList.add(testUserName);
   systemShare = TapisGsonUtils.getGson().fromJson(rawDataShare, SystemShare.class);
   
   // Service call
   svc.shareSystem(rTestUser5, sysId, systemShare);
   
   // Test retrieval using specified authn method
   SystemShare systemShareTest = svc.getSystemShare(rTestUser5, sysId);
   System.out.println("Found item: " + sysId);

   // Verify system share fields
   Assert.assertNotNull(systemShareTest, "System Share information found.");
   Assert.assertEquals(systemShareTest.getUserList(), testUserList);
   // Retrieve users, test user is on the list
   boolean userFound = false;
   for (var user : systemShareTest.getUserList()) {
     if (user.equals(testUserName)) { userFound = true; }
     System.out.printf("Shared with userName: %s%n", user);
   }
   Assert.assertTrue(userFound);  
   
   // **************************  Unsharing system  ***************************
   
   // Service call
   svc.unshareSystem(rTestUser5, sysId, systemShare);
   
   // Test retrieval using specified authn method
   systemShareTest = svc.getSystemShare(rTestUser5, sysId);
   System.out.println("Found item: " + sysId);

   // Verify system share fields
   Assert.assertNotNull(systemShareTest, "System Share information found.");
   // Retrieve users, test user is not on the list
   userFound = false;
   for (var user : systemShareTest.getUserList()) {
     if (user.equals(testUserName)) { userFound = true; }
     System.out.printf("userName: %s%n", user);
   }
   Assert.assertFalse(userFound);  

   // **************************  Sharing system publicly  ***************************
   // Service call
   svc.shareSystemPublicly(rTestUser5, sysId);
   
   // Test retrieval using specified authn method
   systemShareTest = svc.getSystemShare(rTestUser5, sysId);
   System.out.println("Found item: " + sysId);

   // Verify system share fields
   Assert.assertNotNull(systemShareTest, "System Share information found.");
   Assert.assertTrue(systemShareTest.isPublic());

   // Verify that isPublic field is set correctly when fetching system.
    TSystem tmpSys = svc.getSystem(rTestUser5, sysId, null, false, false,
                      null, sharedCtxNull, resourceTenantNull, fetchShareInfoTrue);
    Assert.assertTrue(tmpSys.isPublic());

    // **************************  Unsharing system publicly  ***************************
   // Service call
   svc.unshareSystemPublicly(rTestUser5, sysId);
   
   // Test retrieval using specified authn method
   systemShareTest = svc.getSystemShare(rTestUser5, sysId);
   System.out.println("Found item: " + sysId);

   // Verify system share fields
   Assert.assertNotNull(systemShareTest, "System Share information found.");
   Assert.assertFalse(systemShareTest.isPublic());

    // Verify that isPublic field is set correctly when fetching system
    tmpSys = svc.getSystem(rTestUser5, sysId, null, false, false, null,
                           sharedCtxNull, resourceTenantNull, fetchShareInfoTrue);
    Assert.assertFalse(tmpSys.isPublic());
  }

  @Test
  public void testChildSystemBasics() throws TapisException, TapisClientException {
    // create a system that does not allow child systems
    TSystem parentSystem_noChildren = makeParentSystem();
    parentSystem_noChildren.setAllowChildren(false);
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem_noChildren, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertFalse(createdParent.isAllowChildren());

    // test that create child system fails
    String notAllowedMsg = null;
    try {
      svc.createChildSystem(rParentChild1, createdParent.getId(), "testChild", "unitTestUser", "/", null, true, rawDataEmptyJson);
    } catch (IllegalStateException ex) {
      notAllowedMsg = ex.getMessage();
    }

    // create a system that does not allow child systems
    TSystem parentSystem = makeParentSystem();
    createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());

    String childId = "childSystem";
    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    TSystem createdChild = svc.createChildSystem(rParentChild1, createdParent.getId(), childId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
    Assert.assertEquals(createdChild.getParentId(), createdParent.getId());
    Assert.assertEquals(createdChild.getId(), childId);
    Assert.assertEquals(createdChild.getEffectiveUserId(), childEffectiveUserId);
    Assert.assertEquals(createdChild.getRootDir(), childRootDir);
    Assert.assertEquals(createdChild.getOwner(), rParentChild1.getOboUserId());
    checkCommonParentChildAttrs(createdParent, createdChild);

    // delete system
    Assert.assertTrue(svc.checkForSystem(rParentChild1, childId));
    int deletes = svc.deleteSystem(rParentChild1, childId);
    Assert.assertEquals(deletes, 1);
    Assert.assertFalse(svc.checkForSystem(rParentChild1, childId));

    // now create a child system with no id, and assert id is assigned
    TSystem defaultIdChild = svc.createChildSystem(rParentChild1, createdParent.getId(), null, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
    Assert.assertEquals(defaultIdChild.getParentId(), createdParent.getId());
    Assert.assertEquals(defaultIdChild.getId(), createdParent.getId() +  "-" + parentChild1);
    Assert.assertEquals(defaultIdChild.getEffectiveUserId(), childEffectiveUserId);
    Assert.assertEquals(defaultIdChild.getRootDir(), childRootDir);
    Assert.assertEquals(defaultIdChild.getOwner(), parentChild1);
    checkCommonParentChildAttrs(createdParent, defaultIdChild);

    // test patching a child system
    String patchedEffectiveId = "newTestOwner";
    JsonObject patchSystemJson = new JsonObject();
    patchSystemJson.addProperty("effectiveUserId", patchedEffectiveId);
    svc.patchSystem(rParentChild1, defaultIdChild.getId(),
            TapisGsonUtils.getGson().fromJson(patchSystemJson, PatchSystem.class), rawDataEmptyJson);
    TSystem patchedSystem = svc.getSystem(rParentChild1, defaultIdChild.getId(), null, false,
            false, null, null, null, fetchShareInfoFalse);
    Assert.assertEquals(patchedSystem.getEffectiveUserId(), patchedEffectiveId);
  }

  @Test
  public void testParentUpdatesAllChildren() throws TapisException, TapisClientException {
    // create a system that does not allow child systems
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());

    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    TSystem child1 = svc.createChildSystem(rParentChild1, createdParent.getId(), "childId1", childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
    TSystem child2 = svc.createChildSystem(rParentChild1, createdParent.getId(), "childId2", childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
    TSystem child3 = svc.createChildSystem(rParentChild1, createdParent.getId(), "childId3", childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);

    checkCommonParentChildAttrs(createdParent, child1);
    checkCommonParentChildAttrs(createdParent, child2);
    checkCommonParentChildAttrs(createdParent, child3);

    JsonObject patchSystemJson = new JsonObject();
    patchSystemJson.addProperty("description", "This is a new description");
    patchSystemJson.addProperty("port", 12345);
    patchSystemJson.addProperty("host", "unittest.tacc.utexas.edu");

    svc.patchSystem(rParentChild1, parentSystem.getId(),
            TapisGsonUtils.getGson().fromJson(patchSystemJson, PatchSystem.class), rawDataEmptyJson);

    // reread the parent and all children
    TSystem patchedSystem = svc.getSystem(rParentChild1, parentSystem.getId(), null, false,
            false, null, null, null, fetchShareInfoFalse);
    child1 = svc.getSystem(rParentChild1, child1.getId(), null, false, false,
            null, null, null, fetchShareInfoFalse);
    child2 = svc.getSystem(rParentChild1, child2.getId(), null, false, false,
            null, null, null, fetchShareInfoFalse);
    child3 = svc.getSystem(rParentChild1, child3.getId(), null, false, false,
            null, null, null, fetchShareInfoFalse);

    // make sure children were updated when parent was updated.
    checkCommonParentChildAttrs(patchedSystem, child1);
    checkCommonParentChildAttrs(patchedSystem, child2);
    checkCommonParentChildAttrs(patchedSystem, child3);
  }

  @Test
  public void testChangeChildOwner() throws TapisException, TapisClientException {
    // create a system that does not allow child systems
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());
    Set<Permission> permissions = new HashSet<Permission>();
    permissions.add(Permission.READ);
    svc.grantUserPermissions(rParentChild1, parentSystem.getId(), parentChild2, permissions, rawDataEmptyJson);

    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    String childSysId = "childSys-" + UUID.randomUUID().toString();
    TSystem child1 = svc.createChildSystem(rParentChild2, createdParent.getId(), childSysId, childEffectiveUserId,
            childRootDir, null, true, rawDataEmptyJson);
    Assert.assertEquals(child1.getOwner(), parentChild2);

    svc.grantUserPermissions(rParentChild1, parentSystem.getId(), parentChild3, permissions, rawDataEmptyJson);
    svc.changeSystemOwner(rParentChild2, child1.getId(), parentChild3);
    TSystem changedOwnerChild = svc.getSystem(rParentChild3, child1.getId(), null, false,
            false, null, null, null, fetchShareInfoFalse);
    Assert.assertEquals(changedOwnerChild.getOwner(), parentChild3);
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testChangeChildOwnerWithoutPermissionFails() throws TapisException, TapisClientException {
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());

    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    String childSysId = "childSys-" + UUID.randomUUID().toString();
    TSystem child1 = svc.createChildSystem(rParentChild1, createdParent.getId(), childSysId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);

    Assert.assertEquals(child1.getOwner(), parentChild1);
    svc.changeSystemOwner(rParentChild2, child1.getId(), parentChild3);
  }

  @Test
  public void testEnableAndDisableChild() throws TapisException, TapisClientException {
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());
    Set<Permission> permissions = new HashSet<Permission>();

    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    String childSysId = "childSys-" + UUID.randomUUID().toString();
    TSystem createdChild = svc.createChildSystem(rParentChild1, createdParent.getId(), childSysId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
    Assert.assertEquals(createdChild.getId(), childSysId);

    TSystem retrievedChild = svc.getSystem(rParentChild1, childSysId, null, false,
            false, null, null, null, fetchShareInfoFalse);
    Assert.assertTrue(retrievedChild.isEnabled());
    svc.disableSystem(rParentChild1, childSysId);
    retrievedChild = svc.getSystem(rParentChild1, childSysId, null, false, false,
            null, null, null, fetchShareInfoFalse);
    Assert.assertFalse(retrievedChild.isEnabled());
    svc.enableSystem(rParentChild1, childSysId);
    retrievedChild = svc.getSystem(rParentChild1, childSysId, null, false, false,
            null, null, null, fetchShareInfoFalse);
    Assert.assertTrue(retrievedChild.isEnabled());
  }


  @Test(expectedExceptions = ForbiddenException.class)
  public void testCreateChildWithoutPermisssionFails() throws TapisException, TapisClientException {
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());

    // update system to allow children
    String childEffectiveUserId = "unitTestUser";
    String childRootDir = "/childRoot";
    String childSysId = "childSys-" + UUID.randomUUID().toString();
    svc.createChildSystem(rParentChild2, createdParent.getId(), childSysId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
  }

 // ************************************************************************
 // **************************  Private Methods  ***************************
 // ************************************************************************

  @Test
  public void testUnlinkChildren() throws TapisException, TapisClientException {
    // create a parent with 4 children
    TSystem parentSystem = makeParentSystem();
    TSystem createdParent = svc.createSystem(rParentChild1, parentSystem, skipCredCheckTrue, rawDataEmptyJson);
    Assert.assertTrue(createdParent.isAllowChildren());
    List<String> childIds = new ArrayList<>();

    for(int i=0;i<4;i++) {
      String childEffectiveUserId = "unitTestUser";
      String childRootDir = "/childRoot";
      String childSysId = "childSys-" + UUID.randomUUID().toString();
      childIds.add(childSysId);
      TSystem createdChild = svc.createChildSystem(rParentChild1, createdParent.getId(), childSysId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
      Assert.assertEquals(createdChild.getId(), childSysId);
      Assert.assertEquals(createdChild.getParentId(), createdParent.getId());
    }

    // unlinkFromParent
    Assert.assertEquals(svc.unlinkFromParent(rParentChild1, childIds.get(2)), 1);

    List<TSystem>  childSystems = svc.getSystems(rParentChild1, Arrays.asList("parentId.eq." + createdParent.getId()),
            -1, null, 0, null, false, null, fetchShareInfoFalse);
    Assert.assertEquals(childSystems.size(), 3);

    TSystem unlinkedChild = svc.getSystem(rParentChild1, childIds.get(2), null, false,
            false, null, null, null, fetchShareInfoFalse);
    Assert.assertNull(unlinkedChild.getParentId());


    // unlinkChild
    Assert.assertEquals(svc.unlinkChildren(rParentChild1, createdParent.getId(), Arrays.asList(childIds.get(3))), 1);
    childSystems = svc.getSystems(rParentChild1, Arrays.asList("parentId.eq." + createdParent.getId()), -1,
            null, 0, null, false, null, fetchShareInfoFalse);
    Assert.assertEquals(childSystems.size(), 2);

    unlinkedChild = svc.getSystem(rParentChild1, childIds.get(3), null, false, false,
            null, null, null, fetchShareInfoFalse);
    Assert.assertNull(unlinkedChild.getParentId());

    // add 2 more children
    for(int i=0;i<2;i++) {
      String childEffectiveUserId = "unitTestUser";
      String childRootDir = "/childRoot";
      String childSysId = "childSys-" + UUID.randomUUID().toString();
      TSystem createdChild = svc.createChildSystem(rParentChild1, createdParent.getId(), childSysId, childEffectiveUserId, childRootDir, null, true, rawDataEmptyJson);
      Assert.assertEquals(createdChild.getId(), childSysId);
      Assert.assertEquals(createdChild.getParentId(), createdParent.getId());
    }

    // unlinkAllChildren
    Assert.assertEquals(svc.unlinkAllChildren(rParentChild1, createdParent.getId()), 4);
    childSystems = svc.getSystems(rParentChild1, Arrays.asList("parentId.eq." + createdParent.getId()), -1,
            null, 0, null, false, null, fetchShareInfoFalse);
    Assert.assertEquals(childSystems.size(), 0);
  }



  // Retrieve and display history for manual checking of system history records
 private void displaySystemHistory(ResourceRequestUser rUser, String systemId) throws TapisException, TapisClientException
 {
   // Retrieve and display history for manual checking of display
   List<SystemHistoryItem> systemHistory = svc.getSystemHistory(rUser, systemId);
   Assert.assertNotNull(systemHistory);
   System.out.println("===============================================================================");
   System.out.printf("History for system: %s Number of history records: %d%n", systemId, systemHistory.size());
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
 }

  // Check password and effective user as part of credentials check
  private void checkCredPasswordAndEffectiveUser(TSystem sys, String password, String user, String effUser)
  {
    Credential cred = sys.getAuthnCredential();
    Assert.assertNotNull(cred, "AuthnCredential should not be null for user: " + user);
    Assert.assertEquals(cred.getAuthnMethod(), AuthnMethod.PASSWORD, "Retrieved authnMethod incorrect");
    Assert.assertNotNull(cred.getPassword(), "AuthnCredential password should not be null for user: " + user);
    Assert.assertEquals(cred.getPassword(), password, "Retrieved password incorrect");
    Assert.assertEquals(sys.getEffectiveUserId(), effUser, "Incorrect effectiveUserId");

  }

  private String getParentSysName(int num) {
    return sysNamePrefix + "_parentSystem_" + num;
  }

  private synchronized TSystem makeParentSystem() {
    // Suffix which should be unique for each system within each integration test
    TSystem system;
    String indexString = "parentSys" + parentSystems.size();
    String name = getParentSysName(parentSystems.size());
    String hostName = "host" + indexString + ".test.org";
    // Constructor initializes all attributes except for JobCapabilities and Credential
    system = new TSystem(-1, tenantName, name, description1 + indexString, TSystem.SystemType.LINUX, null,
            hostName, isEnabledTrue, effectiveUserId1 + indexString, prot1.getAuthnMethod(), "bucket" + indexString, "/root" + indexString,
            prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
            null,
            canExecTrue, jobRuntimes1, "jobWorkDir" + indexString, jobEnvVariables1, jobMaxJobs1, jobMaxJobsPerUser1,
            canRunBatchTrue, enableCmdPrefixTrue, mpiCmd1, batchScheduler1, logicalQueueList1, queueA1.getName(),
            batchSchedulerProfile1, capList1, tags1, notes1, importRefId1, uuidNull, isDeletedFalse,
            allowChildrenTrue, parentIdNull, createdNull, updatedNull);
    system.setJobRuntimes(jobRuntimes1);
    system.setBatchLogicalQueues(logicalQueueList1);
    system.setJobCapabilities(capList1);
    parentSystems.add(system);
    return system;
  }
}
