package edu.utexas.tacc.tapis.systems.migrate;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.service.ServiceClientsFactory;
import edu.utexas.tacc.tapis.systems.service.ServiceContextFactory;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_SECRET;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_ACCESS_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PASSWORD;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PRIVATE_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_PUBLIC_KEY;
import static edu.utexas.tacc.tapis.systems.model.Credential.SK_KEY_REFRESH_TOKEN;
import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;
import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;
import static edu.utexas.tacc.tapis.systems.service.AuthUtils.PERM_SPEC_TEMPLATE;

/*
 * Perform a one-time java based non-DB migration of data for the Systems service.
 *
 * This implementation is for migration to version 1.2.0. This implementation migrates System credential
 * secrets in SK from old paths to paths that include static/dynamic.
 *
 * The program should be started up in the same manner as the Systems service api application.
 * Typically, it is run only once, although significant effort should be made to ensure that each
 * incarnation of this job is idempotent since it may have to be run more than once if there are issues.
 *
 * The Systems service api should be shut down before running this job. The job output (the logs) should
 * be checked to confirm there were no errors.
 *
 * This program will need to be updated for migration to a specific version of the Systems service.
 *
 * By default, it is a dry run, no permanent changes are made.
 * To apply changes use option --apply or set env variable TAPIS_MIGRATE_JOB_APPLY to "apply_changes"
 * The use of an env var is supported (RuntimeParameters.java) because this appears to be the easiest way to pass
 *   in information to the program when running via a kubernetes job. A special string is used for the env var
 *   rather than a boolean because all attempts failed when attempting to use true/false for a kubernetes job.
 * 
 * Based on SKAdmin from tapis-java repository: https://github.com/tapis-project/tapis-java
 */
public class MigrateJob
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(MigrateJob.class);

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final MigrateJobParameters _parms;

  private SystemsDao dao;
  private ServiceClients serviceClients;

  private boolean isApply;
  private boolean envApply;
  private String msgPrefix;
  private String siteAdminTenantId;

  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public MigrateJob(MigrateJobParameters parms)
  {
    // Parameters cannot be null.
    if (parms == null) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "MigrateJob", "parms");
      _log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    _parms = parms;
  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /**
   * Main method
   * @param args the command line parameters
   * @throws Exception on error
   */
  public static void main(String[] args) throws Exception
  {
    // Parse the command line parameters.
    MigrateJobParameters parms = null;
    parms = new MigrateJobParameters(args);

    // Create an instance, set it up and run the migration
    MigrateJob migrateJob = new MigrateJob(parms);
    // Note: setUp sets envApply from RuntimeParameters.
    migrateJob.setUp();
    migrateJob.migrate();
  }

  /**
   * Perform the migration
   * @throws TapisException on error
   */
  public void migrate() throws TapisException, TapisClientException
  {
    // Only apply changes if asked to do so
    isApply = _parms.isApply || envApply;
    msgPrefix = "DRY-RUN MIGRATE:";
    if (isApply) msgPrefix = "APPLY MIGRATE:";
    migrateAllCredentialsToStaticDynamic();
  }

  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */

  /*
   * Migrate all credentials to a static or dynamic path in SK
   * This is a one time migration for Tapis Systems version 1.1.5
   * Iterate over all tenants, all systems in a tenant and all users of a system
   */
  private void migrateAllCredentialsToStaticDynamic() throws TapisException, TapisClientException
  {
    System.out.printf("%s START Migrating secrets to static/dynamic paths%n", msgPrefix);
    // We will need to call SK to find users of a system, remove secrets and re-write them to new paths.
    SKClient skClient = getSKClient();
    // Get the list of tenants and iterate over them
    var tenants = new TreeMap<String, Tenant>(TenantManager.getInstance().getTenants());
    for (String tenantId : tenants.keySet())
    {
      System.out.printf("%s Checking for systems. Tenant %s%n", msgPrefix, tenantId);
      // Fetch all systems including deleted items and iterate
      var systemIdSet = new TreeSet<String>(dao.getSystemIDs(tenantId, true));
      for (String systemId : systemIdSet)
      {
        System.out.printf("%s Tenant %s System: %s%n", msgPrefix, tenantId, systemId);
        // We will need info from the system, so fetch it
        TSystem system = dao.getSystem(tenantId, systemId, true);
        // Determine if effUser is static or dynamic
        boolean isStaticEffectiveUser = !system.getEffectiveUserId().equals(APIUSERID_VAR);

        // Use Security Kernel client to find all users with perms associated with the system.
        String permSpec = String.format(PERM_SPEC_TEMPLATE, tenantId, "%", systemId);
        var userNames = new TreeSet<String>(skClient.getUsersWithPermission(tenantId, permSpec));
        // Iterate over all users
        for (String userName : userNames)
        {
          // Determine targetUser for credential. If static then effectiveUserId, else the Tapis user
          String targetUser = isStaticEffectiveUser ? system.getEffectiveUserId() : userName;
          System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s isStatic: %s%n", msgPrefix,
                            tenantId, systemId, userName, targetUser, isStaticEffectiveUser);
          // Get credential with all secrets from old path
          Credential cred = getAllOldSecretsForTargetUser(skClient, tenantId, systemId, userName, targetUser);
          if (cred == null)
          {
            System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s No old secrets%n", msgPrefix,
                    tenantId, systemId, userName, targetUser);
          }
          else
          {
            System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got old secrets%n", msgPrefix,
                    tenantId, systemId, userName, targetUser);
            // Write secrets to the new path
            createCredentialAtNewPath(skClient, tenantId, systemId, userName, targetUser, cred, isStaticEffectiveUser);
            System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Wrote new secrets%n", msgPrefix,
                    tenantId, systemId, userName, targetUser);
            // Remove the old secrets
            deleteOldCredential(skClient, tenantId, systemId, userName, targetUser, cred);
            System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Removed old secrets%n", msgPrefix,
                    tenantId, systemId, userName, targetUser);
          }
        }
      }
    }
  }

  /*
   * Get credential for targetUser with all secrets filled in
   */
  private Credential getAllOldSecretsForTargetUser(SKClient skClient, String tenantId, String systemId,
                                                   String userName, String targetUser)
          throws TapisClientException
  {
    Credential credential = null;
    boolean secretsFound = false;
    // Construct basic SK secret parameters
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);

    // Fill in systemId and targetUser for the path to the secret.
    // Set tenant, system and user associated with the secret.
    // These values are used to build the vault path to the secret.
    sParms.setTenant(tenantId).setSysId(systemId).setSysUser(targetUser);

    // NOTE: Next line is needed for the SK call. Not clear if it should be targetUser, serviceUserId, oboUser.
    //       If not set then the first getAuthnCred in SystemsServiceTest.testUserCredentials
    //          fails. But it appears the value does not matter. Even an invalid userId appears to be OK.
    // For migration there is no oboUser, use the service name
    sParms.setUser(TapisConstants.SERVICE_NAME_SYSTEMS);

    // We will need to fetch each keyType in turn and fill in the secrets before we can create a Credential
    //   with all secrets populated
    SkSecret skSecret;
    Map<String, String> dataMap;
    var dataMapFull = new HashMap<String, String>();

    // Fetch each key type in turn
    // NOTE ignore all TapisClientExceptions because SK throws the exception if the secret does not exist.
    // PASSWORD
    sParms.setKeyType(KeyType.password);
    skSecret = null;
    try { skSecret = skClient.readSecret(sParms); } catch (TapisClientException e) {}
    if (skSecret != null)
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap != null)
      {
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got password%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
        dataMapFull.put(SK_KEY_PASSWORD, dataMap.get(SK_KEY_PASSWORD));
        secretsFound = true;
      }
    }
    // SSH_KEY
    sParms.setKeyType(KeyType.sshkey);
    skSecret = null;
    try { skSecret = skClient.readSecret(sParms); } catch (TapisClientException e) {}
    if (skSecret != null)
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap != null)
      {
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got private key%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
        dataMapFull.put(SK_KEY_PRIVATE_KEY, dataMap.get(SK_KEY_PRIVATE_KEY));
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got public key%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
        dataMapFull.put(SK_KEY_PUBLIC_KEY, dataMap.get(SK_KEY_PUBLIC_KEY));
        secretsFound = true;
      }
    }
    // ACCESS_KEY
    sParms.setKeyType(KeyType.accesskey);
    skSecret = null;
    try { skSecret = skClient.readSecret(sParms); } catch (TapisClientException e) {}
    if (skSecret != null)
    {
      dataMap = skSecret.getSecretMap();
      if (dataMap != null)
      {
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got access key%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
        dataMapFull.put(SK_KEY_ACCESS_KEY, dataMap.get(SK_KEY_ACCESS_KEY));
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Got access secret%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
        dataMapFull.put(SK_KEY_ACCESS_SECRET, dataMap.get(SK_KEY_ACCESS_SECRET));
        secretsFound = true;
      }
    }

    // If secrets found create a credential
    if (secretsFound)
    {
      credential = new Credential(null, null,
              dataMapFull.get(SK_KEY_PASSWORD),
              dataMapFull.get(SK_KEY_PRIVATE_KEY),
              dataMapFull.get(SK_KEY_PUBLIC_KEY),
              dataMapFull.get(SK_KEY_ACCESS_KEY),
              dataMapFull.get(SK_KEY_ACCESS_SECRET),
              dataMapFull.get(SK_KEY_ACCESS_TOKEN),
              dataMapFull.get(SK_KEY_REFRESH_TOKEN),
              null); // No support yet for ssh certificates
    }
    return credential;
  }

  /*
   * Create secrets at new path including dynamic/static in path
   */
  private void createCredentialAtNewPath(SKClient skClient, String tenantId,  String systemId, String userName,
                                         String targetUser, Credential credential, boolean isStatic)
          throws TapisClientException
  {
    // Construct basic SK secret parameters including tenant, system and Tapis user for credential
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretWriteParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    // Fill in systemId and targetUserPath for the path to the secret.
    String targetUserPath = getTargetUserSecretPath(targetUser, isStatic);
    sParms.setSysId(systemId).setSysUser(targetUserPath);
    System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s TargetPath: %s%n", msgPrefix,
                      tenantId, systemId, userName, targetUser, targetUserPath);
    Map<String, String> dataMap;
    // Check for each secret type and write values if they are present
    // Note that multiple secrets may be present.
    // Store password if present
    if (!StringUtils.isBlank(credential.getPassword()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.password);
      dataMap.put(SK_KEY_PASSWORD, credential.getPassword());
      sParms.setData(dataMap);
      // First 2 parameters correspond to tenant and user from request payload
      // Tenant is used in constructing full path for secret, user is not used.
      // For migration there is no oboUser, use the service name
      if (isApply) skClient.writeSecret(tenantId, TapisConstants.SERVICE_NAME_SYSTEMS, sParms);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Wrote password%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
    }
    // Store PKI keys if both present
    if (!StringUtils.isBlank(credential.getPublicKey()) && !StringUtils.isBlank(credential.getPublicKey()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.sshkey);
      dataMap.put(SK_KEY_PUBLIC_KEY, credential.getPublicKey());
      dataMap.put(SK_KEY_PRIVATE_KEY, credential.getPrivateKey());
      sParms.setData(dataMap);
      if (isApply) skClient.writeSecret(tenantId, TapisConstants.SERVICE_NAME_SYSTEMS, sParms);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Wrote ssh keys%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
    }
    // Store Access key and secret if both present
    if (!StringUtils.isBlank(credential.getAccessKey()) && !StringUtils.isBlank(credential.getAccessSecret()))
    {
      dataMap = new HashMap<>();
      sParms.setKeyType(KeyType.accesskey);
      dataMap.put(SK_KEY_ACCESS_KEY, credential.getAccessKey());
      dataMap.put(SK_KEY_ACCESS_SECRET, credential.getAccessSecret());
      sParms.setData(dataMap);
      if (isApply) skClient.writeSecret(tenantId, TapisConstants.SERVICE_NAME_SYSTEMS, sParms);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Wrote access key/secret%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
    }
  }

  /**
   * Delete all secrets from old path
   */
  private void deleteOldCredential(SKClient skClient, String tenantId, String systemId,
                                   String userName, String targetUser, Credential cred)
          throws TapisClientException
  {
    System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Removing old secrets%n", msgPrefix,
            tenantId, systemId, userName, targetUser);
    var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
    sMetaParms.setTenant(tenantId).setUser(TapisConstants.SERVICE_NAME_SYSTEMS);
    sMetaParms.setSysId(systemId).setSysUser(targetUser);

    // Construct basic SK secret parameters and attempt to destroy each type of secret if it was set.
    // If destroy attempt throws an exception then log a message and continue.
    // PASSWORD
    if (!StringUtils.isBlank(cred.getPassword()))
    {
      sMetaParms.setKeyType(KeyType.password);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Attempting destroy for PASSWORD%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
      try
      {
        if (isApply) skClient.destroySecretMeta(sMetaParms);
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Secret destroyed: PASSWORD%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
      }
      catch (TapisClientException e) { System.out.printf(e.getMessage()); }
    }
    // PKI_KEYS
    if (!StringUtils.isBlank(cred.getPrivateKey()))
    {
      sMetaParms.setKeyType(KeyType.sshkey);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Attempting destroy for PKI_KEYS%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
      try
      {
        if (isApply) skClient.destroySecretMeta(sMetaParms);
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Secret destroyed: PKI_KEYS%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
      }
      catch (TapisClientException e) { System.out.printf(e.getMessage()); }
    }
    // ACCESS_KEY
    if (!StringUtils.isBlank(cred.getAccessKey()))
    {
      sMetaParms.setKeyType(KeyType.accesskey);
      System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Attempting destroy for ACCESS_KEY%n", msgPrefix,
                        tenantId, systemId, userName, targetUser);
      try
      {
        if (isApply) skClient.destroySecretMeta(sMetaParms);
        System.out.printf("%s Tenant %s System: %s User: %s TargetUser: %s Secret destroyed: ACCESS_KEY%n", msgPrefix,
                tenantId, systemId, userName, targetUser);
      }
      catch (TapisClientException e) { System.out.printf(e.getMessage()); }
    }
  }

  /*
   * Return segment of secret path for target user, including static or dynamic scope
   * Note that SK uses + rather than / to create sub-folders.
   */
  static private String getTargetUserSecretPath(String targetUser, boolean isStatic)
  {
    return String.format("%s+%s", isStatic ? "static" : "dynamic", targetUser);
  }

  /*
   * Setup for making service and dao calls.
   */
  void setUp() throws Exception
  {
    // Setup for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    ServiceLocatorUtilities.bind(locator, new AbstractBinder()
    {
      @Override
      protected void configure()
      {
        bind(SystemsServiceImpl.class).to(SystemsService.class);
        bind(SystemsServiceImpl.class).to(SystemsServiceImpl.class);
        bind(SystemsDaoImpl.class).to(SystemsDao.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class);
      }
    });
    locator.inject(this);

    RuntimeParameters runParms = RuntimeParameters.getInstance();
    // Initialize TenantManager and services
    String url = runParms.getTenantsSvcURL();
    TenantManager.getInstance(url).getTenants();

    String siteId = runParms.getSiteId();
    siteAdminTenantId = TenantManager.getInstance(url).getSiteAdminTenantId(siteId);
    // Initialize services
    SystemsServiceImpl svcImpl = locator.getService(SystemsServiceImpl.class);
    svcImpl.initService(siteId, siteAdminTenantId, RuntimeParameters.getInstance());
    dao = new SystemsDaoImpl();
    serviceClients = ServiceClients.getInstance();
    envApply = runParms.isMigrateJobApply();

    // Log our config
    System.out.println(runParms.getRuntimeInfo());
  }

  /**
   * Get Security Kernel client
   * Note: Systems service always calls SK as itself.
   *       tenant = siteAdminTenant
   *       user = systems
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient() throws TapisException
  {
    SKClient skClient;
    try
    {
      skClient = serviceClients.getClient(TapisConstants.SERVICE_NAME_SYSTEMS, siteAdminTenantId, SKClient.class);
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, siteAdminTenantId,
                                   TapisConstants.SERVICE_NAME_SYSTEMS);
      throw new TapisException(msg, e);
    }
    return skClient;
  }
}
