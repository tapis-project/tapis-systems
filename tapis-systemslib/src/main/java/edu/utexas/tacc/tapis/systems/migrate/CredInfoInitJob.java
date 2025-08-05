package edu.utexas.tacc.tapis.systems.migrate;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecretVersionMetadata;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import org.apache.commons.lang3.Strings;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.config.RuntimeParameters;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.dao.SystemsDaoImpl;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.service.AuthUtils;
import edu.utexas.tacc.tapis.systems.service.CredUtils;
import edu.utexas.tacc.tapis.systems.service.ServiceClientsFactory;
import edu.utexas.tacc.tapis.systems.service.ServiceContextFactory;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import edu.utexas.tacc.tapis.systems.service.SystemsServiceImpl;
import edu.utexas.tacc.tapis.systems.service.SysUtils;

import static edu.utexas.tacc.tapis.systems.model.Credential.TOP_LEVEL_SECRET_NAME;

/*
 * CredInfoInitJob used to initialize the CredInfo table based on the records in Vault and SK.
 *
 * This implementation is for initializing entries in the table systems_cred_info based on information in
 * the SK vault. The table systems_cred_info was introduced in version 1.9.0. Through version 1.9.1
 * the table systems_cred_info was not used yet to track all credential metadata. It was used in the same way
 * as the previous incarnation of the table when it was named systems_login_user.
 *
 * This implementation initializes the credential metadata records based on entries in SK vault.
 * Records in systems_cred_info are created/updated as needed.
 * See CredInfoInitJobParameters for options.
 * Some of the code copied from the SkUtility program in repo tapis-security.
 *
 * The program should be started up in the same manner as the Systems service api application.
 * Typically, it is run only once, although significant effort should be made to ensure that each
 * incarnation of this job is idempotent since it may have to be run more than once if there are issues.
 *
 * Please see docker image build script release/docker_build_credinfoinitjob.sh and other related files in release dir.
 *
 * *************** WARNING ***************
 *  The Systems service api must be shut down before running this job to avoid the possibility of data
 *  corruption. The job output (the logs) should be checked to confirm there were no errors.
 * *************** WARNING ***************
 *
 * By default, it is a dry run, no permanent changes are made.
 * To apply changes use option --apply or set env variable TAPIS_MIGRATE_JOB_APPLY to "apply_changes"
 * The use of an env var is supported (RuntimeParameters.java) because this appears to be the easiest way to pass
 *   in information to the program when running via a kubernetes job. A special string is used for the env var
 *   rather than a boolean because all attempts failed when attempting to use true/false for a kubernetes job.
 * 
 * Based on MigrateJob class.
 */
public class CredInfoInitJob
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(CredInfoInitJob.class);

  // ----- Constants related to paths in Vault -----
  // Base URL path for walking tree to find Tapis meta records in Vault.
  private static final String VAULT_BASE_URL_META = "v1/secret/metadata";
  // Base URL path for walking tree to find Tapis data records in Vault.
  private static final String VAULT_BASE_URL_DATA = "v1/secret/data";
  // Root of the tapis secrets subtree in Vault.
  private static final String TAPIS_ROOT = "tapis";
  // Path element for tenants in Vault.
  private static final String TENANT_ROOT = String.format("%s/tenant", TAPIS_ROOT);
  // Path element for systems in Vault.
  private static final String SYSTEM_ELEMENT = "system";
  // Path element for systems secret suffix in Vault.
  private static final String SYSTEM_SECRET_SUFFIX = "S1";
  // Path element for users in Vault.
  private static final String USER_ELEMENT = "user";
  // Delimiter for user field is +
  private static final Pattern SPLIT_PLUS_PATTERN = Pattern.compile("\\+");

  /* ********************************************************************** */
  /*                                 Records                                */
  /* ********************************************************************** */
  // Wrapper for secret info metadata.
  public record SecretMetaInfo(String tenantId, String systemId, String targetUser, boolean isStatic,
                               boolean hasPassword, boolean hasPkiKeys, boolean hasAccessKey, boolean hasToken,
                               boolean hasTmsKeys) {}

  // The valid types as expected on input.
  enum SKVaultSecretKeyType {sshkey, password, accesskey, token, tmskey, cert}

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final CredInfoInitJobParameters _parms;
  private final HttpClient _httpClient; // Client for calls to Vault
  private CredUtils credUtils;
  private ServiceClients serviceClients;
  private boolean isApply;
  private boolean envApply;
  private String msgPrefix;
  private String siteAdminTenantId;
  private static ResourceRequestUser rUserSvc;
  SystemsDao dao;

  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  public CredInfoInitJob(CredInfoInitJobParameters parms)
  {
    // Parameters cannot be null.
    if (parms == null) {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "CredInfoInitJob", "parms");
      _log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    _parms = parms;
    _httpClient  = HttpClient.newHttpClient();
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
    CredInfoInitJobParameters parms = new CredInfoInitJobParameters(args);

    // Create an instance, set it up and run the migration
    CredInfoInitJob credInfoInitJob = new CredInfoInitJob(parms);
    // Note: setUp sets envApply from RuntimeParameters.
    credInfoInitJob.setUp();
    credInfoInitJob.run();
  }

  /**
   * Perform the migration
   * @throws Exception on error
   */
  public void run() throws Exception
  {
    // Only apply changes if asked to do so
    isApply = _parms.isApply || envApply;
    msgPrefix = "DRY-RUN CREDINFOINIT:";
    if (isApply) msgPrefix = "APPLY CREDINFOINIT:";
    initializeAllCredentialInfoRecords();
  }

  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */

  /*
   * Initialize all CredentialInfo records in the table systems_cred_info based on records in SK vault.
   * Records are created or updated as needed.
   * Walk the secrets tree in Vault iterating over all tenants, all systems in a tenant and all users of a system
   * Synchronized SK secret metadata with data in systems_cred_info table.
   */
  private void initializeAllCredentialInfoRecords() throws Exception
  {
    int totalSystemsProcessed = 0;
    int totalUsersProcessed = 0;
    int totalLegacyRecords = 0;
    System.out.printf("%s START Initialize CredentialInfo records paths%n", msgPrefix);
    // Check status of Vault.
    info("Checking status of Vault");
    checkVaultStatus();
    List<String> tenants = getAllTenants();
    info("Tenants count based on vault records: " + tenants.size());

    // Iterate over tenants
    for (String tenant: tenants)
    {
      debug("Processing tenant: " + tenant);
      // Figure out systems to process
      List<String> systems = getAllSystemsForTenant(tenant);
      debug(" ******** Systems count based on vault records: " + systems.size() + " ********");
      // Iterate over systems
      for (String system : systems)
      {
        debug(String.format("Found system. Tenant: %s System: %s", tenant, system));
        // Get all users under system
        List<String> users = getUsers(tenant, system);
        debug("******** Users count based on vault records: " + users.size() + " ********");
        boolean isLegacy;
        // Iterate over users
        for(String user :users)
        {
          isLegacy = !Strings.CI.startsWith(user,"static+") && !Strings.CI.startsWith(user,"dynamic+");
          info(String.format("Processing record. Tenant: %s System: %s User field: %s isLegacy: %b",
                             tenant, system, user, isLegacy));
          initCredInfoRecord(tenant, system, user);
          if (isLegacy)
          {
            String fullPath = String.format("%s/%s/%s/%s/%s/%s/%s/%s",
                    _parms.vurl,VAULT_BASE_URL_META,TENANT_ROOT,tenant,SYSTEM_ELEMENT,system,USER_ELEMENT,user);
            info("Legacy record vault path: " + fullPath);
            totalLegacyRecords++;
          }
        }
        totalUsersProcessed += users.size();
      }
      totalSystemsProcessed += systems.size();
    }
    info("******** Total Tenants processed: " + tenants.size() + " ********");
    info("******** Total Systems processed: " + totalSystemsProcessed + " ********");
    info("******** Total Users processed  : " + totalUsersProcessed + " ********");
    info("******** Total Legacy user records processed  : " + totalLegacyRecords + " ********");
    System.out.printf("%s END Initialize CredentialInfo records paths%n", msgPrefix);
  }

  /*
   * Setup for making service and dao calls.
   */
  private void setUp() throws Exception
  {
    System.out.println("Starting setup");
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
        bind(SystemsDaoImpl.class).to(SystemsDaoImpl.class);
        bind(SysUtils.class).to(SysUtils.class);
        bind(AuthUtils.class).to(AuthUtils.class);
        bind(CredUtils.class).to(CredUtils.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class);
      }
    });
    locator.inject(this);
    RuntimeParameters runParms = RuntimeParameters.getInstance();
    // Initialize TenantManager and services
    String url = runParms.getTenantsSvcURL();
    System.out.println("Getting tenants");
    TenantManager.getInstance(url).getTenants();

    String siteId = runParms.getSiteId();
    String svcName = TapisConstants.SERVICE_NAME_SYSTEMS;
    String svcTenant = runParms.getServiceAdminTenant();
    siteAdminTenantId = TenantManager.getInstance(url).getSiteAdminTenantId(siteId);
    // Initialize services
    System.out.println("Init dao and svc classes");
    dao = locator.getService(SystemsDaoImpl.class);
    if (dao.checkDB() != null) throw new Exception("DB CHECK FAILED");
    SystemsServiceImpl svcImpl = locator.getService(SystemsServiceImpl.class);
    svcImpl.initService(siteId, siteAdminTenantId, RuntimeParameters.getInstance());
    credUtils = locator.getService(CredUtils.class);
    serviceClients = ServiceClients.getInstance();
    envApply = runParms.isMigrateJobApply();
    var authUser = new AuthenticatedUser(svcName, svcTenant, TapisThreadContext.AccountType.service.name(), null,
          svcName, svcTenant, null, siteId, null);
    rUserSvc = new ResourceRequestUser(authUser);
    // Log our config
    System.out.println(runParms.getRuntimeInfo());
  }

  /*
   * Get Security Kernel client
   * Note: Systems service always calls SK as itself.
   *       tenant = siteAdminTenant
   *       user = systems
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

  /*
   * getTenants
   * A LIST on tapis/tenant will yield a list of all tenants under that path
   * Exit with a 1 on error.
   * If we cannot get tenants then it is an unrecoverable error.
   */
  private List<String> getAllTenants() throws Exception
  {
    List<String> tenants = new ArrayList<>();
    // Build the full path
    String fullPath = String.format("%s/%s/%s",_parms.vurl,VAULT_BASE_URL_META,TENANT_ROOT);
    // Make the request to list
    HttpResponse<String> resp = sendListRequest(fullPath);
    // Check return code.
    int rc = resp.statusCode();
    debug("Received HTTP status code: " + rc);
    if (rc == 404)
    {
      // This should never happen. It means no tenants.
      warn("No tenants found");
      return tenants;
    }
    else if (rc >= 300)
    {
      // Looks like an error.
      errorExit("Received http status code " + rc + " on LIST request to vault. FullPath: " + fullPath);
    }

    // Intermediate node. Response body should look like this: {"data": {"keys": ["foo", "foo/"]}}.
    // Parse the response to get the keys
    tenants = getKeysFromResponse(resp);
    return tenants;
  }

  /*
   * getSystems
   * A LIST on tapis/tenant/<tenant_id>/system will yield a list of all systems under that path
   */
  private List<String> getAllSystemsForTenant(String tenant) throws Exception
  {
    List<String> systems = new ArrayList<>();
    // Build the full path
    String fullPath = String.format("%s/%s/%s/%s/%s",_parms.vurl,VAULT_BASE_URL_META,TENANT_ROOT,tenant,SYSTEM_ELEMENT);
    // Make the request to list
    HttpResponse<String> resp = sendListRequest(fullPath);
    // Check return code.
    int rc = resp.statusCode();
    debug("Received HTTP status code: " + rc);
    if (rc == 404)
    {
      // Indicates no systems for this tenant. This could happen.
      warn("No systems found for tenant. Tenant: " + tenant);
      return systems;
    }
    else if (rc >= 300)
    {
      // Looks like an error.
      errorExit("Received http status code " + rc + " on LIST request to vault. FullPath: " + fullPath);
    }
    // Intermediate node. Response body should look like this: {"data": {"keys": ["foo", "foo/"]}}.
    // Parse the response to get the keys
    systems = getKeysFromResponse(resp);
    debug("Number of systems: " + systems.size());
    return systems;
  }

  /*
   * getUsers
   * A LIST on tapis/tenant/<tenant_id>/system/<system_id>/user will yield a list of all users under that path
   */
  private List<String> getUsers(String tenant, String system) throws Exception
  {
    List<String> users = new ArrayList<>();
    // Build the full path
    String fullPath =
          String.format("%s/%s/%s/%s/%s/%s/%s/",
                _parms.vurl,VAULT_BASE_URL_META,TENANT_ROOT,tenant,SYSTEM_ELEMENT,system,USER_ELEMENT);
    // Make the request to list
    HttpResponse<String> resp = sendListRequest(fullPath);
    // Check return code.
    int rc = resp.statusCode();
    debug("Received HTTP status code: " + rc);
    if (rc == 404)
    {
      // Indicates no systems for this tenant. This could happen.
      warn("No systems found for tenant. Tenant: " + tenant);
      return users;
    }
    else if (rc >= 300)
    {
      // Looks like an error.
      errorExit("Received http status code " + rc + " on LIST request to vault. FullPath: " + fullPath);
    }
    // Intermediate node. Response body should look like this: {"data": {"keys": ["foo", "foo/"]}}.
    // Parse the response to get the keys
    users = getKeysFromResponse(resp);
    debug("Number of users: " + users.size());
    var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
    if (jsonObj == null) errorExit("Unable to create Json object from response.");
    // NOTE: Somehow (at least in DEV) we end up with a few paths that end in static/ or dynamic/
    //   So they look like legacy records, but apparently there are no secrets in those paths because the paths
    //   remain after the step to remove the legacy record.
    //    error("************************************ RESPONSE USERS: " + jsonObj.toString());
    return users;
  }

  /* ---------------------------------------------------------------------- */
  /* checkVaultStatus:                                                      */
  /* ---------------------------------------------------------------------- */
  private void checkVaultStatus() throws Exception
  {
    // Get vault information.
    String baseUrl = _parms.vurl;
    String tok = _parms.vtok;

    // Issue request.
    HttpRequest request = HttpRequest.newBuilder()
          .uri(new URI(baseUrl + "/v1/sys/health"))
          .headers("X-Vault-Token", tok, "Accept", "application/json",
                "Content-Type", "application/json")
          .GET()
          .build();
    HttpResponse<String> resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Check status code.
    int rc = resp.statusCode();
    if (rc >= 300) {
      String msg = "Received http status code " + rc + " on request to " +
            "vault: " + request.uri().toString() + ".";
      throw new RuntimeException(msg);
    }

    // Parse the response body.
    var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
    if (jsonObj == null) {
      String msg = "Received http status code " + rc + " and no response content " +
            "on request to vault: " + request.uri().toString() + ".";
      throw new RuntimeException(msg);
    }
    boolean sealed = jsonObj.get("sealed").getAsBoolean();
    String version = jsonObj.get("version").getAsString();
    info("Vault at " + baseUrl + " is at version " + version +
          " and is " + (sealed ? "" : "not ") + "sealed.");
    if (sealed) {
      String msg = "Unable to continue because vault at " + baseUrl + " is sealed.";
      throw new RuntimeException(msg);
    }
  }

  /**
   * Send http LIST request
   * @param fullPath - url for request
   * @return http response
   */
  private HttpResponse<String> sendListRequest(String fullPath)
        throws URISyntaxException, IOException, InterruptedException
  {
    var reqUri = new URI(fullPath);
    debug("Sending LIST request to: " + reqUri);
    HttpRequest request = HttpRequest.newBuilder().uri(reqUri)
          .headers("X-Vault-Token", _parms.vtok, "Accept", "application/json",
                "Content-Type", "application/json")
          .method("LIST", HttpRequest.BodyPublishers.noBody())
          .build();
    return _httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /**
   * Get keys from http LIST response
   * @param resp - response from request
   * @return List of keys as strings with trailig slash (/) removed
   */
  private List<String> getKeysFromResponse(HttpResponse<String> resp)
  {
    List<String> keysAsString = new ArrayList<>();
    var jsonObj = TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
    if (jsonObj == null) errorExit("Unable to create Json object from response.");
    var dataObj = jsonObj.get("data");
    if (dataObj == null) errorExit("Did not find data field in json object from response.");
    var data = dataObj.getAsJsonObject();
    var keysObj = data.get("keys");
    if (keysObj == null) errorExit("Did not find keys field in json object from response.");
    var keys = data.get("keys").getAsJsonArray();
    // Create the list of keys
    for (int i = 0; i < keys.size(); i++)
    {
      String keyStr = Strings.CS.removeEnd(keys.get(i).getAsString(), "/");
      keysAsString.add(keyStr);
    }
    return keysAsString;
  }

  /*
   * getSecretMetadata
   * Determine secret metadata by making calls to vault under path v1/secret/data/
   */
  private SecretMetaInfo getSecretMetadata(String tenant, String system, String userField, String targetUser, boolean isStatic)
        throws Exception
  {
    // Build the base path for secret data
    String baseSecretDataPath = String.format("%s/%s/%s/%s/%s/%s/%s/%s",
          _parms.vurl,VAULT_BASE_URL_DATA,TENANT_ROOT,tenant,SYSTEM_ELEMENT,system,USER_ELEMENT,userField);
    // For each secret type build the path and attempt to check for data
    boolean hasPassword = checkSecretData(baseSecretDataPath, SKVaultSecretKeyType.password);
    boolean hasPkiKeys = checkSecretData(baseSecretDataPath, SKVaultSecretKeyType.sshkey);
    boolean hasAccessKey = checkSecretData(baseSecretDataPath, SKVaultSecretKeyType.accesskey);
    boolean hasToken = checkSecretData(baseSecretDataPath, SKVaultSecretKeyType.token);
    boolean hasTmsKeys = checkSecretData(baseSecretDataPath, SKVaultSecretKeyType.tmskey);
    return new SecretMetaInfo(tenant, system, targetUser, isStatic, hasPassword, hasPkiKeys, hasAccessKey, hasToken, hasTmsKeys);
  }

  /*
   * checkSecretData
   * Determine if secret of given type is present.
   */
  private boolean checkSecretData(String baseSecretDataPath, SKVaultSecretKeyType keytype)
        throws Exception
  {
    // Build the full path to the secret
    String fullPath = String.format("%s/%s/%s", baseSecretDataPath, keytype.toString(), SYSTEM_SECRET_SUFFIX);

    // Make the GET request
    // Parse the response body and return the value of the data object.
    // The secrets should look like:  "data": {"data": {"foo": "bar"}, "metadata": {..}}
    HttpRequest request;
    HttpResponse<String> resp;
    var reqUri = new URI(fullPath);
    debug("Sending GET request to: " + reqUri);
    request = HttpRequest.newBuilder().uri(reqUri)
          .headers("X-Vault-Token", _parms.vtok, "Accept", "application/json",
                "Content-Type", "application/json")
          .build();
    resp = _httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Check return code.
    int rc = resp.statusCode();
    String headers = resp.headers().toString();
    String respStr = resp.toString();
    var location = resp.headers().firstValue("location");
    warn(String.format("Received HTTP status code: %d location: %s Headers: %s Response: %s", rc, location, headers, respStr));
    // If not found then no secret data, so return false
    if (rc == 404) return false;
    // For error status code log an error and return false
    if (rc >= 300)
    {
      warn("Received http status code " + rc + " on GET request to " + reqUri);
      debug(String.format("Received HTTP status code: %d location: %s Headers: %s Response: %s", rc, location, headers, respStr));
      return false;
    }

    // Parse the response body and return the value of the data object.
    // The secrets look like:  "data": {"data": {"foo": "bar"}, "metadata": {..}}
    JsonObject jsonObj =  TapisGsonUtils.getGson().fromJson(resp.body(), JsonObject.class);
    if (jsonObj == null)
    {
      error("Unable to create Json object from response.");
      return false;
    }
    var dataObj = jsonObj.get("data");
    if (dataObj == null)
    {
      error("Did not find data field in json object from response.");
      return false;
    }
    var dataJsonObj = dataObj.getAsJsonObject();

    if (dataJsonObj == null)
    {
      error("Unable to get dataJsonObj from response.");
      return false;
    }
    // Log if found
    debug(String.format("Found secret. KeyType: %s reqUri: %s", SKVaultSecretKeyType.sshkey, reqUri));
    return true;
  }

  /**
   * Read user record from Vault, use attributes to sync with CredentialInfo table.
   * Vault paths are in the form:
   *      secret/tapis/tenant/<tenant_id>/system/<system_id>/user/static+<target_user>
   *    or
   *      secret/tapis/tenant/<tenant_id>/system/<system_id>/user/dynamic+<target_user>
   * Vault paths for Systems secrets always end with <secret_type>/S1
   *   where secret_type is password, sshkey, accesskey or
   * @param tenant tenant to process
   * @param system system to process
   * @param userField field with user data, static/dynamic plus username
   * @throws Exception on error
   */
  private void initCredInfoRecord(String tenant, String system, String userField) throws Exception
  {
    trace(String.format("initCredInfoRecord. Tenant: %s system: %s user: %s.", tenant, system, userField));
    boolean isStatic;
    String userName;

    // If user field begins with static+ or dynamic+ then it is a non-legacy record we process it
    if (Strings.CI.startsWith(userField,"static+"))
    {
      isStatic = true;
      userName = SPLIT_PLUS_PATTERN.split(userField, 2)[1];
      trace(String.format("Found static record. Tenant: %s System: %s User field: %s Username: %s",
            tenant, system, userField, userName));
    }
    else if (Strings.CI.startsWith(userField,"dynamic+"))
    {
      isStatic = false;
      userName = SPLIT_PLUS_PATTERN.split(userField, 2)[1];
      trace(String.format("Found dynamic record. Tenant: %s System: %s User field: %s Username: %s",
            tenant, system, userField, userName));
    }
    else
    {
      // It is a legacy record. Remove it.
      if (isApply && _parms.rmLegacy)
      {
        info(String.format("Removing legacy record. Tenant: %s System: %s User field: %s", tenant, system, userField));
        // Remove SK records
        // Determine targetUserPath for the path to the secret. For legacy record it is just the username.
        String targetUserPath = userField;
        // Surround all SK related code in a try block. Catch any SK errors and throw a TapisSecurityException
        try
        {
          var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
          // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
          sMetaParms.setTenant(tenant).setUser(userField);
          sMetaParms.setSysId(system).setSysUser(targetUserPath);

          // Construct basic SK secret parameters and attempt to destroy each type of secret.
          // If destroy attempt throws an exception then log a message and continue.
          sMetaParms.setKeyType(KeyType.password);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm password: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.sshkey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm sshkey: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.accesskey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm accesskey: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.token);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm token: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.tmskey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm tmskey: " + e.getMessage()); }
        } catch (TapisClientException tce) {error("Error rm tmskey: " + tce.getMessage()); throw new TapisSecurityException(tce);}
      }
      return;
    }
    debug(String.format("Found record. Tenant: %s System: %s TargetUsername: %s isStatic: %b",
          tenant, system, userName, isStatic));
    // Determine metadata for this user as a java record
    SecretMetaInfo secretMetadata = getSecretMetadata(tenant, system, userField, userName, isStatic);
    trace("Found secret metadata: " + secretMetadata);
    // We have a record, sync with SK
    if (isApply)
    {
      // Fetch the system, we will use the seqId and owner
      TSystem sys = dao.getSystem(tenant, system); // For seqId, owner
      if (sys == null)
      {
        // System is missing or deleted. Remove record.
        info(String.format("System is missing or deleted. Remove record. Tenant: %s System: %s User field: %s", tenant, system, userField));
        // Remove SK records
        // Determine targetUserPath for the path to the secret. For legacy record it is just the username.
        String targetUserPath = userField;
        // Surround all SK related code in a try block. Catch any SK errors and throw a TapisSecurityException
        try
        {
          var sMetaParms = new SKSecretMetaParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);
          // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
          sMetaParms.setTenant(tenant).setUser(userField);
          sMetaParms.setSysId(system).setSysUser(targetUserPath);

          // Construct basic SK secret parameters and attempt to destroy each type of secret.
          // If destroy attempt throws an exception then log a message and continue.
          sMetaParms.setKeyType(KeyType.password);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm password: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.sshkey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm sshkey: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.accesskey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm accesskey: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.token);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm token: " + e.getMessage()); }
          sMetaParms.setKeyType(KeyType.tmskey);
          try {getSKClient().destroySecretMeta(sMetaParms);} catch (Exception e) { error("Error rm tmskey: " + e.getMessage()); }
        } catch (TapisClientException tce) {error("Error rm tmskey: " + tce.getMessage()); throw new TapisSecurityException(tce);}
        return;
      }

      String fmt = "Write CredentialInfo. tenant: %s sysId: %s tapisUser: %s isStatic: %b";
      trace(String.format(fmt, secretMetadata.tenantId, secretMetadata.systemId, secretMetadata.targetUser, secretMetadata.isStatic));
      CredentialInfo ci = credUtils.initCredInfoRecordFromVaultMetadata(rUserSvc, tenant, sys, isStatic, secretMetadata);
      fmt = "Wrote CredentialInfo. tenant: %s sysId: %s tapisUser: %s isStatic: %b, loginUserMapping: %s " +
            "hostLoginUser: %s hasCredentials: %b hasPassword: %b hasPkiKeys: %b hasAccessKey: %b hasToken %b hasTmsKeys: %b";
      trace(String.format(fmt, ci.getTenant(), ci.getSystemId(), ci.getTapisUser(), ci.isStatic(), ci.getLoginUserMapping(), ci.getHostLoginUser(),
                               ci.hasCredentials(), ci.hasPassword(), ci.hasPkiKeys(), ci.hasAccessKey(), ci.hasToken(),
                               ci.hasTmsKeys()));
    }
  }

  // Print out error message and exit
  private void errorExit(String s) { System.out.printf("ERROR: %s%n", s); System.exit(1); }
  // Print out error message
  private void error(String s) { System.out.printf("ERROR: %s%n", s); }
  // Print warning message
  private void warn(String s) { if (_parms.verbose) System.out.println("WARN: " + s); }
  // Print info message
  private void info(String s) { if (!_parms.quiet) System.out.println("INFO: " + s); }
  // Print debug message
  private void debug(String s) { if (_parms.verbose) System.out.println("DEBUG: " + s); }
  // Print trace message
  private void trace(String s) { if (_parms.verbose) System.out.println("TRACE: " + s); }
}
