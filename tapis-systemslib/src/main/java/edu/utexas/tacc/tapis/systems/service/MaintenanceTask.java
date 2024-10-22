package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.KeyType;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import static edu.utexas.tacc.tapis.systems.model.Credential.*;

/*
 * Support maintenance tasks for the Systems service
 * Contains a single public method that is run at fixed intervals using a ScheduledExecutorService.
 */
public final class MaintenanceTask implements Runnable
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(MaintenanceTask.class);

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private SysUtils sysUtils;
  @Inject
  private AuthUtils authUtils;
  @Inject
  private CredUtils credUtils;

  // ResourceRequestUser associated with maintenance task. Should only be used for logging.
  private ResourceRequestUser rUser;

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  public MaintenanceTask(ResourceRequestUser rUser1)
  {
    rUser = rUser1;
  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /*
   * Main method
   */
  public void run()
  {
    log.info(LibUtils.getMsg("SYSLIB_MAINT_TASK_RUN"));
    try
    {
      // Run maintenance tasks for CredInfo table
      credInfoRunMaintenance();
    }
    catch (Exception e)
    {
      log.error(LibUtils.getMsg("SYSLIB_MAINT_TASK_ERR", e.getMessage()), e);
    }
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /*
   * Check the systems_cred_info table and update as needed
   *  - For each PENDING record read info from SK and update the cred info table.
   *  TODO/TBD only do this at startup? - Mark all FAILED records as PENDING
   *  - For each PENDING record read info from SK and update the cred info table.
   */
  private void credInfoRunMaintenance() throws TapisException
  {
    // TODO Remove Create records in credInfo table as needed for undeleted systems that have a static effectiveUserId
    // TODO Remove NO, this should only be done at startup when process is single threaded.
    // TODO Remove dao.credInfoInitStaticSystems();

    // For each PENDING record read info from SK and update the cred info table.
// TODO    credInfoSyncPendingRecords();

    // Mark all FAILED records as PENDING
// TODO    dao.credInfoMarkFailedAsPending();

    // For each PENDING record read info from SK and update the cred info table.
// TODO    credInfoSyncPendingRecords();
  }

  /**
   * TODO Move this method to CredUtils?
   * Sync all CredInfo PENDING records with SK
   */
  private void credInfoSyncPendingRecords(ResourceRequestUser rUser, SystemsDao dao)
          throws TapisException
  {
    // Find all PENDING records
    List<CredentialInfo> pendingRecords = dao.credInfoGetPendingRecords();
    // For each record sync it with SK
    for (CredentialInfo credInfo: pendingRecords)
    {
      // TODO/TBD start a db connection and use selectForUpdate to synchronize on the record?
      //          pass in db connection instead of dao?
      // TODO/TBD still need to check that record is PENDING?
      credInfoSyncWithSK(rUser, dao, credInfo);
    }
  }

  /**
   * For a given record in the SYSTEMS_CRED_INFO table read from SK and update the record.
   */
  private void credInfoSyncWithSK(ResourceRequestUser rUser, SystemsDao dao, CredentialInfo credInfo)
          throws TapisException
  {
    // TODO/TBD start a db connection and use selectForUpdate to synchronize on the record?
    //          do that here or in calling method and pass in db connection instead of dao?
    // Mark record as IN_PROGRESS
    LocalDateTime updated = TapisUtils.getUTCTimeNow();
    dao.credInfoUpdateStatus(credInfo, SyncStatus.IN_PROGRESS, updated);
    //TODO Call SK to get credential info.
    // On any error mark as failed and return
    CredentialInfo skCredInfo;
    try
    {
      skCredInfo = getSkCredInfo(rUser, dao, credInfo);
    }
    catch (Exception e)
    {
      // Mark record as failed
      // TODO message
      String failMsg = LibUtils.getMsg("SYSLIB_???????");
      dao.credInfoMarkInProgressAsFailed(failMsg);;
      return;
    }

    // Update CredInfo table record - clear failure info, set status to COMPLETE
    dao.credInfoMarkAsComplete(skCredInfo);
  }

  /**
   * For syncing data.
   * Given CredentialInfo record call SK to get latest data.
   * No exceptions are caught.
   *
   * @param rUser ResourceRequest user, for logging purposes
   * @param credInfo CredentialInfo object with current data from Systems server datastore
   * @throws TapisException - on DAO error
   * @throws TapisClientException - on SK error
   * @return CredentialInfo object with latest data from Security Kernel (SK)
   */ // TODO pass in credUtils instead of dao
  CredentialInfo getSkCredInfo(ResourceRequestUser rUser, SystemsDao dao, CredentialInfo credInfo)
          throws TapisException, TapisClientException
  {
    CredentialInfo skCredInfo;
    boolean hasCredentials, hasPassword, hasPkiKeys, hasAccessKey, hasToken;
    String tenant = credInfo.getTenant();
    String targetUser = credInfo.getTapisUser();
    String systemId = credInfo.getSystemId();
    boolean isStaticEffectiveUser = !credInfo.isStatic();
    TSystem.AuthnMethod defaultAuthnMethod= dao.getSystemDefaultAuthnMethod(tenant, systemId);
    // Construct basic SK secret parameters
    // Establish secret type ("system") and secret name ("S1")
    var sParms = new SKSecretReadParms(SecretType.System).setSecretName(TOP_LEVEL_SECRET_NAME);

    // Fill in systemId and targetUserPath for the path to the secret.
    String targetUserPath = CredUtils.getTargetUserSecretPath(targetUser, isStaticEffectiveUser);

    // Set tenant, system and user associated with the secret.
    // These values are used to build the vault path to the secret.
    sParms.setTenant(tenant).setSysId(systemId).setSysUser(targetUserPath);

    // NOTE: For secrets of type "system" setUser value not used in the path, but SK requires that it be set.
    sParms.setUser(targetUser);

    // PASSWORD
    sParms.setKeyType(KeyType.password);
    SkSecret skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasPassword = false;
    else
    {
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasPassword = false;
      else hasPassword = !StringUtils.isBlank(dataMap.get(SK_KEY_PASSWORD));
    }
    // PKI_KEYS
    sParms.setKeyType(KeyType.sshkey);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasPkiKeys = false;
    else
    {
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasPkiKeys = false;
      else hasPkiKeys = !StringUtils.isBlank(dataMap.get(SK_KEY_PRIVATE_KEY));
    }
    // ACCESS_KEY
    sParms.setKeyType(KeyType.accesskey);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasAccessKey = false;
    else
    {
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasAccessKey = false;
      else hasAccessKey = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_KEY));
    }
    // TOKEN
    sParms.setKeyType(KeyType.token);
    skSecret = sysUtils.getSKClient(rUser).readSecret(sParms);
    if (skSecret == null) hasToken = false;
    else
    {
      var dataMap = skSecret.getSecretMap();
      if (dataMap == null) hasToken = false;
      else hasToken = !StringUtils.isBlank(dataMap.get(SK_KEY_ACCESS_TOKEN));
    }

    // Determine if credentials are registered for defaultAuthnMethod of the system
    hasCredentials = (TSystem.AuthnMethod.PASSWORD.equals(defaultAuthnMethod) && hasPassword) ||
            (TSystem.AuthnMethod.PKI_KEYS.equals(defaultAuthnMethod) && hasPkiKeys) ||
            (TSystem.AuthnMethod.ACCESS_KEY.equals(defaultAuthnMethod) && hasAccessKey) ||
            (TSystem.AuthnMethod.TOKEN.equals(defaultAuthnMethod) && hasToken);
    // TODO? Create credentialInfo
//  public CredentialInfo(int systemSeqId1, String tenant1, String systemId1, String tapisUser1, String loginUser1,
//    boolean isStatic1, boolean hasCredentials1, boolean hasPassword1, boolean hasPkiKeys1,
//    boolean hasAccessKey1, boolean hasToken1, SyncStatus syncStatus1, int syncFailCount1,
//    String syncFailMessage1, Instant syncFailed1, java.time.Instant created1, java.time.Instant updated1)
//    return skCredInfo;
    return null; // TODO
  }
}