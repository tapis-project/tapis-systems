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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
  private final ResourceRequestUser rUser;

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
   * Main method. Performs the following:
   *   - Update the systems_cred_info table to keep it in sync with SK
   */
  public void run()
  {
    log.info(LibUtils.getMsg("SYSLIB_MAINT_RUN_BEGIN"));
    try
    {
      // Run maintenance tasks for CredInfo table
      credInfoRunMaintenance();
    }
    catch (Exception e)
    {
      log.error(LibUtils.getMsg("SYSLIB_MAINT_RUN_ERR", e.getMessage()), e);
    }
    log.info(LibUtils.getMsg("SYSLIB_MAINT_RUN_END"));
  }

  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /*
   * Check the systems_cred_info table and update as needed
   *  - Mark all FAILED records as PENDING
   *  - For each PENDING record read info from SK and update the cred info table.
   */
  private void credInfoRunMaintenance() throws TapisException
  {
    // Mark all FAILED records as PENDING
    credInfoMarkFailedAsPending();
    // For each PENDING record read info from SK and update the cred info table.
    credInfoSyncPendingRecords();
  }

  /**
   * Multithreaded update of all CredInfo FAILED records to PENDING
   */
  private void credInfoMarkFailedAsPending()
  {
    // Find all FAILED records
    List<CredentialInfo> failedRecords = dao.credInfoGetRecordsInStatus(SyncStatus.FAILED);
    String msg = LibUtils.getMsg("SYSLIB_MAINT_CREDINFO_FAIL_COUNT", failedRecords.size());
    log.info(msg);
    // For each record update the status
    for (CredentialInfo credInfo: failedRecords)
    {
      // Get the shared record in the locked state (WE MUST UNLOCK)
      CredentialInfo lockedCredInfo = credUtils.getLockedInMemoryCredInfo(credInfo);
      // null means it got removed from DB before we got to it, so we must skip
      if (lockedCredInfo == null) continue;
      try
      {
        // Make sure still in FAILED, if not then skip
        if (!SyncStatus.FAILED.equals(lockedCredInfo.getSyncStatus())) { continue; }
        // Update status to PENDING
        credUtils.updateCredentialInfoStatus(rUser, lockedCredInfo, SyncStatus.PENDING);
      }
      finally
      {
        lockedCredInfo.mutex.unlock();
      }
    }
  }

  /**
   * Multithreaded sync of all CredInfo PENDING records with SK
   */
  private void credInfoSyncPendingRecords()
  {
    // Find all PENDING records
    List<CredentialInfo> pendingRecords = dao.credInfoGetRecordsInStatus(SyncStatus.PENDING);
    String msg = LibUtils.getMsg("SYSLIB_MAINT_CREDINFO_PENDING_COUNT", pendingRecords.size());
    log.info(msg);
    // For each record sync it with SK
    for (CredentialInfo credInfo: pendingRecords)
    {
      // Get the shared record in the locked state (WE MUST UNLOCK)
      CredentialInfo lockedCredInfo = credUtils.getLockedInMemoryCredInfo(credInfo);
      // null means it got removed from DB before we got to it, so we must skip
      if (lockedCredInfo == null) continue;
      try
      {
        // Make sure still in PENDING, if not then skip
        if (!SyncStatus.PENDING.equals(lockedCredInfo.getSyncStatus())) { continue; }
        // Update status to IN_PROGRESS
        credUtils.updateCredentialInfoStatus(rUser, lockedCredInfo, SyncStatus.IN_PROGRESS);
        try
        {
          // Sync records
          credUtils.readCredInfoFromSK(rUser, lockedCredInfo);
          credUtils.updateCredentialInfo(rUser, lockedCredInfo);
          // Update status to COMPLETED
          credUtils.updateCredentialInfoStatus(rUser, lockedCredInfo, SyncStatus.COMPLETED);
        }
        catch (Exception e)
        {
          // Log error, update the credInfo record to FAILED.
          String failMsg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_SYNC_FAIL", rUser, credInfo.getTenant(),
                credInfo.getSystemId(), credInfo.getTapisUser(), credInfo.getLoginUserMapping(), credInfo.isStatic(),
                credInfo.getSyncFailCount(), e.getMessage());
          log.error(failMsg);
          // Update failure related attributes of the credInfo
          lockedCredInfo.setSyncFailed(TapisUtils.getUTCTimeNow().toInstant(ZoneOffset.UTC));
          lockedCredInfo.setSyncFailCount(lockedCredInfo.getSyncFailCount()+1);
          lockedCredInfo.setSyncFailMessage(e.getMessage());
          lockedCredInfo.setSyncStatus(SyncStatus.FAILED);
          credUtils.updateCredentialInfo(rUser, lockedCredInfo);
        }
      }
      finally
      {
        lockedCredInfo.mutex.unlock();
      }
    }
  }
}