package edu.utexas.tacc.tapis.systems.service;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;

/*
 * Support maintenance tasks for the Systems service
 * Contains a static public method that is run at fixed intervals using a ScheduledExecutorService.
 */
public final class MaintenanceTask
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(MaintenanceTask.class);

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private final SystemsDao dao;
  private final CredUtils credUtils;

  // ResourceRequestUser associated with maintenance task. Should only be used for logging.
  private final ResourceRequestUser rUser;

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  public MaintenanceTask(ResourceRequestUser rUser1, SystemsDao dao1, CredUtils credUtils1)
  {
    rUser = rUser1;
    dao = dao1;
    credUtils = credUtils1;
  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /*
   * Main method. Performs the following:
   *   - Update the systems_cred_info table to keep it in sync with SK
   */
  public static void runMaintenance(MaintenanceTask maintenanceTask)
  {
    log.info(LibUtils.getMsg("SYSLIB_MAINT_RUN_BEGIN"));
    try
    {
      // Run maintenance tasks for CredInfo table
      maintenanceTask.credInfoRunMaintenance();
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
  private void credInfoRunMaintenance()
  {
    // Log start
    int totalCount = dao.getCredInfoTotalCount();
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_MAINT_BEGIN", totalCount));
    // Mark all FAILED records as PENDING
    credUtils.credInfoMarkFailedAsPending(rUser);
    // For each PENDING record read info from SK and update the cred info table.
    credUtils.syncPendingCredInfoRecords(rUser);
    // Log end
    totalCount = dao.getCredInfoTotalCount();
    log.info(LibUtils.getMsg("SYSLIB_CREDINFO_MAINT_END", totalCount));
  }
}