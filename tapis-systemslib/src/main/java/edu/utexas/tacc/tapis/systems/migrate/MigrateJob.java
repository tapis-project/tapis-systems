package edu.utexas.tacc.tapis.systems.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/*
 * Run java based non-DB migration steps for the Systems service.
 *
 * By default, it is a dry run, no permanent changes are made.
 * Use option -w or --wetrun to apply changes.
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

    // Start the worker.
    MigrateJob migrateJob = new MigrateJob(parms);
    migrateJob.migrate();
  }

  /**
   * Perform the migration
   * @throws TapisException on error
   */
  public void migrate() throws TapisException
  {
    // Only apply changes if asked to do so
    boolean isDryRun = !_parms.isWetRun;

  }

  /* ********************************************************************** */
  /*                            Private Methods                             */
  /* ********************************************************************** */

}
