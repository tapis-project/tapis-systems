package edu.utexas.tacc.tapis.systems.model;

import java.util.Set;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.service.CredUtils;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import static edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus.*;

/*
 * Class representing elements of Finite State Machine (FSM) for synchronization state of CredentialInfo records.
 * Note that even if not used as an event driven FSM it is still useful for validating state transitions.
 * Main usefulness is in catching difficult to find bugs introduced by future code changes.
 * Also, this is a good place for documentation.
 *
 * There is a fair bit of commented code that we might consider removing in the future.
 * But if we do ever have need of an event driven FSM it would be a starting point.
 *
 * Transitions
 * When records are first created they start off in the PENDING state
 *
 * ********************************************************************
 * States: <non-existent> PENDING IN_PROGRESS COMPLETED FAILED
 * ********************************************************************
 *
 * ================================================================================================
 * Transitions that can happen during single-threaded startup. See CredUtils.credInfoInit()
 * ================================================================================================
 * IN_PROGRESS    -> FAILED
 * FAILED         -> PENDING
 * <non-existent> -> PENDING
 * ------------------------------------------------------------------------------------------------
 * ================================================================================================
 * Transitions that can happen during run of maintenance thread. See MaintenanceTask.credInfoRunMaintenance
 * ================================================================================================
 * FAILED         -> PENDING
 * PENDING        -> IN_PROGRESS
 * IN_PROGRESS    -> COMPLETED
 * IN_PROGRESS    -> FAILED
 *
 * ================================================================================================
 * Transitions that can happen during create/update
 * ================================================================================================
 * <non-existent> -> PENDING
 * COMPLETED      -> PENDING
 * PENDING        -> IN_PROGRESS
 * IN_PROGRESS    -> COMPLETED
 * FAILED         -> PENDING
 * IN_PROGRESS    -> FAILED
 *
 * ================================================================================================
 * Transitions that can happen during delete of credential
 * ================================================================================================
 * COMPLETED      -> PENDING
 * FAILED         -> PENDING
 * PENDING        -> IN_PROGRESS
 * IN_PROGRESS    -> <non-existent>
 * IN_PROGRESS    -> FAILED
 * <non-existent> -> PENDING
 *
 * ------------------------------------------------------------------------------------------------
 * Normal flow until deleted
 *    Pending->InProgress    - start of an update attempt (at start-up, for example)
 *    InProgress->Completed  - successful update
 *    Completed->Pending     - ready for an update attempt
 *    Completed->InProgress  - start of an updated attempt
 * Normal flow when deleted
 *    Completed->Deleted
 * Abnormal flows
 *    InProgress->Failed - Error during update
 *    Failed->Pending    - ready for an update attempt
 *
 * Based on StatefulJ FSM library.
 * This class is non-instantiable.
 */
public final class CredInfoFSM
{
  // Private constructor to make it non-instantiable
  private CredInfoFSM() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  public static final String FSM_NAME = CredInfoFSM.class.getSimpleName();
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(CredUtils.class);

  // File containing csv records that can be used to initialize data at application start
  public static final String CREDINFO_INIT_TMP_CSV_FILE = "/tmp/tapis_sys_cred_info_init.csv";

//  public static final State<CredInfoSyncState> PendingState = new StateImpl<>(PENDING.name());
//  public static final State<CredInfoSyncState> InProgressState = new StateImpl<>(IN_PROGRESS.name());
//  public static final State<CredInfoSyncState> FailedState = new StateImpl<>(SyncStatus.FAILED.name());
//  public static final State<CredInfoSyncState> CompletedState = new StateImpl<>(SyncStatus.COMPLETED.name());

  // Static initializer for transitions
//  static { initializeTransitions(); }

  // Events
  public static final String PendingToInProgress = String.format("%s-%s", PENDING, IN_PROGRESS);
  public static final String InProgressToCompleted = String.format("%s-%s", IN_PROGRESS, COMPLETED);
  public static final String InProgressToFailed = String.format("%s-%s", IN_PROGRESS, FAILED);
  public static final String CompletedToPending = String.format("%s-%s", COMPLETED, PENDING);
  public static final String CompletedToInProgress = String.format("%s-%s", COMPLETED, IN_PROGRESS);
  public static final String FailedToPending = String.format("%s-%s", FAILED, PENDING);
  public static final Set<String> allowedEvents =
        Set.of(PendingToInProgress, InProgressToCompleted, InProgressToFailed, CompletedToPending,
               CompletedToInProgress, FailedToPending);

  // Actions, e.g.
//  public static final Action<CredInfoSyncState> pendingToInProgressAction = new CredInfoSyncAction<>(IN_PROGRESS.name());
//  public static final Action<CredInfoSyncState> inProgressToCompletedAction = new CredInfoSyncAction<>(SyncStatus.COMPLETED.name());
//  public static final Action<CredInfoSyncState> inProgressToFailedAction = new CredInfoSyncAction<>(SyncStatus.FAILED.name());
//  public static final Action<CredInfoSyncState> completedToPendingAction = new CredInfoSyncAction<>(PENDING.name());

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // List of all states
//  private static final List<State<CredInfoSyncState>> states = createStateList();

  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */

  /*
   * Determine if transition is allowed
   */
  public static void checkForAllowedTransition(ResourceRequestUser rUser, SyncStatus beginSate, SyncStatus endState)
  {
    String transition = String.format("%s-%s", beginSate, endState);
    if (!CredInfoFSM.allowedEvents.contains(transition))
    {
      String msg = LibUtils.getMsgAuth("SYSLIB_CREDINFO_INIT_FSM_INVALID_TRANSITION", rUser, transition);
      log.error(msg);
      throw new IllegalStateException();
    }
  }

  /* ********************************************************************** */
  /*                       Private methods                                  */
  /* ********************************************************************** */

  /*
   * Create list of all possible states
   * @return unmodifiable list of all possible states
   */
//  private static List<State<CredInfoSyncState>> createStateList()
//  {
//    return List.of(PendingState, InProgressState, CompletedState, FailedState);
//  }

//  private static void initializeTransitions()
//  {
//    // Transitions
//    // When records are first created they start off in the PENDING state
//    // Normal flow until deleted
//    //    Pending->InProgress    - start of an update attempt
//    //    InProgress->Completed  - successful update
//    //    Completed->Pending     - ready for an update attempt
//    PendingState.addTransition(PendingToInProgress, InProgressState);
//    InProgressState.addTransition(InProgressToCompleted, CompletedState);
//    CompletedState.addTransition(CompletedToPending, PendingState);
//

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
//  public static List<State<CredInfoSyncState>> getStates() { return states; }
}
