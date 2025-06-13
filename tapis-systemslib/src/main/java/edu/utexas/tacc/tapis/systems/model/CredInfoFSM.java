package edu.utexas.tacc.tapis.systems.model;

import java.util.List;
import java.util.Set;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.service.CredUtils;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.impl.StateImpl;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;
import static edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus.*;

/*
 * Class representing elements of Finite State Machine (FSM) for synchronization state of CredentialInfo records.
 * Note that even if not used as an event driven FSM it is still useful for validating state transitions.
 * Main usefulness is in catching difficult to find bugs introduced by future code changes.
 * Also, this is a good place for documentation.
 *
 * Transitions
 * When records are first created they start off in the PENDING state
 *
 * ================================================================================================
 * Transitions that can happen during single-threaded startup. See CredUtils.credInfoInit()
 * ================================================================================================
 * IN_PROGRESS    -> FAILED
 * DELETED        -> <non-existent>
 * <non-existent> -> PENDING
 * FAILED         -> PENDING
 * ------------------------------------------------------------------------------------------------
 * ================================================================================================
 * TODO Transitions that can happen during run of maintenance thread. See MaintenanceTask.credInfoRunMaintenance
 * ================================================================================================
 * FAILED         -> PENDING
 * PENDING        -> IN_PROGRESS
 * IN_PROGRESS    -> COMPLETED
 * IN_PROGRESS    -> FAILED
 *
 * ------------------------------------------------------------------------------------------------
 * Normal flow until deleted
 *    Pending->InProgress    - start of an update attempt (at start-up, for example)
 *    InProgress->Completed  - successful update
 *    Completed->Pending     - ready for an update attempt TODO/TBD needed?
 *    Completed->InProgress  - start of an updated attempt
 * Normal flow when deleted
 *    Completed->Deleted
 * Abnormal flows
 *    InProgress->Failed - Error during update
 *    Pending->Deleted   - deleted before update started
 *    Pending->Failed    - error during move from Pending to InProgress or Deleted. Possible? // TODO/TBD
 *    Failed->Pending    - ready for an update attempt
 *    Failed->Deleted    - deleted before becoming ready for an update attempt
 *    Deleted->Pending   - ready for an update attempt prior to clean up of deleted records
 *    Deleted->Deleted   - cred delete prior to clean up of deleted records
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

  public static final State<CredInfoSyncState> PendingState = new StateImpl<>(PENDING.name());
  public static final State<CredInfoSyncState> InProgressState = new StateImpl<>(IN_PROGRESS.name());
  public static final State<CredInfoSyncState> FailedState = new StateImpl<>(SyncStatus.FAILED.name());
  public static final State<CredInfoSyncState> DeletedState = new StateImpl<>(SyncStatus.DELETED.name());
  public static final State<CredInfoSyncState> CompletedState = new StateImpl<>(SyncStatus.COMPLETED.name());

  // Static initializer for transitions
  static { initializeTransitions(); }

  // Events
  public static final String PendingToInProgress = String.format("%s-%s", PENDING, IN_PROGRESS);
  public static final String InProgressToCompleted = String.format("%s-%s", IN_PROGRESS, COMPLETED);
  public static final String InProgressToFailed = String.format("%s-%s", IN_PROGRESS, FAILED);
  public static final String CompletedToPending = String.format("%s-%s", COMPLETED, PENDING); // TODO/TBD needed?
  public static final String CompletedToInProgress = String.format("%s-%s", COMPLETED, IN_PROGRESS);
  public static final String CompletedToDeleted = String.format("%s-%s", COMPLETED, DELETED);
  public static final String FailedToPending = String.format("%s-%s", FAILED, PENDING);
  public static final String DeletedToPending = String.format("%s-%s", DELETED, PENDING);
  public static final String PendingToDeleted = String.format("%s-%s", PENDING, DELETED);
  public static final String FailedToDeleted = String.format("%s-%s", FAILED, DELETED);
  public static final String DeletedToDeleted = String.format("%s-%s", DELETED, DELETED);
  // public static final String PendingToFailed = String.format("%s-%s", PENDING, FAILED); // TODO/TBD needed?
  public static final Set<String> allowedEvents =
        Set.of(PendingToInProgress, InProgressToCompleted, InProgressToFailed, CompletedToPending, CompletedToInProgress,
               CompletedToDeleted, FailedToPending, DeletedToPending, PendingToDeleted, FailedToDeleted, DeletedToDeleted);

  // Actions
  public static final Action<CredInfoSyncState> pendingToInProgressAction = new CredInfoSyncAction<>(IN_PROGRESS.name());
  public static final Action<CredInfoSyncState> inProgressToCompletedAction = new CredInfoSyncAction<>(SyncStatus.COMPLETED.name());
  public static final Action<CredInfoSyncState> inProgressToFailedAction = new CredInfoSyncAction<>(SyncStatus.FAILED.name());
  public static final Action<CredInfoSyncState> deletedToPendingAction = new CredInfoSyncAction<>(PENDING.name());
  public static final Action<CredInfoSyncState> completedToPendingAction = new CredInfoSyncAction<>(PENDING.name());

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // List of all states
  private static final List<State<CredInfoSyncState>> states = createStateList();

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

  /**
   * Create list of all possible states
   * @return unmodifiable list of all possible states
   */
  private static List<State<CredInfoSyncState>> createStateList()
  {
    return List.of(PendingState, InProgressState, FailedState, DeletedState, CompletedState);
  }

  private static void initializeTransitions()
  {
    // Transitions
    // When records are first created they start off in the PENDING state
    // Normal flow until deleted
    //    Pending->InProgress    - start of an update attempt
    //    InProgress->Completed  - successful update
    //    Completed->Pending     - ready for an update attempt
    PendingState.addTransition(PendingToInProgress, InProgressState);
    InProgressState.addTransition(InProgressToCompleted, CompletedState);
    CompletedState.addTransition(CompletedToPending, PendingState);

    // Normal flow when deleted
    CompletedState.addTransition(CompletedToDeleted, DeletedState);

    // Abnormal flows
    //    InProgress->Failed - Error during update
    //    Pending->Deleted   - deleted before update started
    //    Pending->Failed    - error during move from Pending to InProgress or Deleted. Possible? // TODO/TBD
    //    Failed->Pending    - ready for an update attempt
    //    Failed->Deleted    - deleted before becoming ready for an update attempt
    //    Deleted->Pending   - ready for an update attempt prior to clean up of deleted records
    InProgressState.addTransition(InProgressToFailed, FailedState);
    PendingState.addTransition(PendingToInProgress, DeletedState);
    PendingState.addTransition(PendingToInProgress, FailedState); // TODO/TBD
    FailedState.addTransition(FailedToPending, PendingState);
    FailedState.addTransition(FailedToDeleted, DeletedState);
    DeletedState.addTransition(DeletedToPending, PendingState);
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public static List<State<CredInfoSyncState>> getStates() { return states; }
}
