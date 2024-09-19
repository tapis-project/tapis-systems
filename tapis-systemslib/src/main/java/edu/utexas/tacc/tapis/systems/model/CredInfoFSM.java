package edu.utexas.tacc.tapis.systems.model;

import java.util.List;
import java.util.Set;

import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.impl.StateImpl;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;

/*
 * Class representing elements of Finite State Machine (FSM) for synchronization state of
 *   CredentialInfo records
 * Used for validating state transitions.
 * Main usefulness is in catching difficult to find bugs introduced by future code changes.
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
  public static final State<CredInfoSyncState> PendingState = new StateImpl<>(SyncStatus.PENDING.name());
  public static final State<CredInfoSyncState> InProgressState = new StateImpl<>(SyncStatus.IN_PROGRESS.name());
  public static final State<CredInfoSyncState> FailedState = new StateImpl<>(SyncStatus.FAILED.name());
  public static final State<CredInfoSyncState> CompletedState = new StateImpl<>(SyncStatus.COMPLETED.name());

  // Static initializer for transitions
  static { initializeTransitions(); }

  // Events
  public static final String PendingToInProgress = "PendingToInProgress";
  public static final String InProgressToCompleted = "InProgressToCompleted";
  public static final String InProgressToFailed = "InProgressToFailed";
  public static final String CompletedToPending = "CompletedToPending";
  public static final String FailedToPending = "FailedToPending";
  // TODO/TBD Do we really need a full FSM?
  //  If all we are checking is that a transition is allowed could we just have a Set of allowed transitions
  //          and check that proposed transition against that set? Do we really need an FSM?
  public static final Set<String> allowedEvents
          = Set.of(PendingToInProgress, InProgressToCompleted, InProgressToFailed, CompletedToPending, FailedToPending);

  // Actions
  public static final Action<CredInfoSyncState> pendingToInProgressAction = new CredInfoSyncAction<>("IN_PROGRESS");
  public static final Action<CredInfoSyncState> inProgressToCompletedAction = new CredInfoSyncAction<>("COMPLETED");
  public static final Action<CredInfoSyncState> inProgressToFailedAction = new CredInfoSyncAction<>("FAILED");
  public static final Action<CredInfoSyncState> completedToPendingAction = new CredInfoSyncAction<>("PENDING");

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // List of all states
  private static final List<State<CredInfoSyncState>> states = createStateList();

//  private final SyncState syncState; // Indicates current status of synchronization between SK and Systems service.
//
//  private final String tenant; // Name of tenant associated with the credential
//  private final String systemId; // Name of the system associated with the credential
//  private final String tapisUser; // Tapis user associated with the credential
//  private final String loginUser; // For a system with a dynamic effectiveUserId, this is the host login user.
//  private final boolean isDynamic; // Indicates if record is for the static or dynamic effectiveUserId case.
//  private final boolean hasCredentials; // Indicates if system has credentials registered for the current defaultAuthnMethod
//  private final boolean hasPassword; // Indicates if credentials for PASSWORD have been registered.
//  private final boolean hasPkiKeys; // Indicates if credentials for PKI_KEYS have been registered.
//  private final boolean hasAccessKey; // Indicates if credentials for ACCESS_KEY have been registered.
//  private final boolean hasToken; // Indicates if credentials for TOKEN have been registered.
//  private final long syncFailCount; // Number of sync attempts that have failed
//  private final String syncFailMessage; // Message indicating why last sync attempt failed
//  private final Instant syncFailed; // UTC time for time of last sync failure.
//  private final Instant created; // UTC time for when record was created
//  private final Instant updated; // UTC time for when record was last updated


  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                       Private methods                                  */
  /* ********************************************************************** */

  /**
   * Create list of all possible states
   * @return unmodifiable list of all possible states
   */
  private static List<State<CredInfoSyncState>> createStateList()
  {
    return List.of(PendingState, InProgressState, FailedState, CompletedState);
  }

  private static void initializeTransitions()
  {
    // Deterministic transitions
    //    Pending->InProgress
    //    Failed->Pending
    PendingState.addTransition(PendingToInProgress, InProgressState);
    FailedState.addTransition(FailedToPending, PendingState);

    // Non-deterministic transitions
    //    Completed->Pending
    //    Completed->InProgress
    //    InProgress->Failed
    //    InProgress-Completed
    CompletedState.addTransition(CompletedToPending, PendingState);
    CompletedState.addTransition(CompletedToPending, InProgressState);
    InProgressState.addTransition(InProgressToCompleted, CompletedState);
    InProgressState.addTransition(InProgressToFailed, FailedState);
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public static List<State<CredInfoSyncState>> getStates() { return states; }
}
