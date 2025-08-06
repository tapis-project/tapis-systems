package edu.utexas.tacc.tapis.systems.model;

import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;

// Class representing an FSM Action
public final class CredInfoSyncAction<SyncState> implements Action<SyncState>
{
  String newSyncState;
  public CredInfoSyncAction(String newState) {newSyncState = newState; }
  public void execute(SyncState syncState, String event, Object ... args) throws RetryException
  {
//    newSyncState = syncState.getState();
    System.out.println("Executing action: " + newSyncState);
  }
}
