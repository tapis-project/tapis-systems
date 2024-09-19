package edu.utexas.tacc.tapis.systems.model;

import org.statefulj.persistence.annotations.State;

// Class representing State based on SyncStatus. Used for FiniteStateMachine(FSM)
public class CredInfoSyncState
{
  public static final String STATE_FIELD_NAME = "SyncStatus";
  @State
  String syncStatus; // State for FSM. StatefulJ memory persister requires this to be a String

  public String getSyncStatus() { return syncStatus; }
}

