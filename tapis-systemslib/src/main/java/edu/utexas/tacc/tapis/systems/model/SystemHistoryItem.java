package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

/*
 * System History
 *
 */
public final class SystemHistoryItem
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Attribute names, also used as field names in Json
  public static final String USER_NAME_FIELD = "userName";
  public static final String OPERATION_FIELD = "operation";
  public static final String CREATED_FIELD = "created";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private final String userName;
  private final SystemOperation operation;
  private final Instant created; // UTC time for when record was created

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public SystemHistoryItem(String userName1, SystemOperation operation1, Instant created1)
  {
    userName = userName1;
    operation = operation1;
    created = created1;
  }


  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public Instant getCreated() { return created; }
  public String getName() { return userName; }
  public SystemOperation getDescription() { return operation; }
}
