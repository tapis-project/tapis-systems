package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import com.google.gson.JsonElement;

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
  public static final String USER_TENANT_FIELD = "userTenant";
  public static final String USER_NAME_FIELD = "userName";
  public static final String OPERATION_FIELD = "operation";
  public static final String UPD_JSON_FIELD = "updJson";
  public static final String CREATED_FIELD = "created";

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  
  private final String userTenant;
  private final String userName;
  private final SystemOperation operation;
  private final JsonElement updJson;
  private final Instant created; // UTC time for when record was created

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public SystemHistoryItem(String userTenant1, String userName1, SystemOperation operation1, 
                          JsonElement jsonElement, Instant created1)
  {
    userTenant = userTenant1;
    userName = userName1;
    operation = operation1;
    updJson = jsonElement;
    created = created1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public String getUserTenant() { return userTenant; }
  public String getUserName() { return userName; }
  public SystemOperation getOperation() { return operation; }
  public JsonElement getUpdJson() { return updJson; }
  public Instant getCreated() { return created; }
}
