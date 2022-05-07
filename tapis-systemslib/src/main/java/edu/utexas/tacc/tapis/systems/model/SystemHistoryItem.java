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

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  private final String jwtTenant;
  private final String jwtUser;
  private final String oboTenant;
  private final String oboUser;
  private final SystemOperation operation;
  private final JsonElement description;
  private final Instant created; // UTC time for when record was created

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public SystemHistoryItem(String jwtTenant1, String jwtUser1, String oboTenant1, String oboUser1,
                           SystemOperation operation1, JsonElement jsonElement, Instant created1)
  {
    jwtTenant = jwtTenant1;
    jwtUser = jwtUser1;
    oboTenant = oboTenant1;
    oboUser = oboUser1;
    operation = operation1;
    description = jsonElement;
    created = created1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public String getJwtTenant() { return jwtTenant; }
  public String getJwtUser() { return jwtUser; }
  public String getOboTenant() { return oboTenant; }
  public String getOboUser() { return oboUser; }
  public SystemOperation getOperation() { return operation; }
  public JsonElement getDescription() { return description; }
  public Instant getCreated() { return created; }
}
