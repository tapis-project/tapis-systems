package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import java.time.Instant;

/*
 * Tapis SystemBasic - minimal number of attributes from a TSystem.
 *
 * Tenant + name must be unique
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class SystemBasic
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private int seqId;           // Unique database sequence number
  private String tenant;     // Name of the tenant for which the system is defined
  private String id;       // Name of the system
  private TSystem.SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private String owner;      // User who owns the system and has full privileges
  private String host;       // Host name or IP address
  private TSystem.AuthnMethod defaultAuthnMethod; // How access authorization is handled by default
  private boolean canExec; // Indicates if system will be used to execute jobs
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor using only required attributes.
   */
  public SystemBasic(String id1, SystemType systemType1, String host1, AuthnMethod defaultAuthnMethod1, boolean canExec1)
  {
    id = id1;
    systemType = systemType1;
    host = host1;
    defaultAuthnMethod = defaultAuthnMethod1;
    canExec = canExec1;
  }

  /**
   * Construct using a TSystem
   */
  public SystemBasic(TSystem tSystem)
  {
    if (tSystem != null)
    {
      seqId = tSystem.getSeqId();
      tenant = tSystem.getTenant();
      id = tSystem.getId();
      systemType = tSystem.getSystemType();
      host = tSystem.getHost();
      defaultAuthnMethod = tSystem.getDefaultAuthnMethod();
      canExec = tSystem.getCanExec();
      created = tSystem.getCreated();
      updated = tSystem.getUpdated();
    }
  }

  /**
   * Copy constructor. Returns a deep copy of a SystemBasic object.
   * Make defensive copies as needed.
   */
  public SystemBasic(SystemBasic t)
  {
    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    id = t.getId();
    created = t.getCreated();
    updated = t.getUpdated();
    tenant = t.getTenant();
    id = t.getId();
    systemType = t.getSystemType();
    owner = t.getOwner();
    host = t.getHost();
    defaultAuthnMethod = t.getDefaultAuthnMethod();
    canExec = t.getCanExec();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public int getSeqId() { return seqId; }
  public Instant getCreated() { return created; }
  public Instant getUpdated() { return updated; }
  public String getTenant() { return tenant; }
  public String getId() { return id; }
  public SystemType getSystemType() { return systemType; }
  public String getOwner() { return owner; }
  public String getHost() { return host; }
  public AuthnMethod getDefaultAuthnMethod() { return defaultAuthnMethod; }
  public boolean getCanExec() { return canExec; }
}
