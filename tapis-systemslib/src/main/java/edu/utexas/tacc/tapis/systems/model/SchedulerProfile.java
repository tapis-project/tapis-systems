package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/*
 * Scheduler Profile
 *
 * Tenant + name must be unique
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class SchedulerProfile
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Attribute names, also used as field names in Json
  public static final String NAME_FIELD = "name";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String OWNER_FIELD = "owner";

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum HiddenOption {MEM}
  public enum SchedulerProfileOperation {create, read, modify, delete, changeOwner}

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private final String tenant;   // Name of the tenant
  private final String name;
  private final String description;
  private String owner;
  private final String moduleLoadCommand;
  private final String[] modulesToLoad;
  private final List<HiddenOption> hiddenOptions;
  private UUID uuid;
  private final Instant created; // UTC time for when record was created
  private final Instant updated; // UTC time for when record was last updated

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor using non-updatable attributes.
   * Rather than exposing otherwise unnecessary setters we use a special constructor.
   */
  public SchedulerProfile(SchedulerProfile sp, String tenant1, String name1)
  {
    if (sp==null || StringUtils.isBlank(tenant1) || StringUtils.isBlank(name1))
      throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    tenant = tenant1;
    name = name1;

    created = sp.getCreated();
    updated = sp.getUpdated();
    description = sp.getDescription();
    owner = sp.getOwner();
    moduleLoadCommand = sp.getModuleLoadCommand();
    modulesToLoad = sp.getModulesToLoad();
    hiddenOptions = sp.getHiddenOptions();
    uuid = sp.getUuid();
  }

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public SchedulerProfile(String tenant1, String name1, String description1, String owner1,
                          String moduleLoadCommand1, String[] modulesToLoad1, List<HiddenOption> hiddenOptions1,
                          UUID uuid1, Instant created1, Instant updated1)
  {
    tenant = tenant1;
    name = name1;
    description = description1;
    owner = owner1;
    moduleLoadCommand = moduleLoadCommand1;
    modulesToLoad = (modulesToLoad1 == null) ? null : modulesToLoad1.clone();
    hiddenOptions = (hiddenOptions1 == null) ? null : new ArrayList<>(hiddenOptions1);
    uuid = uuid1;
    created = created1;
    updated = updated1;
  }

  /**
   * Copy constructor. Returns a deep copy.
   * The getters make defensive copies as needed.
   */
  public SchedulerProfile(SchedulerProfile t)
  {
    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    created = t.getCreated();
    updated = t.getUpdated();
    uuid = t.getUuid();
    tenant = t.getTenant();
    name = t.getName();
    description = t.getDescription();
    owner = t.getOwner();
    moduleLoadCommand = t.getModuleLoadCommand();
    modulesToLoad = t.getModulesToLoad();
    hiddenOptions = t.getHiddenOptions();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************
  /**
   * Check constraints on attributes.
   * Make checks that do not require a dao or service call.
   * Check only internal consistency and restrictions.
   *
   * @return  list of error messages, empty list if no errors
   */
  public List<String> checkAttributeRestrictions()
  {
    var errMessages = new ArrayList<String>();
    checkAttrRequired(errMessages);
    checkAttrValidity(errMessages);
    checkAttrStringLengths(errMessages);
    return errMessages;
  }


  // ************************************************************************
  // *********************** Private methods *********************************
  // ************************************************************************

  /**
   * Check for missing required attributes
   *   profileName
   */
  private void checkAttrRequired(List<String> errMessages)
  {
    if (StringUtils.isBlank(name)) errMessages.add(LibUtils.getMsg(TSystem.CREATE_MISSING_ATTR, NAME_FIELD));
  }

  /**
   * Check for invalid attributes
   *   systemId, host
   *   rootDir must start with /
   */
  private void checkAttrValidity(List<String> errMessages)
  {
    if (!StringUtils.isBlank(name) && !TSystem.isValidId(name))
        errMessages.add(LibUtils.getMsg(TSystem.INVALID_STR_ATTR, NAME_FIELD, name));
  }

  /**
   * Check for attribute strings that exceed limits
   *   id, description, owner, effectiveUserId, bucketName, rootDir
   *   dtnSystemId, dtnMountPoint, dtnMountSourcePath, jobWorkingDir
   */
  private void checkAttrStringLengths(List<String> errMessages)
  {
    if (!StringUtils.isBlank(name) && name.length() > TSystem.MAX_ID_LEN)
    {
      errMessages.add(LibUtils.getMsg(TSystem.TOO_LONG_ATTR, NAME_FIELD, TSystem.MAX_ID_LEN));
    }

    if (!StringUtils.isBlank(description) && description.length() > TSystem.MAX_DESCRIPTION_LEN)
    {
      errMessages.add(LibUtils.getMsg(TSystem.TOO_LONG_ATTR, DESCRIPTION_FIELD, TSystem.MAX_DESCRIPTION_LEN));
    }

    if (!StringUtils.isBlank(owner) && owner.length() > TSystem.MAX_USERNAME_LEN)
    {
      errMessages.add(LibUtils.getMsg(TSystem.TOO_LONG_ATTR, OWNER_FIELD, TSystem.MAX_USERNAME_LEN));
    }
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }
  public String getName() { return name; }
  public String getDescription() { return description; }
  public String getOwner() { return owner; }
  public void setOwner(String o) { owner = o; };
  public String getModuleLoadCommand() { return moduleLoadCommand; }
  public String[] getModulesToLoad() { return (modulesToLoad == null) ? null : modulesToLoad.clone(); }
  public List<HiddenOption> getHiddenOptions() { return (hiddenOptions == null) ? null : new ArrayList<>(hiddenOptions); }

  public UUID getUuid() { return uuid; }

  public void setUuid(UUID u) { uuid = u; }
}
