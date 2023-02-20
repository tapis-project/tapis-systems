package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/*
 * Module load command + modules to load.
 * Item in a ModuleLoads list contained in a scheduler profile
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class ModuleLoadSpec
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************


  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private final String moduleLoadCommand;
  private final String[] modulesToLoad;

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public ModuleLoadSpec(String moduleLoadCommand1, String[] modulesToLoad1)
  {
    moduleLoadCommand = moduleLoadCommand1;
    modulesToLoad = (modulesToLoad1 == null) ? null : modulesToLoad1.clone();
  }

  /**
   * Copy constructor. Returns a deep copy.
   * The getters make defensive copies as needed.
   */
  public ModuleLoadSpec(ModuleLoadSpec t)
  {
    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    moduleLoadCommand = t.getModuleLoadCommand();
    modulesToLoad = t.getModulesToLoad();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Private methods *********************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public String getModuleLoadCommand() { return moduleLoadCommand; }
  public String[] getModulesToLoad() { return (modulesToLoad == null) ? null : modulesToLoad.clone(); }
}
