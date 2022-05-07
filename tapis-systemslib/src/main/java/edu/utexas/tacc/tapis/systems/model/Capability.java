package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.util.Objects;

/*
 * Capability class representing a capability supported by a TSystem, such as what job schedulers the system supports,
 *   what software is on the system, the hardware on which the system is running, the type of OS the system is running,
 *   the version of the OS, container support, etc.
 * Each TSystem definition contains a list of capabilities supported by that system.
 * An Application or Job definition may specify required capabilities.
 * Used for determining eligible systems for running an application or job.
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 * Tenant + system + category + name must be unique.
 */
public final class Capability
{
  public enum Category {SCHEDULER, OS, HARDWARE, SOFTWARE, JOB, CONTAINER, MISC, CUSTOM}
  public enum Datatype{STRING, INTEGER, BOOLEAN, NUMBER, TIMESTAMP}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  public static final String DEFAULT_VALUE = "";
  public static final int DEFAULT_PRECEDENCE = 100;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final Category category; // Type or category of capability
  private final String name;   // Name of the capability
  private final Datatype datatype; // Datatype associated with the value
  private final int precedence;  // Precedence. Higher number has higher precedence.
  private final String value;  // Value or range of values

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  // Constructor initializing all fields.
  public Capability(Category category1, String name1, Datatype datatype1, int precedence1, String value1)
  {
    category = category1;
    name = name1;
    datatype = datatype1;
    precedence = precedence1;
    value = value1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public Category getCategory() { return category; }
  public String getName() { return name; }
  public Datatype getDatatype() { return datatype; }
  public int getPrecedence() { return precedence; }
  public String getValue() { return value; }

  @Override
  public String toString() {return TapisUtils.toString(this);}

  @Override
  public boolean equals(Object o)
  {
    if (o == this) return true;
    // Note: no need to check for o==null since instanceof will handle that case
    if (!(o instanceof Capability)) return false;
    var that = (Capability) o;
    return (Objects.equals(this.category, that.category) && Objects.equals(this.name, that.name) &&
            Objects.equals(this.datatype, that.datatype) && this.precedence==that.precedence &&
            Objects.equals(this.value, that.value));
  }

  @Override
  public int hashCode()
  {
    int retVal = (name == null ? 1 : name.hashCode());
    retVal = 31 * retVal + (category == null ? 0 : category.hashCode());
    retVal = 31 * retVal + (datatype == null ? 0 : datatype.hashCode());
    retVal = 31 * retVal + Integer.hashCode(precedence);
    retVal = 31 * retVal + (value == null ? 0 : value.hashCode());
    return retVal;
  }
}
