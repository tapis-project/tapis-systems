package edu.utexas.tacc.tapis.systems.api.utils;

import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A simple general purpose key-value pair class to make it easier to process json with the same structure.
 * Key may not contain the character "="
 */
public final class KeyValuePair
{
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(KeyValuePair.class);

  private final String key;   // Name for the logical queue
  private final String value;   // Name for the logical queue
  private String keyValueStr;

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public KeyValuePair(String key1, String value1)
  {
    key = key1;
    value = value1;
    keyValueStr = key1 + "=" + value1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getKey() { return key; }
  public String getValue() { return value; }

  @Override
  public String toString()
  {
    if (keyValueStr == null) keyValueStr = key + "=" + value;
    return keyValueStr;
  }

  public static KeyValuePair fromString(String s)
  {
    if (StringUtils.isBlank(s)) return new KeyValuePair("","");
    int e1 = s.indexOf('=');
    String k = s.substring(0, e1);
    String v = "";
    // Everything after "=" is the value
    if (e1 > 0) v = s.substring(e1+1);
    return new KeyValuePair(k, v);
  }
}
