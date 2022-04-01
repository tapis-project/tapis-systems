package edu.utexas.tacc.tapis.systems.model;

import org.apache.commons.lang3.StringUtils;

/*
 * Class for KeyValuePair in a System definition.
 * Key should not contain the character "=" because it would change the value
 * TODO/TBD: Make immutable? Check for use of setters
 * TODO/TBD: Override equals/hashCode so List.equals can work. For change history checking
 *           Make comparable?
 *           Use google's autoValue annotation for equals/hashCode?
 *           Better yet use java's new record keyword? But turns out this breaks gson.
 */
public final class KeyValuePair
{
  private String key;
  private String value = "";
  // Mark keyValueStr as transient so Gson will not include it.
  private transient String keyValueStr;

  public KeyValuePair(String key1, String value1)
  {
    key = key1;
    value = value1;
    keyValueStr = key1 + "=" + value1;
  }

  public String getKey() { return key; }
  public void setKey(String s) { key = s; }
  public String getValue() { return value; }
  public void setValue(String s) { value = StringUtils.isBlank(s) ? "" : s; }

  @Override
  public String toString()
  {
    if (StringUtils.isBlank(value)) value = "";
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
