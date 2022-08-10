package edu.utexas.tacc.tapis.systems.model;

import java.util.Set;

/*
 * System Share
 *
 */
public final class SystemShare
{

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  private final boolean publicShare;
  private final Set<String> users;


  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor for jOOQ with input parameter matching order of columns in DB
   * Also useful for testing
   */
  public SystemShare(boolean publicShare1, Set<String> sysIDs1)
  {
    publicShare = publicShare1;
    users = sysIDs1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public boolean isPublic() { return publicShare; }
  public Set<String> getUserList() { return users; }
}
