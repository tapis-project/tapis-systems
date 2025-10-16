package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/*
 * System Share
 */
public final class SystemShare
{

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  private final boolean publicShare;
  private final Set<String> users;
  private final List<SkShare> skShares;


  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /*
   * Constructor taking all final arguments
   */
  public SystemShare(boolean publicShare1, Set<String> userIDs1, List<SkShare> skShares1)
  {
    publicShare = publicShare1;
    users = userIDs1;
    skShares = (skShares1 == null || skShares1.isEmpty()) ? Collections.emptyList() : skShares1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public boolean isPublic() { return publicShare; }
  public Set<String> getUserList() { return users; }
  public List<SkShare> getSkShares() { return skShares; }
}
