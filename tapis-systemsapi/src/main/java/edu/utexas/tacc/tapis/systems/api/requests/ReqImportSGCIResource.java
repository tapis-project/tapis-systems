package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;

import java.util.List;

/*
 * Class representing system attributes that can be set when creating a system by
 * importing an SGCI resource definition.
 * sgciResourceId is required, all others are optional.
 */
public final class ReqImportSGCIResource
{
  public String sgciResourceId; // ID of SGCI resource - required
  public String id;       // Name of the system
  public String description; // Description of the system
  public String owner;      // User who owns the system and has full privileges
  public boolean enabled; // Indicates if systems is currently enabled
  public String effectiveUserId; // User to use when accessing system, may be static or dynamic
  public Credential authnCredential; // Credential to be stored in or retrieved from the Security Kernel
  public List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  public String[] tags;       // List of arbitrary tags as strings
  public Object notes;      // Simple metadata as json
}