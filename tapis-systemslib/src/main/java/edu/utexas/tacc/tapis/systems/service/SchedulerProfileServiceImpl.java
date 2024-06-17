package edu.utexas.tacc.tapis.systems.service;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.dao.SystemsDao;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile.SchedulerProfileOperation;
import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.TSystem.APIUSERID_VAR;

/*
 * Service level methods for Scheduler Profiles.
 *   Uses Dao layer and other service library classes to perform all top level service operations.
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 */
@Service
public class SchedulerProfileServiceImpl
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(SchedulerProfileServiceImpl.class);

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // Use HK2 to inject singletons
  @Inject
  private SystemsDao dao;
  @Inject
  private AuthUtils authUtils;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Create a scheduler profile.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param schedulerProfile - Pre-populated SchedulerProfile object (including tenant and name)
   * @throws TapisException - for Tapis related exceptions
   * @throws IllegalStateException - resource exists OR is in invalid state
   * @throws IllegalArgumentException - invalid parameter passed in
   */
  public void createSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile schedulerProfile)
          throws TapisException, TapisClientException, IllegalStateException, IllegalArgumentException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.create;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (schedulerProfile == null) throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    // Construct Json string representing the resource about to be created
    String createJsonStr = TapisGsonUtils.getGson().toJson(schedulerProfile);
    log.trace(LibUtils.getMsgAuth("SYSLIB_CREATE_TRACE", rUser, createJsonStr));
    String oboTenant = schedulerProfile.getTenant();
    String schedProfileName = schedulerProfile.getName();

    // ---------------------------- Check inputs ------------------------------------
    // Required attributes: tenant, name, moduleLoadCommand
    if (StringUtils.isBlank(oboTenant) || StringUtils.isBlank(schedProfileName))
    {
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_CREATE_ERROR_ARG", rUser, schedProfileName));
    }

    // Check if schedulerProfile already exists
    if (dao.checkForSchedulerProfile(oboTenant, schedProfileName))
    {
      throw new IllegalStateException(LibUtils.getMsgAuth("SYSLIB_PRF_EXISTS", rUser, schedProfileName));
    }

    // ----------------- Resolve variables for any attributes that might contain them --------------------
    // For schedulerProfile this is only the owner which may be set to $apiUserId
    // Resolve owner if necessary. If empty or "${apiUserId}" then fill in with oboUser.
    String owner = schedulerProfile.getOwner();
    if (StringUtils.isBlank(owner) || owner.equalsIgnoreCase(APIUSERID_VAR)) schedulerProfile.setOwner(rUser.getOboUserId());

    // Check authorization
    authUtils.checkAuthForProfile(rUser, op, schedulerProfile.getName(), schedulerProfile.getOwner());

    // ---------------- Check constraints on SchedulerProfile attributes ------------------------
    validateSchedulerProfile(rUser, schedulerProfile);

    // No distributed transactions so no distributed rollback needed
    // Make Dao call to persist the resource
    dao.createSchedulerProfile(rUser, schedulerProfile);
  }

  /**
   * Get all scheduler profiles
   * NOTE: Anyone can read, no filtering based on auth.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @return List of scheduler profiles
   * @throws TapisException - for Tapis related exceptions
   */
  public List<SchedulerProfile> getSchedulerProfiles(ResourceRequestUser rUser) throws TapisException
  {
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    return dao.getSchedulerProfiles(rUser.getOboTenantId());
  }

  /**
   * getSchedulerProfile
   * NOTE: Anyone can read, no auth check
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - Name of the profile
   * @return schedulerProfile or null if not found or user not authorized.
   * @throws TapisException - for Tapis related exceptions
   */
  public SchedulerProfile getSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.read;
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));
    // Use dao to get resource.
    return dao.getSchedulerProfile(rUser.getOboTenantId(), name);
  }

  /**
   * Delete scheduler profile
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - name of profile
   * @throws TapisException - for Tapis related exceptions
   */
  public int deleteSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException, TapisClientException, IllegalArgumentException
  {
    SchedulerProfileOperation op = SchedulerProfileOperation.delete;
    // Check inputs. If anything null or empty throw an exception
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));

    String oboTenant = rUser.getOboTenantId();

    // If profile does not exist or has been deleted then return 0 changes
    if (!dao.checkForSchedulerProfile(oboTenant, name)) return 0;

    // Check authorization
    authUtils.checkAuthForProfile(rUser, op, name, null);

    // Use dao to delete the resource
    return dao.deleteSchedulerProfile(oboTenant, name);
  }

  /**
   * checkForSchedulerProfile
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param name - Name of the profile
   * @return true if system exists and has not been deleted, false otherwise
   * @throws TapisException - for Tapis related exceptions
   */
  public boolean checkForSchedulerProfile(ResourceRequestUser rUser, String name)
          throws TapisException
  {
    if (rUser == null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT_AUTHUSR"));
    if (StringUtils.isBlank(name))
      throw new IllegalArgumentException(LibUtils.getMsgAuth("SYSLIB_NULL_INPUT_PROFILE", rUser));

    return dao.checkForSchedulerProfile(rUser.getOboTenantId(), name);
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /**
   * Check constraints on SchedulerProfile attributes.
   * Collect and report as many errors as possible so they can all be fixed before next attempt
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param profile1 - the profile to check
   * @throws IllegalStateException - if any constraints are violated
   */
  private static void validateSchedulerProfile(ResourceRequestUser rUser, SchedulerProfile profile1) throws IllegalStateException
  {
    // Make api level checks, i.e. checks that do not involve a dao or service call.
    List<String> errMessages = profile1.checkAttributeRestrictions();

    // Now make checks that do require a dao or service call.
    // NOTE: Currently no such checks needed.

    // If validation failed throw an exception
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = SysUtils.getListOfErrors(rUser, profile1.getName(), errMessages);
      log.error(allErrors);
      throw new IllegalStateException(allErrors);
    }
  }
}
