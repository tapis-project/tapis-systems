package edu.utexas.tacc.tapis.systems.api.resources;

import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostSchedulerProfile;
import edu.utexas.tacc.tapis.systems.api.responses.RespSchedulerProfile;
import edu.utexas.tacc.tapis.systems.api.responses.RespSchedulerProfiles;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/*
 * JAX-RS REST resource for a SchedulerProfile
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see openapi-systems repo file SystemsAPI.yaml
 *       and note at top of SystemsResource.java
 */
@Path("/v3/systems/schedulerProfile")
public class SchedulerProfileResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SchedulerProfileResource.class);

  // Json schema resource files.
  private static final String FILE_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SchedulerProfilePostRequest.json";

  // Message keys
  private static final String INVALID_JSON_INPUT = "NET_INVALID_JSON_INPUT";
  private static final String JSON_VALIDATION_ERR = "TAPIS_JSON_VALIDATION_ERROR";
  private static final String CREATE_ERR = "SYSAPI_PRF_CREATE_ERROR";
  private static final String LIST_ERR = "SYSAPI_PRF_LIST_ERROR";
  private static final String LIB_UNAUTH = "SYSLIB_PRF_UNAUTH";
  private static final String API_UNAUTH = "SYSAPI_PRF_UNAUTH";
  private static final String TAPIS_FOUND = "TAPIS_FOUND";
  private static final String NOT_FOUND = "SYSAPI_PRF_NOT_FOUND";

  // Format strings
  private static final String PRF_CNT_STR = "%d scheduler profiles";

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // Default values
  public static final String DEFAULT_OWNER = TSystem.APIUSERID_VAR;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private Request _request;

  // **************** Inject Services using HK2 ****************
  @Inject
  private SystemsService systemsService;

  private final String className = getClass().getSimpleName();

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Create a scheduler profile
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to created object
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createSchedulerProfile(InputStream payloadStream,
                                         @Context SecurityContext securityContext)
  {
    String opName = "createSchedulerProfile";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString());

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson;
    String msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    ReqPostSchedulerProfile req;
    try { req = TapisGsonUtils.getGson().fromJson(rawJson, ReqPostSchedulerProfile.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // If req is null that is an unrecoverable error
    if (req == null)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, "N/A", "ReqPostSchedulerProfile == null");
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }


    // If owner not provided default to apiUserId
    String ownerId = req.owner;
    if (StringUtils.isBlank(req.owner)) ownerId = DEFAULT_OWNER;

    // Create a scheduler profile from the request
    var schedProfile =
            new SchedulerProfile(rUser.getOboTenantId(), ownerId, req.description, req.owner, req.moduleLoadCommand,
                                 req.modulesToLoad, req.hiddenOptions, null, null, null);


    // ---------------------------- Make service call to create -------------------------------
    // Pull out name for convenience
    String profileName = schedProfile.getName();
    try
    {
      systemsService.createSchedulerProfile(rUser, schedProfile, rawJson);
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_PRF_EXISTS"))
      {
        // IllegalStateException with msg containing PRF_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_PRF_EXISTS", rUser, profileName);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else if (e.getMessage().contains(LIB_UNAUTH))
      {
        // IllegalStateException with msg containing UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, profileName, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid object was passed in
        msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, profileName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, profileName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, profileName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + profileName;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.CREATED, ApiUtils.getMsgAuth("SYSAPI_PRF_CREATED", rUser, profileName), resp1);
  }

  /**
   * getSchedulerProfile
   * @param name - name of the profile
   * @param securityContext - user identity
   * @return Response with scheduler profile as the result
   */
  @GET
  @Path("{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchedulerProfile(@PathParam("name") String name,
                                      @Context SecurityContext securityContext)
  {
    String opName = "getSchedulerProfile";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "name="+name);

    SchedulerProfile schedulerProfile;
    try
    {
      schedulerProfile = systemsService.getSchedulerProfile(rUser, name);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_PRF_GET_ERROR", rUser, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Resource was not found.
    if (schedulerProfile == null)
    {
      String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, name);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the information.
    RespSchedulerProfile resp1 = new RespSchedulerProfile(schedulerProfile);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "SchedulerProfile", name), resp1);
  }

  /**
   * getSchedulerProfiles
   * Retrieve all scheduler profiles
   * @param securityContext - user identity
   * @return - list of profiles
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchedulerProfiles(@Context SecurityContext securityContext)
  {
    String opName = "getSchedulerProfiles";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString());

    // ------------------------- Retrieve records -----------------------------
    RespSchedulerProfiles successResponse;
    try
    {
      var schedulerProfiles = systemsService.getSchedulerProfiles(rUser);
      String itemCountStr = String.format(PRF_CNT_STR, schedulerProfiles.size());

      successResponse = new RespSchedulerProfiles(schedulerProfiles);

      return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "SchedulerProfiles", itemCountStr),
                                   successResponse);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth(LIST_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
  }

  /**
   * Delete a profile
   * @param name - name of profile
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @DELETE
  @Path("{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSchedulerProfile(@PathParam("name") String name,
                                         @Context SecurityContext securityContext)
  {
    String opName = "deleteSchedulerProfile";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "name="+name);

    // ---------------------------- Make service call to delete the profile -------------------------------
    int changeCount;
    String msg;
    try
    {
      changeCount = systemsService.deleteSchedulerProfile(rUser, name);
    }
    catch (NotAuthorizedException e)
    {
      msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, name, opName);
      _log.warn(msg);
      return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth("SYSAPI_PRF_DEL_ERROR", rUser, name, opName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PRF_DEL_ERROR", rUser, name, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    // Return the number of objects impacted.
    ResultChangeCount count = new ResultChangeCount();
    count.changes = changeCount;
    RespChangeCount resp1 = new RespChangeCount(count);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth("SYSAPI_PRF_DELETED", rUser, name), resp1);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Create an OK response given message and base response to put in result
   * @param msg - message for resp.message
   * @param resp - base response (the result)
   * @return - Final response to return to client
   */
  private static Response createSuccessResponse(Status status, String msg, RespAbstract resp)
  {
    return Response.status(status).entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp)).build();
  }
}
