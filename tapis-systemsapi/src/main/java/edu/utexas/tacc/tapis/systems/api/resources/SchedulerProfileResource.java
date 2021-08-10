package edu.utexas.tacc.tapis.systems.api.resources;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBoolean;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultBoolean;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostSchedulerProfile;
import edu.utexas.tacc.tapis.systems.api.responses.RespSchedulerProfile;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.SchedulerProfile;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import static edu.utexas.tacc.tapis.systems.model.TSystem.AUTHN_CREDENTIAL_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.NOTES_FIELD;

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

  private static final String SYSTEMS_SVC = StringUtils.capitalize(TapisConstants.SERVICE_NAME_SYSTEMS);

  // Json schema resource files.
  private static final String FILE_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SchedulerProfilePostRequest.json";

  // Message keys
  private static final String INVALID_JSON_INPUT = "NET_INVALID_JSON_INPUT";
  private static final String JSON_VALIDATION_ERR = "TAPIS_JSON_VALIDATION_ERROR";
  private static final String UPDATE_ERR = "SYSAPI_UPDATE_ERROR";
  private static final String CREATE_ERR = "SYSAPI_CREATE_ERROR";
  private static final String SELECT_ERR = "SYSAPI_SELECT_ERROR";
  private static final String LIB_UNAUTH = "SYSLIB_UNAUTH";
  private static final String API_UNAUTH = "SYSAPI_SYS_UNAUTH";
  private static final String TAPIS_FOUND = "TAPIS_FOUND";
  private static final String NOT_FOUND = "SYSAPI_NOT_FOUND";
  private static final String UPDATED = "SYSAPI_UPDATED";

  // Format strings
  private static final String SYS_CNT_STR = "%d systems";

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private HttpHeaders _httpHeaders;
  @Context
  private Application _application;
  @Context
  private UriInfo _uriInfo;
  @Context
  private SecurityContext _securityContext;
  @Context
  private ServletContext _servletContext;
  @Context
  private Request _request;

  // **************** Inject Services using HK2 ****************
  @Inject
  private SystemsService systemsService;

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

    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

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

    // Create a scheduler profile from the request
    SchedulerProfile schedProfile =
            new SchedulerProfile(rUser.getOboTenantId(), req.name, req.description, req.owner, req.moduleLoadCommand,
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
        // IllegalStateException with msg containing SYS_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_PRF_EXISTS", rUser, profileName);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else if (e.getMessage().contains(LIB_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
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
    return createSuccessResponse(Status.CREATED, ApiUtils.getMsgAuth("SYSAPI_CREATED", rUser, profileName), resp1);
  }

  /**
   * getSchedulerProfile
   * @param profileId - name of the profile
   * @param securityContext - user identity
   * @return Response with system object as the result
   */
  @GET
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystem(@PathParam("id") String profileId,
                            @Context SecurityContext securityContext)
  {
    String opName = "getSchedulerProfile";
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    SchedulerProfile schedulerProfile;
    try
    {
      schedulerProfile = systemsService.getSchedulerProfile(rUser, profileId);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_GET_PRF_ERROR", rUser, profileId, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Resource was not found.
    if (schedulerProfile == null)
    {
      String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, profileId);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSchedulerProfile resp1 = new RespSchedulerProfile(schedulerProfile);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "SchedulerProfile", profileId), resp1);
  }

  /**
   * getSchedulerProfiles
   * Retrieve all scheduler profiles
   * @param securityContext - user identity
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchedulerProfiles(@Context SecurityContext securityContext)
  {
    String opName = "getSchedulerProfiles";
    // Trace this request.
    if (_log.isTraceEnabled()) logRequest(opName);

    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // TODO
    return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse("WIP", PRETTY)).build();

//    // ------------------------- Retrieve records -----------------------------
//    Response successResponse;
//    try
//    {
//      var schedulerProfiles = systemsService.getSchedulerProfiles(rUser);
//      resp1 = new RespSchedulerProfiles(schedulerProfiles);
//
//      return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, SYSTEMS_SVC, ), resp1);
//    }
//    catch (Exception e)
//    {
//      String msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
//      _log.error(msg, e);
//      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
//    }
//    return successResponse;
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  private void logRequest(String opName) {
    String msg = MsgUtils.getMsg("TAPIS_TRACE_REQUEST", getClass().getSimpleName(), opName,
            "  " + _request.getRequestURL());
    _log.trace(msg);
  }

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
