package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonSyntaxException;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemShare;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.SystemShare;
import edu.utexas.tacc.tapis.systems.service.SystemsService;


/*
 * JAX-RS REST resource for Tapis System share
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-systemsapi/src/main/resources/SystemsAPI.yaml
 *       and note at top of GeneralResource.java
 * Annotations map HTTP verb + endpoint to method invocation.
 * Secrets are stored in the Security Kernel
 *
 */
@Path("/v3/systems")
public class ShareResource {
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(ShareResource.class);

  //Json schema resource files.
  private static final String SHARE_SYSTEM_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/ShareSystemRequest.json";
 
  // Message keys
  private static final String NOT_FOUND = "SYSAPI_NOT_FOUND";
  private static final String INVALID_JSON_INPUT = "NET_INVALID_JSON_INPUT";
  private static final String JSON_VALIDATION_ERR = "TAPIS_JSON_VALIDATION_ERROR";
  private static final String UPDATE_ERR = "SYSAPI_UPDATE_ERROR";
  private static final String API_UNAUTH = "SYSAPI_SYS_UNAUTH";
  private static final String UPDATED = "SYSAPI_UPDATED";

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
  private SystemsService service;
  
  private final String className = getClass().getSimpleName();

  
  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************
  /**
   * getShare
   * @param appId - name of the app
   * @param securityContext - user identity
   * @return Response with share information object as the result
   */
  @GET
  @Path("/share/{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getShareSystem(@PathParam("systemId") String systemId,
        @Context SecurityContext securityContext)
  {
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    //RespAbstract resp1;
    SystemShare systemShare;
    
    try
    {
      // Retrieve system share object
      systemShare = service.getSystemShare(rUser, systemId);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    
    // System not found
    if (systemShare == null) {
     String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    
    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system share information.
    RespSystemShare resp1 = new RespSystemShare(systemShare);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(UPDATED, "SystemShare", systemId), resp1);
  }
  
  /**
   * Create or update sharing information for a system
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("/share/{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response shareSystem(@PathParam("systemId") String systemId,
                              InputStream payloadStream,
                              @Context SecurityContext securityContext)
  {
    String opName = "createUpdateShare";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);

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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, SHARE_SYSTEM_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Create a SystemShare from the json and validate constraints -------------------------
    SystemShare systemShare;
    try { systemShare = TapisGsonUtils.getGson().fromJson(rawJson, SystemShare.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_PUT_TRACE", rUser, rawJson));

    // No attributes are required. Constraints validated and defaults filled in on server side.

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      service.shareSystem(rUser, systemId, systemShare, rawJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains(API_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
  }
  
  /**
   * Create or update sharing information for a system. The system will be unshared with the list of users 
   * provided in the request body
   * 
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("/unshare/{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response unshareSystem(@PathParam("systemId") String systemId,
                              InputStream payloadStream,
                              @Context SecurityContext securityContext)
  {
    String opName = "unshare";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);

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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, SHARE_SYSTEM_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Create a SystemShare from the json and validate constraints -------------------------
    SystemShare systemShare;
    try { systemShare = TapisGsonUtils.getGson().fromJson(rawJson, SystemShare.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_PUT_TRACE", rUser, rawJson));

    // No attributes are required. Constraints validated and defaults filled in on server side.

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      service.shareSystem(rUser, systemId, systemShare, rawJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains(API_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
  }
  
  /**
   * Create or update sharing information for a system
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("/share/{systemId}/public")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response shareSystemPublicly(@PathParam("systemId") String systemId,
                              @Context SecurityContext securityContext)
  {
    
    String opName = "sharePublicly";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);

    // No attributes are required. Constraints validated and defaults filled in on server side.
    // No secrets in PatchSystem so no need to scrub

    // ---------------------------- Make service call to update the system -------------------------------
    String msg;
    try
    {
      service.shareSystemPublicly(rUser, systemId);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains(API_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PatchSystem was passed in
        msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
  }
    /**
     * Unsharing a system publicly
     * @param systemId - name of the system
     * @param payloadStream - request body
     * @param securityContext - user identity
     * @return response containing reference to updated object
     */
    @POST
    @Path("/unshare/{systemId}/public")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response unshareSystemPublicly(@PathParam("systemId") String systemId,
                                @Context SecurityContext securityContext)
    {
      
      String opName = "sharePublicly";
      // ------------------------- Retrieve and validate thread context -------------------------
      TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
      // Check that we have all we need from the context, the jwtTenantId and jwtUserId
      // Utility method returns null if all OK and appropriate error response if there was a problem.
      Response resp = ApiUtils.checkContext(threadContext, PRETTY);
      if (resp != null) return resp;

      // Create a user that collects together tenant, user and request information needed by the service call
      ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

      // Trace this request.
      if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);

      // No attributes are required. Constraints validated and defaults filled in on server side.
      // No secrets in PatchSystem so no need to scrub

      // ---------------------------- Make service call to update the system -------------------------------
      String msg;
      try
      {
        service.unshareSystemPublicly(rUser, systemId);
      }
      catch (NotFoundException e)
      {
        msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
        _log.warn(msg);
        return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      catch (IllegalStateException e)
      {
        if (e.getMessage().contains(API_UNAUTH))
        {
          // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
          msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
          _log.warn(msg);
          return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
        }
        else
        {
          // IllegalStateException indicates an Invalid PatchSystem was passed in
          msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
          _log.error(msg);
          return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
        }
      }
      catch (IllegalArgumentException e)
      {
        // IllegalArgumentException indicates somehow a bad argument made it this far
        msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      catch (Exception e)
      {
        msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
        _log.error(msg, e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
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