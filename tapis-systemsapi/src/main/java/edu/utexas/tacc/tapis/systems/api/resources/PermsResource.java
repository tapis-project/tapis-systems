package edu.utexas.tacc.tapis.systems.api.resources;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import org.glassfish.grizzly.http.server.Request;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.responses.RespNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.Permission;
import edu.utexas.tacc.tapis.systems.service.SystemsService;

/*
 * JAX-RS REST resource for Tapis System permissions
 *
 * These methods should do the minimal amount of validation and processing of incoming requests and
 *   then make the service method call.
 * One reason for this is the service methods are much easier to test.
 *
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-systemsapi/src/main/resources/SystemsAPI.yaml
 *       and note at top of GeneralResource.java
 * Annotations map HTTP verb + endpoint to method invocation.
 * Permissions are stored in the Security Kernel
 *
 */
@Path("/v3/systems/perms")
public class PermsResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(PermsResource.class);

  // Json schema resource files.
  private static final String FILE_PERMS_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/PermsRequest.json";

  // Field names used in Json
  private static final String PERMISSIONS_FIELD = "permissions";

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
   * Assign specified permissions for given system and user.
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/{systemId}/user/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response grantUserPerms(@PathParam("systemId") String systemId,
                                 @PathParam("userName") String userName,
                                 InputStream payloadStream,
                                 @Context SecurityContext securityContext)
  {
    String opName = "grantUserPerms";
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    // Check that we have all we need from the context
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,"userName="+userName);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "grantUserPerms");
    if (resp != null) return resp;

    // Read the payload into a string.
    String json;
    String msg;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_JSON_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg);
    }

    // ------------------------- Extract and validate payload -------------------------
    var permsList = new HashSet<Permission>();
    resp = checkAndExtractPayload(rUser, systemId, userName, json, permsList);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to assign the permissions
    try
    {
      service.grantUserPermissions(rUser, systemId, userName, permsList, json);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    String permsListStr = permsList.stream().map(Enum::name).collect(Collectors.joining(","));
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_GRANTED", rUser, systemId, userName, permsListStr);
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
      .build();
  }

  /**
   * getUserPerms
   * @return Response with list of permissions
   */
  @GET
  @Path("/{systemId}/user/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUserPerms(@PathParam("systemId") String systemId,
                               @PathParam("userName") String userName,
                               @Context SecurityContext securityContext)
  {
    String opName = "getUserPerms";
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    // Check that we have all we need from the context
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,"userName="+userName);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "getUserPerms");
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to get the permissions
    Set<Permission> perms;
    String msg;
    try { perms = service.getUserPermissions(rUser, systemId, userName); }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    ResultNameArray names = new ResultNameArray();
    List<String> permNames = new ArrayList<>();
    for (Permission perm : perms) { permNames.add(perm.name()); }
    names.names = permNames.toArray(TSystem.EMPTY_STR_ARRAY);
    RespNameArray resp1 = new RespNameArray(names);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
      MsgUtils.getMsg("TAPIS_FOUND", "System permissions", perms.size() + " items"), PRETTY, resp1)).build();
  }

  /**
   * Revoke permission for given system and user.
   * @return basic response
   */
  @DELETE
  @Path("/{systemId}/user/{userName}/{permission}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response revokeUserPerm(@PathParam("systemId") String systemId,
                                 @PathParam("userName") String userName,
                                 @PathParam("permission") String permissionStr,
                                 @Context SecurityContext securityContext)
  {
    String opName = "revokeUserPerm";
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    // Check that we have all we need from the context
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,"userName="+userName,"permission="+permissionStr);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "revokeUserPerm");
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to revoke the permissions
    var permsList = new HashSet<Permission>();
    String msg;
    try
    {
      Permission perm = Permission.valueOf(permissionStr);
      permsList.add(perm);
      service.revokeUserPermissions(rUser, systemId, userName, permsList, null);
    }
    catch (IllegalArgumentException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ENUM_ERROR", rUser, systemId, userName, permissionStr, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_REVOKED", rUser, systemId, userName, permissionStr);
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
      .build();
  }

  /**
   * Revoke permissions for given system and user.
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/{systemId}/user/{userName}/revoke")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response revokeUserPerms(@PathParam("systemId") String systemId,
                                  @PathParam("userName") String userName,
                                  InputStream payloadStream,
                                  @Context SecurityContext securityContext)
  {
    String opName = "revokeUserPerms";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    // Check that we have all we need from the context
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,"userName="+userName);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "revokeUserPerms");
    if (resp != null) return resp;

    // Read the payload into a string.
    String json;
    String msg;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_JSON_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg);
    }

    // ------------------------- Extract and validate payload -------------------------
    var permsList = new HashSet<Permission>();
    resp = checkAndExtractPayload(rUser, systemId, userName, json, permsList);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to revoke the permissions
    try
    {
      service.revokeUserPermissions(rUser, systemId, userName, permsList, json);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    String permsListStr = permsList.stream().map(Enum::name).collect(Collectors.joining(","));
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_REVOKED", rUser, systemId, userName, permsListStr);
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
      .build();
  }


  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check json payload and extract permissions list.
   * @param systemId - name of the system, for constructing response msg in case of err
   * @param userName - name of user associated with the perms request, for constructing response msg in case of err
   * @param json - Request json extracted from payloadStream
   * @param permsList - List for resulting permissions extracted from payload
   * @return - null if all checks OK else Response containing info
   */
  private Response checkAndExtractPayload(ResourceRequestUser rUser, String systemId, String userName,
                                          String json, Set<Permission> permsList)
  {
    String msg;
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_PERMS_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_JSON_INVALID", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg);
    }

    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);

    // Extract permissions from the request body
    JsonArray perms = null;
    if (obj.has(PERMISSIONS_FIELD)) perms = obj.getAsJsonArray(PERMISSIONS_FIELD);
    if (perms != null && perms.size() > 0)
    {
      for (int i = 0; i < perms.size(); i++)
      {
        // Remove quotes from around incoming string
        String permStr = StringUtils.remove(perms.get(i).toString(),'"');
        // Convert the string to an enum and add it to the list
        try {permsList.add(Permission.valueOf(permStr)); }
        catch (IllegalArgumentException e)
        {
          msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_ENUM_ERROR", rUser, systemId, userName, permStr, e.getMessage());
          _log.error(msg, e);
          throw new BadRequestException(msg);
        }
      }
    }

    msg = null;
    // Check values. We should have at least one permission
    if (perms == null || perms.size() <= 0)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_PERMS_NOPERMS", rUser, systemId, userName);
    }

    // If validation failed log error message and return response
    if (msg != null)
    {
      _log.error(msg);
      throw new BadRequestException(msg);
    }
    else return null;
  }
}
