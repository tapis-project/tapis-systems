package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBoolean;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultBoolean;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostSystem;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPutSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemHistory;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystems;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import static edu.utexas.tacc.tapis.systems.model.TSystem.CAN_EXEC_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_AUTHN_METHOD_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EFFECTIVE_USER_ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.HOST_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.NOTES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.AUTHN_CREDENTIAL_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.SYSTEM_TYPE_FIELD;

/*
 * JAX-RS REST resource for a Tapis System (edu.utexas.tacc.tapis.systems.model.TSystem)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see openapi-systems repo file SystemsAPI.yaml
 *       and note at top of SystemsResource.java
 */
@Path("/v3/systems")
public class SystemResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemResource.class);

  private static final String SYSTEMS_SVC = StringUtils.capitalize(TapisConstants.SERVICE_NAME_SYSTEMS);

  // Json schema resource files.
  private static final String FILE_SYSTEM_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPostRequest.json";
  private static final String FILE_SYSTEM_PUT_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPutRequest.json";
  private static final String FILE_SYSTEM_UPDATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPatchRequest.json";
  private static final String FILE_SYSTEM_SEARCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemSearchRequest.json";
  private static final String FILE_SYSTEM_MATCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/MatchConstraintsRequest.json";

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

  // Operation names
  private static final String OP_ENABLE = "enableSystem";
  private static final String OP_DISABLE = "disableSystem";
  private static final String OP_CHANGEOWNER = "changeSystemOwner";
  private static final String OP_DELETE = "deleteSystem";
  private static final String OP_UNDELETE = "undeleteSystem";

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // Top level summary attributes to be included by default in some cases.
  public static final List<String> SUMMARY_ATTRS =
          new ArrayList<>(List.of(ID_FIELD, SYSTEM_TYPE_FIELD, OWNER_FIELD, HOST_FIELD,
                  EFFECTIVE_USER_ID_FIELD, DEFAULT_AUTHN_METHOD_FIELD, CAN_EXEC_FIELD));

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
   * Create a system
   * Create a system using a request body. System name must be unique within a tenant and can be composed of
   * alphanumeric characters and the following special characters [-._~].
   * Name must begin with an alphabetic character and can be no more than 80 characters in length.
   * Description is optional with a maximum length of 2048 characters.
   *
   * Credentials may be optionally provided in the authnCredential attribute of the request body.
   * The Systems service does not store the secrets, they are persisted in the Security Kernel.
   * The secrets are stored in the Security Kernel under a Tapis user name.
   * TODO/TBD
   *    Note that in addition to secrets the authnCredential attribute may contain a Tapis user and a login user.
   *    The Tapis user is used to associate the credentials with a login user.
   *    If the System has a static effectiveUserId then the System owner will always be the Tapis user.
   *    If the Tapis user is not provided then it defaults to the System owner.
   *    If the login user is not provided then it defaults to the Tapis user.
   *    A mapping between the Tapis user and the login user is recorded.
   *
   * By default any credentials provided for LINUX type systems are verified. Use query parameter
   * skipCredentialCheck=true to bypass initial verification of credentials.
   *
   * Note that certain attributes in the request body (such as tenant) are allowed but ignored so that the JSON
   * result returned by a GET may be modified and used when making a POST request to create a system.
   * The attributes that are allowed but ignored are
   *
   *   - tenant
   *   - uuid
   *   - deleted
   *   - created
   *   - updated
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to created object
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createSystem(InputStream payloadStream,
                               @QueryParam("skipCredentialCheck") @DefaultValue("false") boolean skipCredCheck,
                               @Context SecurityContext securityContext)
  {
    String opName = "createSystem";
    // Note that although the following approximately 30 line block of code is very similar for many endpoints the
    //   slight variations and use of fetched data makes it difficult to refactor into common routines.
    // Common routines might make the code even more complex and difficult to follow.

    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                                   "skipCredentialCheck="+skipCredCheck);

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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    ReqPostSystem req;
    // ------------------------- Create a TSystem from the json and validate constraints -------------------------
    try { req = TapisGsonUtils.getGson().fromJson(rawJson, ReqPostSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // If req is null that is an unrecoverable error
    if (req == null)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, "N/A", "ReqPostSystem == null");
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Create a TSystem from the request
    TSystem tSystem = createTSystemFromPostRequest(rUser.getOboTenantId(), req, rawJson);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (tSystem.getAuthnCredential() != null) scrubbedJson = maskCredSecrets(rawJson);
    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_CREATE_TRACE", rUser, scrubbedJson));

    // Fill in defaults and check constraints on TSystem attributes
    tSystem.setDefaults();
    resp = validateTSystem(tSystem, rUser);
    if (resp != null) return resp;

    // ---------------------------- Make service call to create the system -------------------------------
    // Pull out system name for convenience
    String systemId = tSystem.getId();
    try
    {
      service.createSystem(rUser, tSystem, skipCredCheck, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_SYS_EXISTS"))
      {
        // IllegalStateException with msg containing SYS_EXISTS indicates object exists - return 409 - Conflict
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_EXISTS", rUser, systemId);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else if (e.getMessage().contains(LIB_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
        _log.error(msg);
        return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success ------------------------------- 
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemId;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.CREATED, ApiUtils.getMsgAuth("SYSAPI_CREATED", rUser, systemId), resp1);
  }

  /**
   * Update selected attributes of a system
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PATCH
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response patchSystem(@PathParam("systemId") String systemId,
                              InputStream payloadStream,
                              @Context SecurityContext securityContext)
  {
    String opName = "patchSystem";
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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_UPDATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Create a PatchSystem from the json and validate constraints -------------------------
    PatchSystem patchSystem;
    try { patchSystem = TapisGsonUtils.getGson().fromJson(rawJson, PatchSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_PATCH_TRACE", rUser, rawJson));

    // Notes require special handling. Else they end up as a LinkedTreeMap which causes trouble when attempting to
    // convert to a JsonObject.
    patchSystem.setNotes(extractNotes(rawJson));

    // No attributes are required. Constraints validated and defaults filled in on server side.
    // No secrets in PatchSystem so no need to scrub

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      service.patchSystem(rUser, systemId, patchSystem, rawJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains(LIB_UNAUTH))
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
   * Update all updatable attributes of a system
   * @param systemId - name of the system
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @PUT
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response putSystem(@PathParam("systemId") String systemId,
                            @QueryParam("skipCredentialCheck") @DefaultValue("false") boolean skipCredCheck,
                            InputStream payloadStream,
                            @Context SecurityContext securityContext)
  {
    String opName = "putSystem";
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "skipCredentialCheck="+skipCredCheck, "systemId="+systemId);

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
    // NOTE that CREATE and PUT are very similar schemas.
    // Only difference should be for PUT there are no required properties.
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_PUT_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Create a System from the json and validate constraints -------------------------
    ReqPutSystem req;
    try { req = TapisGsonUtils.getGson().fromJson(rawJson, ReqPutSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // If req is null that is an unrecoverable error
    if (req == null)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", rUser, systemId, opName, "ReqPutSystem == null");
      _log.error(msg);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Create a TSystem from the request
    TSystem putSystem = createTSystemFromPutRequest(rUser.getOboTenantId(), systemId, req, rawJson);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (putSystem.getAuthnCredential() != null) scrubbedJson = maskCredSecrets(rawJson);
    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_PUT_TRACE", rUser, scrubbedJson));

    // Fill in defaults and check constraints on TSystem attributes
    // NOTE: We do not have all the Tapis System attributes yet so we cannot validate it
    putSystem.setDefaults();

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      service.putSystem(rUser, putSystem, skipCredCheck, scrubbedJson);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains(LIB_UNAUTH))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth(API_UNAUTH, rUser, systemId, opName);
        _log.warn(msg);
        return Response.status(Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid PutSystem was passed in
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
   * Enable a system
   * @param systemId - name of system
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @POST
  @Path("{systemId}/enable")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response enableSystem(@PathParam("systemId") String systemId,
                               @Context SecurityContext securityContext)
  {
    return postSystemSingleUpdate(OP_ENABLE, systemId, null, securityContext);
  }

  /**
   * Disable a system
   * @param systemId - name of the system
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @POST
  @Path("{systemId}/disable")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response disableSystem(@PathParam("systemId") String systemId,
                                @Context SecurityContext securityContext)
  {
    return postSystemSingleUpdate(OP_DISABLE, systemId, null, securityContext);
  }

  /**
   * Delete a system
   * @param systemId - name of system
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @POST
  @Path("{systemId}/delete")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSystem(@PathParam("systemId") String systemId,
                               @Context SecurityContext securityContext)
  {
    return postSystemSingleUpdate(OP_DELETE, systemId, null, securityContext);
  }

  /**
   * Undelete a system
   * @param systemId - name of the system
   * @param securityContext - user identity
   * @return - response with change count as the result
   */
  @POST
  @Path("{systemId}/undelete")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response undeleteSystem(@PathParam("systemId") String systemId,
                                 @Context SecurityContext securityContext)
  {
    return postSystemSingleUpdate(OP_UNDELETE, systemId, null, securityContext);
  }

  /**
   * Change owner of a system
   * @param systemId - name of the system
   * @param userName - name of the new owner
   * @param securityContext - user identity
   * @return response containing reference to updated object
   */
  @POST
  @Path("{systemId}/changeOwner/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response changeSystemOwner(@PathParam("systemId") String systemId,
                                    @PathParam("userName") String userName,
                                    @Context SecurityContext securityContext)
  {
    return postSystemSingleUpdate(OP_CHANGEOWNER, systemId, userName, securityContext);
  }

  /**
   * getSystem
   * @param systemId - name of the system
   * @param authnMethodStr - authn method to use instead of default
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @param skipTapisAuth - skip tapis perms auth, calling service has checked.
   * @param securityContext - user identity
   * @return Response with system object as the result
   */
  @GET
  @Path("{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystem(@PathParam("systemId") String systemId,
                            @QueryParam("authnMethod") @DefaultValue("") String authnMethodStr,
                            @QueryParam("requireExecPerm") @DefaultValue("false") boolean requireExecPerm,
                            @QueryParam("returnCredentials") @DefaultValue("false") boolean getCreds,
                            @QueryParam("skipTapisAuthorization") @DefaultValue("false") boolean skipTapisAuth,
                            @Context SecurityContext securityContext)
  {
    String opName = "getSystem";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                                   "systemId="+systemId, "authnMethod="+authnMethodStr,
                                                   "requireExecPerm="+requireExecPerm,
                                                   "returnCredentials="+getCreds,
                                                   "skipTapisAuthorization="+skipTapisAuth);

    // Check that authnMethodStr is valid if is passed in
    AuthnMethod authnMethod = null;
    try { if (!StringUtils.isBlank(authnMethodStr)) authnMethod =  AuthnMethod.valueOf(authnMethodStr); }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_ACCMETHOD_ENUM_ERROR", rUser, systemId, authnMethodStr, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    List<String> selectList = threadContext.getSearchParameters().getSelectList();

    TSystem tSystem;
    try
    {
      tSystem = service.getSystem(rUser, systemId, authnMethod, requireExecPerm, getCreds, skipTapisAuth);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Resource was not found.
    if (tSystem == null)
    {
      String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(tSystem, selectList);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "System", systemId), resp1);
  }

  /**
   * getHistory
   * @param systemId - name of the system
   * @param securityContext - user identity
   * @return Response with system history object as the result
   */
  @GET
  @Path("{systemId}/history")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getHistory(@PathParam("systemId") String systemId,
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
    List<SystemHistoryItem> systemHistory;
    
    try
    {
      // Retrieve system history List
      systemHistory = service.getSystemHistory(rUser, systemId);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    
    // System not found
    if (systemHistory == null || systemHistory.size()==0) {
   	 String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
   	  _log.warn(msg);
   	  return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    
    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system history information.
    RespSystemHistory resp1 = new RespSystemHistory(systemHistory);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "SystemHistory", systemId), resp1);
  }
  
  /**
   * isEnabled
   * Check if resource is enabled.
   * @param systemId - name of system
   * @param securityContext - user identity
   * @return Response with boolean result
   */
  @GET
  @Path("{systemId}/isEnabled")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response isEnabled(@PathParam("systemId") String systemId,
                            @Context SecurityContext securityContext)
  {
    String opName = "isEnabled";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);

    boolean isEnabled;
    try
    {
      isEnabled = service.isEnabled(rUser, systemId);
    }
    catch (NotFoundException e)
    {
      String msg = ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we made the check
    ResultBoolean respResult = new ResultBoolean();
    respResult.aBool = isEnabled;
    RespBoolean resp1 = new RespBoolean(respResult);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg("TAPIS_FOUND", "System", systemId), resp1);
  }

  /**
   * getSystems
   * Retrieve all systems accessible by requester and matching any search conditions provided.
   * NOTE: The query parameters search, limit, orderBy, skip, startAfter are all handled in the filter
   *       QueryParametersRequestFilter. No need to use @QueryParam here.
   * @param securityContext - user identity
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystems(@Context SecurityContext securityContext,
                             @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted)
  {
    String opName = "getSystems";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "showDeleted="+showDeleted);

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, null, srchParms, showDeleted);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    return successResponse;
  }

  /**
   * searchSystemsQueryParameters
   * Dedicated search endpoint for System resource. Search conditions provided as query parameters.
   * @param securityContext - user identity
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Path("search")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsQueryParameters(@Context SecurityContext securityContext,
                                               @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted)
  {
    String opName = "searchSystemsGet";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "showDeleted="+showDeleted);

    // Create search list based on query parameters
    // Note that some validation is done for each condition but the back end will handle translating LIKE wildcard
    //   characters (* and !) and deal with escaped characters.
    List<String> searchList;
    try
    {
      searchList = SearchUtils.buildListFromQueryParms(_uriInfo.getQueryParameters());
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SEARCH_ERROR", rUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();
    srchParms.setSearchList(searchList);

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, null, srchParms, showDeleted);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    return successResponse;
  }

  /**
   * searchSystemsRequestBody
   * Dedicated search endpoint for System resource. Search conditions provided in a request body.
   * Request body contains an array of strings that are concatenated to form the full SQL-like search string.
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @param showDeleted - whether or not to included resources that have been marked as deleted.
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @POST
  @Path("search")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsRequestBody(InputStream payloadStream,
                                           @Context SecurityContext securityContext,
                                           @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted)
  {
    String opName = "searchSystemsPost";
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "showDeleted="+showDeleted);

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
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_SEARCH_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Construct final SQL-like search string using the json
    // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
    // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
    String sqlSearchStr;
    try
    {
      sqlSearchStr = SearchUtils.getSearchFromRequestJson(rawJson);
    }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, sqlSearchStr, srchParms, showDeleted);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    return successResponse;
  }

//  /**
//   * matchConstraints
//   * Retrieve details for systems. Use request body to specify constraint conditions as an SQL-like WHERE clause.
//   * Request body contains an array of strings that are concatenated to form the full SQL-like search string.
//   * @param payloadStream - request body
//   * @param securityContext - user identity
//   * @return - list of systems accessible by requester and matching constraint conditions.
//   */
//  @POST
//  @Path("match/constraints")
//  @Consumes(MediaType.APPLICATION_JSON)
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response matchConstraints(InputStream payloadStream,
//                                   @Context SecurityContext securityContext)
//  {
//    String opName = "matchConstraints";
//    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
//    // Utility method returns null if all OK and appropriate error response if there was a problem.
//    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
//    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
//    if (resp != null) return resp;
//
//    // Create a user that collects together tenant, user and request information needed by the service call
//    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
//
//  // Trace this request.
//    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "showDeleted="+showDeleted);
//
//    // ------------------------- Extract and validate payload -------------------------
//    // Read the payload into a string.
//    String rawJson;
//    String msg;
//    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
//    catch (Exception e)
//    {
//      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
//      _log.error(msg, e);
//      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
//    }
//    // Create validator specification and validate the json against the schema
//    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_MATCH_REQUEST);
//    try { JsonValidator.validate(spec); }
//    catch (TapisJSONException e)
//    {
//      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
//      _log.error(msg, e);
//      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
//    }
//
//    // Construct final SQL-like search string using the json
//    // When put together full string must be a valid SQL-like where clause. This will be validated in the service call.
//    // Not all SQL syntax is supported. See SqlParser.jj in tapis-shared-searchlib.
//    String matchStr;
//    try
//    {
//      matchStr = SearchUtils.getMatchFromRequestJson(rawJson);
//    }
//    catch (JsonSyntaxException e)
//    {
//      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
//      _log.error(msg, e);
//      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
//    }
//
//    // ------------------------- Retrieve records -----------------------------
//    List<TSystem> systems;
//    try {
//      systems = systemsService.getSystemsSatisfyingConstraints(rUser, matchStr);
//    }
//    catch (Exception e)
//    {
//      msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
//      _log.error(msg, e);
//      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
//    }
//
//    if (systems == null) systems = Collections.emptyList();
//
//    // ---------------------------- Success -------------------------------
//    RespSystems resp1 = new RespSystems(systems);
//    String itemCountStr = String.format(SYS_CNT_STR, systems.size());
//    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, SYSTEMS_SVC, itemCountStr), resp1);
//  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * changeOwner, enable, disable, delete and undelete follow same pattern
   * Note that userName only used for changeOwner
   * @param opName Name of operation.
   * @param systemId Id of system to update
   * @param userName new owner name for op changeOwner
   * @param securityContext Security context from client call
   * @return Response to be returned to the client.
   */
  private Response postSystemSingleUpdate(String opName, String systemId, String userName,
                                          SecurityContext securityContext)
  {
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
    {
      // NOTE: We deliberately do not check for blank. If empty string passed in we want to record it here.
      if (userName!=null)
        ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, "userName="+userName);
      else
        ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId);
    }

    // ---------------------------- Make service call to update the system -------------------------------
    int changeCount;
    String msg;
    try
    {
      if (OP_ENABLE.equals(opName))
        changeCount = service.enableSystem(rUser, systemId);
      else if (OP_DISABLE.equals(opName))
        changeCount = service.disableSystem(rUser, systemId);
      else if (OP_DELETE.equals(opName))
        changeCount = service.deleteSystem(rUser, systemId);
      else if (OP_UNDELETE.equals(opName))
        changeCount = service.undeleteSystem(rUser, systemId);
      else
        changeCount = service.changeSystemOwner(rUser, systemId, userName);
    }
    catch (NotFoundException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOT_FOUND", rUser, systemId);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    catch (IllegalStateException e)
    {
      if (e.getMessage().contains("SYSLIB_UNAUTH"))
      {
        // IllegalStateException with msg containing SYS_UNAUTH indicates operation not authorized for apiUser - return 401
        msg = ApiUtils.getMsgAuth("SYSAPI_SYS_UNAUTH", rUser, systemId, opName);
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
    // Return the number of objects impacted.
    ResultChangeCount count = new ResultChangeCount();
    count.changes = changeCount;
    RespChangeCount resp1 = new RespChangeCount(count);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
  }

  /**
   * Create a TSystem from a ReqPostSystem
   * Check for req == null should have already been done
   */
  private static TSystem createTSystemFromPostRequest(String tenantId, ReqPostSystem req, String rawJson)
  {
    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);

    var tSystem = new TSystem(-1, tenantId, req.id, req.description, req.systemType, req.owner, req.host,
                       req.enabled, req.effectiveUserId, req.defaultAuthnMethod, req.bucketName, req.rootDir,
                       req.port, req.useProxy, req.proxyHost, req.proxyPort,
                       req.dtnSystemId, req.dtnMountPoint, req.dtnMountSourcePath, req.isDtn,
                       req.canExec, req.jobRuntimes, req.jobWorkingDir, req.jobEnvVariables, req.jobMaxJobs,
                       req.jobMaxJobsPerUser, req.canRunBatch, req.mpiCmd, req.batchScheduler, req.batchLogicalQueues,
                       req.batchDefaultLogicalQueue, req.batchSchedulerProfile, req.jobCapabilities,
                       req.tags, notes, req.importRefId, null, false, null, null);
    tSystem.setAuthnCredential(req.authnCredential);
    return tSystem;
  }

  /**
   * Create a TSystem from a ReqPutSystem
   */
  private static TSystem createTSystemFromPutRequest(String tenantId, String systemId, ReqPutSystem req, String rawJson)
  {
    // Extract Notes from the raw json.
    Object notes = extractNotes(rawJson);

    // NOTE: Following attributes are not updatable and must be filled in on service side.
    TSystem.SystemType systemType = null;
    String owner = null;
    boolean enabled = true;
    String bucketName = null;
    String rootDir = null;
    boolean isDtn = false;
    boolean canExec = true;
    var tSystem = new TSystem(-1, tenantId, systemId, req.description, systemType, owner, req.host,
            enabled, req.effectiveUserId, req.defaultAuthnMethod, bucketName, rootDir,
            req.port, req.useProxy, req.proxyHost, req.proxyPort,
            req.dtnSystemId, req.dtnMountPoint, req.dtnMountSourcePath, isDtn,
            canExec, req.jobRuntimes, req.jobWorkingDir, req.jobEnvVariables, req.jobMaxJobs, req.jobMaxJobsPerUser,
            req.canRunBatch, req.mpiCmd, req.batchScheduler, req.batchLogicalQueues, req.batchDefaultLogicalQueue,
            req.batchSchedulerProfile, req.jobCapabilities, req.tags, notes, req.importRefId, null, false, null, null);
    tSystem.setAuthnCredential(req.authnCredential);
    return tSystem;
  }

  /**
   * Fill in defaults and check restrictions on TSystem attributes
   * Use TSystem method to check internal consistency of attributes.
   * If DTN is used verify that dtnSystemId exists with isDtn = true
   * Collect and report as many errors as possible so they can all be fixed before next attempt
   * NOTE: JsonSchema validation should handle some of these checks but we check here again for robustness.
   *
   * @return null if OK or error Response
   */
  private Response validateTSystem(TSystem tSystem1, ResourceRequestUser rUser)
  {
    String msg;

    // Make call for lib level validation
    List<String> errMessages = tSystem1.checkAttributeRestrictions();

    // Now validate attributes that have special handling at API level.

    // If DTN is used (i.e. dtnSystemId is set) verify that dtnSystemId exists with isDtn = true
    if (!StringUtils.isBlank(tSystem1.getDtnSystemId()))
    {
      TSystem dtnSystem = null;
      try
      {
        dtnSystem = service.getSystem(rUser, tSystem1.getDtnSystemId(), null, false, false, false);
      }
      catch (NotAuthorizedException e)
      {
        msg = ApiUtils.getMsg("SYSAPI_DTN_NOT_AUTH", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
      catch (TapisClientException | TapisException e)
      {
        msg = ApiUtils.getMsg("SYSAPI_DTN_CHECK_ERROR", tSystem1.getDtnSystemId(), e.getMessage());
        _log.error(msg, e);
        errMessages.add(msg);
      }
      if (dtnSystem == null)
      {
        msg = ApiUtils.getMsg("SYSAPI_DTN_NO_SYSTEM", tSystem1.getDtnSystemId());
        errMessages.add(msg);
      }
      else if (!dtnSystem.isDtn())
      {
          msg = ApiUtils.getMsg("SYSAPI_DTN_NOT_DTN", tSystem1.getDtnSystemId());
          errMessages.add(msg);
      }
    }

    // If validation failed log error message and return response
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = ApiUtils.getListOfErrors(errMessages, rUser, tSystem1.getId());
      _log.error(allErrors);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(allErrors, PRETTY)).build();
    }
    return null;
  }

  /**
   * Extract notes from the incoming json
   */
  private static Object extractNotes(String rawJson)
  {
    Object notes = null;
    // Check inputs
    if (StringUtils.isBlank(rawJson)) return notes;
    // Turn the request string into a json object and extract the notes object
    JsonObject topObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!topObj.has(NOTES_FIELD)) return notes;
    notes = topObj.getAsJsonObject(NOTES_FIELD);
    return notes;
  }

  /**
   * AuthnCredential details can contain secrets. Mask any secrets given
   * and return a string containing the final redacted Json.
   * @param rawJson Json from request
   * @return A string with any secrets masked out
   */
  private static String maskCredSecrets(String rawJson)
  {
    if (StringUtils.isBlank(rawJson)) return rawJson;
    // Get the Json object and prepare to extract info from it
    JsonObject sysObj = TapisGsonUtils.getGson().fromJson(rawJson, JsonObject.class);
    if (!sysObj.has(AUTHN_CREDENTIAL_FIELD)) return rawJson;
    var credObj = sysObj.getAsJsonObject(AUTHN_CREDENTIAL_FIELD);
    maskSecret(credObj, CredentialResource.PASSWORD_FIELD);
    maskSecret(credObj, CredentialResource.PRIVATE_KEY_FIELD);
    maskSecret(credObj, CredentialResource.PUBLIC_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_KEY_FIELD);
    maskSecret(credObj, CredentialResource.ACCESS_SECRET_FIELD);
    maskSecret(credObj, CredentialResource.CERTIFICATE_FIELD);
    sysObj.remove(AUTHN_CREDENTIAL_FIELD);
    sysObj.add(AUTHN_CREDENTIAL_FIELD, credObj);
    return sysObj.toString();
  }

  /**
   * If the Json object contains a non-blank value for the field then replace the value with the mask value.
   */
  private static void maskSecret(JsonObject credObj, String field)
  {
    if (!StringUtils.isBlank(ApiUtils.getValS(credObj.get(field), "")))
    {
      credObj.remove(field);
      credObj.addProperty(field, SECRETS_MASK);
    }
  }

  /**
   *  Common method to return a list of systems given a search list and search parameters.
   *  srchParms must be non-null
   *  One of srchParms.searchList or sqlSearchStr must be non-null
   */
  private Response getSearchResponse(ResourceRequestUser rUser, String sqlSearchStr,
                                     SearchParameters srchParms, boolean showDeleted)
          throws Exception
  {
    RespAbstract resp1;
    List<TSystem> systems;
    int totalCount = -1;
    String itemCountStr;

    List<String> searchList = srchParms.getSearchList();
    List<String> selectList = srchParms.getSelectList();
    if (selectList == null || selectList.isEmpty()) selectList = SUMMARY_ATTRS;

    // If limit was not specified then use the default
    int limit = (srchParms.getLimit() == null) ? SearchParameters.DEFAULT_LIMIT : srchParms.getLimit();
    // Set some variables to make code easier to read
    int skip = srchParms.getSkip();
    String startAfter = srchParms.getStartAfter();
    boolean computeTotal = srchParms.getComputeTotal();
    String orderBy = srchParms.getOrderBy();
    List<OrderBy> orderByList = srchParms.getOrderByList();

    if (StringUtils.isBlank(sqlSearchStr))
      systems = service.getSystems(rUser, searchList, limit, orderByList, skip,
                                          startAfter, showDeleted);
    else
      systems = service.getSystemsUsingSqlSearchStr(rUser, sqlSearchStr, limit,
                                                           orderByList, skip, startAfter, showDeleted);
    if (systems == null) systems = Collections.emptyList();
    itemCountStr = String.format(SYS_CNT_STR, systems.size());
    if (computeTotal && limit <= 0) totalCount = systems.size();

    // If we need the count and there was a limit then we need to make a call
    if (computeTotal && limit > 0)
    {
      totalCount = service.getSystemsTotalCount(rUser, searchList, orderByList,
                                                       startAfter, showDeleted);
    }

    // ---------------------------- Success -------------------------------
    resp1 = new RespSystems(systems, limit, orderBy, skip, startAfter, totalCount, selectList);

    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, SYSTEMS_SVC, itemCountStr), resp1);
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
