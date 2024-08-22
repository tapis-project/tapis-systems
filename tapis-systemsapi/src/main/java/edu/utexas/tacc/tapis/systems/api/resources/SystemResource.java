package edu.utexas.tacc.tapis.systems.api.resources;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
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
import javax.ws.rs.WebApplicationException;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
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
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostSystem;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostChildSystem;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPutSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystem;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystemHistory;
import edu.utexas.tacc.tapis.systems.api.responses.RespSystems;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.SystemHistoryItem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.UnlinkInfo;

import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;
import static edu.utexas.tacc.tapis.systems.model.TSystem.*;

/*
 * JAX-RS REST resource for a Tapis System (edu.utexas.tacc.tapis.systems.model.TSystem)
 *
 * These methods should do the minimal amount of validation and processing of incoming requests and
 *   then make the service method call.
 * One reason for this is the service methods are much easier to test.
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-systems, file SystemsAPI.yaml
 */
@Path("/v3/systems")
public class SystemResource {
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(SystemResource.class);

  private static final String SYSTEMS_SVC = StringUtils.capitalize(TapisConstants.SERVICE_NAME_SYSTEMS);

  // Json schema resource files.
  private static final String FILE_SYSTEM_CREATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPostRequest.json";
  private static final String FILE_SYSTEM_CREATE_CHILD_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/ChildSystemPostRequest.json";
  private static final String FILE_SYSTEM_PUT_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPutRequest.json";
  private static final String FILE_SYSTEM_UPDATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemPatchRequest.json";
  private static final String FILE_SYSTEM_CHILD_UPDATE_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/ChildSystemPatchRequest.json";
  private static final String FILE_SYSTEM_SEARCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/SystemSearchRequest.json";
  private static final String FILE_SYSTEM_MATCH_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/MatchConstraintsRequest.json";

  // Message keys
  private static final String INVALID_JSON_INPUT = "NET_INVALID_JSON_INPUT";
  private static final String JSON_VALIDATION_ERR = "TAPIS_JSON_VALIDATION_ERROR";
  private static final String UPDATE_ERR = "SYSAPI_UPDATE_ERROR";
  private static final String CREATE_ERR = "SYSAPI_CREATE_ERROR";
  private static final String SELECT_ERR = "SYSAPI_SELECT_ERROR";
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
  private static final String OP_UNLINK_FROM_PARENT = "unlinkFromParent";
  private static final String OP_UNLINK_CHILDREN = "unlinkChildren";
  private static final String OP_UNLINK_ALL_CHILDREN = "unlinkAllChildren";

  private static final Pair<ARGUMENT_TYPE, Object> NO_ADDITIONAL_ARGS = null;

  private enum ARGUMENT_TYPE {
    ARG_USER_NAME,
    ARG_CHILD_SYSTEMS,
    ARG_PARENT_SYS
  }

  // Always return a nicely formatted response
  public static final boolean PRETTY = true;

  // Top level summary attributes to be included by default in some cases.
  public static final List<String> SUMMARY_ATTRS =
          new ArrayList<>(List.of(ID_FIELD, SYSTEM_TYPE_FIELD, OWNER_FIELD, HOST_FIELD,
                  EFFECTIVE_USER_ID_FIELD, DEFAULT_AUTHN_METHOD_FIELD, CAN_EXEC_FIELD, PARENT_ID_FIELD));

  // Default for getSystem
  public static final List<String> DEFAULT_GETSYS_ATTRS = new ArrayList<>(List.of(SEL_ALL_ATTRS));

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
   * If effectiveUserId is static then credentials may be optionally provided in the authnCredential attribute of the
   * request body. If effective user is dynamic (i.e. ${apiUserId}) then credentials may not be provided.
   * The Systems service does not store the secrets, they are persisted in the Security Kernel.
   *
   * By default, any credentials provided for LINUX type systems are verified. Use query parameter
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
                               @Context SecurityContext securityContext) throws TapisClientException
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
      throw new BadRequestException(msg, e);
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_CREATE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }
    // ------------------------- Create a TSystem from the json -------------------------
    ReqPostSystem req;
    try { req = TapisGsonUtils.getGson().fromJson(rawJson, ReqPostSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }
    // If req is null that is an unrecoverable error
    if (req == null)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, "N/A", "ReqPostSystem == null");
      _log.error(msg);
      throw new BadRequestException(msg);
    }

    // Create a TSystem from the request
    TSystem tSystem = createTSystemFromPostRequest(rUser.getOboTenantId(), req, rawJson);
    boolean creatingCreds = (tSystem.getAuthnCredential() != null);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (creatingCreds) scrubbedJson = maskCredSecrets(rawJson);
    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_CREATE_TRACE", rUser, scrubbedJson));

    // ---------------------------- Make service call to create the system -------------------------------
    // Pull out system name for convenience
    String systemId = tSystem.getId();
    try
    {
      tSystem = service.createSystem(rUser, tSystem, skipCredCheck, scrubbedJson);
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
      else if (e.getMessage().contains("SYSLIB_CREATE_RESERVED"))
      {
        msg = ApiUtils.getMsgAuth("SYSAPI_CREATE_RESERVED", rUser, systemId);
        _log.warn(msg);
        return Response.status(Status.CONFLICT).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
      }
      else
      {
        // IllegalStateException indicates an Invalid TSystem was passed in
        msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
        _log.error(msg);
        throw new BadRequestException(msg, e);
      }
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // If credentials provided, and we are validating them, make sure they were OK.
    // If validation failed then system was not created, and we need to report an error.
    if (!skipCredCheck && creatingCreds)
    {
      // We only support registering credentials in the static effective user case, so in log messages report effUserId.
      String userName = tSystem.getEffectiveUserId();
      // Check validation result. Return UNAUTHORIZED (401) if not valid.
      resp = ApiUtils.checkCredValidationResult(rUser, systemId, userName, tSystem.getAuthnCredential(),
                                                tSystem.getDefaultAuthnMethod(), skipCredCheck);
      if (resp != null) return resp;
    }

    // ---------------------------- Success -------------------------------
    // Success means the object was created.
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString() + "/" + systemId;
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.CREATED, ApiUtils.getMsgAuth("SYSAPI_CREATED", rUser, systemId), resp1);
  }

  /**
   * Create a child system given a parent system id
   * @param payloadStream - request body
   * @param securityContext - user identity
   * @param systemId - parent system id
   * @return response containing reference to created object
   */
  @POST
  @Path("{systemId}/createChildSystem")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createChildSystem(InputStream payloadStream,
                               @PathParam("systemId") String systemId,
                               @Context SecurityContext securityContext) throws TapisClientException {
    String opName = "createChildSystem";

    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) {
      return resp;
    }

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled()) {
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
              "systemId="+systemId);
    }

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson;
    String msg;
    try {
      rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8);
    } catch (Exception e) {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_CREATE_CHILD_REQUEST);
    try {
      JsonValidator.validate(spec);
    } catch (TapisJSONException e) {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    ReqPostChildSystem childSystemRequest;
    try {
      childSystemRequest = TapisGsonUtils.getGson().fromJson(rawJson, ReqPostChildSystem.class);
    } catch (JsonSyntaxException e) {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    // ---------------------------- Make service call -------------------------------
    TSystem childSystem;
    try
    {
      childSystem = service.createChildSystem(rUser, systemId, childSystemRequest.id,
              childSystemRequest.effectiveUserId, childSystemRequest.rootDir,
              childSystemRequest.owner, childSystemRequest.enabled, rawJson);
    } catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) {
      // Pass through not found or not auth to let exception mapper handle it.
      throw e;
    } catch (Exception e) {
      // IllegalStateException indicates an Invalid TSystem was passed in
      msg = ApiUtils.getMsgAuth(CREATE_ERR, rUser, systemId, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }

    ResultResourceUrl respUrl = new ResultResourceUrl();
    URI uri = URI.create(_request.getRequestURL().append("/../../").append(childSystem.getId()).toString()).normalize();

    respUrl.url = uri.toString();
    return createSuccessResponse(Status.CREATED, ApiUtils.getMsgAuth("SYSAPI_CREATED", rUser, systemId),
            new RespResourceUrl(respUrl));
  }

  /**
   * Update specified attributes of a system
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
                              @Context SecurityContext securityContext) throws TapisClientException
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

    // load the correct json validation file depending on if this is a parent or child system
    String jsonValidationFile = isChildSystem(rUser, systemId) ?
            FILE_SYSTEM_CHILD_UPDATE_REQUEST : FILE_SYSTEM_UPDATE_REQUEST ;

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson;
    String msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, jsonValidationFile);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    // ------------------------- Create a PatchSystem from the json and validate constraints -------------------------
    PatchSystem patchSystem;
    try { patchSystem = TapisGsonUtils.getGson().fromJson(rawJson, PatchSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
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
    catch (IllegalStateException e)
    {
      // IllegalStateException indicates an Invalid PatchSystem was passed in
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    // Success means updates were applied
    ResultResourceUrl respUrl = new ResultResourceUrl();
    respUrl.url = _request.getRequestURL().toString();
    RespResourceUrl resp1 = new RespResourceUrl(respUrl);
    return createSuccessResponse(Status.OK, ApiUtils.getMsgAuth(UPDATED, rUser, systemId, opName), resp1);
  }

  /**
   * Update all updatable attributes of a system using a request body identical to POST. System must exist.
   *
   * Note that although the attribute authnCredential is allowed, it is ignored as discussed in the next paragraph.
   * Certain attributes in the request body (such as tenant) are allowed but ignored so that the JSON result returned
   *   by a GET may be modified and used when making a PUT request to update a system.
   *
   *   The attributes that are allowed but ignored for both PUT and POST are
   *      tenant
   *      uuid
   *      deleted
   *      created
   *      updated
   *  In addition for a PUT operation the following attributes are allowed but ignored
   *      id
   *      systemType
   *      owner
   *      enabled
   *      authnCredential
   *      bucketName
   *      rootDir
   *      canExec
   *  Note that the following attributes may be modified using other endpoints: owner, enabled, deleted, authnCredential
   *
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
                            @Context SecurityContext securityContext) throws TapisClientException
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
      throw new BadRequestException(msg, e);
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
      throw new BadRequestException(msg, e);
    }

    // ------------------------- Create a System from the json -------------------------
    ReqPutSystem req;
    try { req = TapisGsonUtils.getGson().fromJson(rawJson, ReqPutSystem.class); }
    catch (JsonSyntaxException e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }
    // If req is null that is an unrecoverable error
    if (req == null)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_UPDATE_ERROR", rUser, systemId, opName, "ReqPutSystem == null");
      _log.error(msg);
      throw new BadRequestException(msg);
    }

    // Create a TSystem from the request
    TSystem putSystem = createTSystemFromPutRequest(rUser.getOboTenantId(), systemId, req, rawJson);
    boolean creatingCreds = (putSystem.getAuthnCredential() != null);

    // Mask any secret info that might be contained in rawJson
    String scrubbedJson = rawJson;
    if (putSystem.getAuthnCredential() != null) scrubbedJson = maskCredSecrets(rawJson);
    if (_log.isTraceEnabled()) _log.trace(ApiUtils.getMsgAuth("SYSAPI_PUT_TRACE", rUser, scrubbedJson));

    // ---------------------------- Make service call to update the system -------------------------------
    try
    {
      putSystem = service.putSystem(rUser, putSystem, skipCredCheck, scrubbedJson);
    }
    catch (IllegalStateException e)
    {
      // IllegalStateException indicates an Invalid PutSystem was passed in
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // If credentials provided, and we are validating them, make sure they were OK.
    // If validation failed then system was not created, and we need to report an error.
    if (!skipCredCheck && creatingCreds)
    {
      // We only support registering credentials in the static effective user case, so in log messages report effUserId.
      String userName = putSystem.getEffectiveUserId();
      // Check validation result. Return UNAUTHORIZED (401) if not valid.
      resp = ApiUtils.checkCredValidationResult(rUser, systemId, userName, putSystem.getAuthnCredential(),
                                                putSystem.getDefaultAuthnMethod(), skipCredCheck);
      if (resp != null) return resp;
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
                               @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_ENABLE, systemId, NO_ADDITIONAL_ARGS, securityContext);
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
                                @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_DISABLE, systemId, NO_ADDITIONAL_ARGS, securityContext);
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
                               @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_DELETE, systemId, NO_ADDITIONAL_ARGS, securityContext);
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
                                 @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_UNDELETE, systemId, NO_ADDITIONAL_ARGS, securityContext);
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
                                    @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_CHANGEOWNER, systemId, new ImmutablePair<>(ARGUMENT_TYPE.ARG_USER_NAME, userName), securityContext);
  }

  /**
   * Unlink a child system from a parent. This makes the child system a standalone system.
   * unlinkChild and unlinkFromParent are identical in that they each make a childSystem become a standalone
   * system. The difference is in the authorization.
   * unlinkFromParent requires access to the child.
   * unlinkChild requires access to the parent.
   *
   * @param childSystemId - id of the child system to unlink
   * @param securityContext - user identity
   * @return  number of records modified as a result of the action
   */
  @POST
  @Path("{childSystemId}/unlinkFromParent")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unlinkFromParent(@PathParam("childSystemId") String childSystemId,
                                   @Context SecurityContext securityContext) throws TapisClientException
  {
    return postSystemSingleUpdate(OP_UNLINK_FROM_PARENT, childSystemId, NO_ADDITIONAL_ARGS, securityContext);
  }

  /**
   * Unlink a child system from a parent. This makes the child system a standalone system.
   * unlinkChild and unlinkFromParent are identical in that they each make a childSystem become a standalone
   * system. The difference is in the authorization.
   * unlinkFromParent requires access to the child.
   * unlinkChild requires access to the parent.
   *
   * @param parentSystemId - id of the parent of the system to unlink
   * @param unlinkInfo - object containing the ids of the child systems to unlink
   * @param securityContext - user identity
   * @return  number of records modified as a result of the action
   */
  @POST
  @Path("{parentSystemId}/unlinkChildren")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response unlinkChild(@PathParam("parentSystemId") String parentSystemId,
                              @QueryParam("all") @DefaultValue("false") boolean unlinkAll,
                              UnlinkInfo unlinkInfo,
                              @Context SecurityContext securityContext) throws TapisClientException
  {
    if(unlinkAll) {
      return postSystemSingleUpdate(OP_UNLINK_ALL_CHILDREN, parentSystemId, NO_ADDITIONAL_ARGS, securityContext);
    } else {
      return postSystemSingleUpdate(OP_UNLINK_CHILDREN, parentSystemId, new ImmutablePair<>(ARGUMENT_TYPE.ARG_CHILD_SYSTEMS, unlinkInfo.childSystemIds), securityContext);
    }
  }

  /**
   * getSystem
   * @param systemId - name of the system
   * @param authnMethodStr - authn method to use instead of default
   * @param requireExecPerm - check for EXECUTE permission as well as READ permission
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth and
   *                          resolving effectiveUserId
   * @param sharedAppCtx - Share grantor for the case of a shared application context.
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
                            @QueryParam("impersonationId") String impersonationId,
                            @QueryParam("sharedAppCtx") String sharedAppCtx,
                            @QueryParam("resourceTenant") String resourceTenant,
                            @Context SecurityContext securityContext) throws TapisClientException
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
                                                   "impersonationId="+impersonationId,
                                                   "resourceTenant="+resourceTenant,
                                                   "sharedAppCtx="+sharedAppCtx);

    // Check that authnMethodStr is valid if is passed in
    AuthnMethod authnMethod = null;
    try { if (!StringUtils.isBlank(authnMethodStr)) authnMethod =  AuthnMethod.valueOf(authnMethodStr); }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_ACCMETHOD_ENUM_ERROR", rUser, systemId, authnMethodStr, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    List<String> selectList = threadContext.getSearchParameters().getSelectList();
    if (selectList == null || selectList.isEmpty()) selectList = DEFAULT_GETSYS_ATTRS;

    // Determine if select contains shareInfo
    boolean fetchShareInfo = isShareInfoRequested(selectList);

    // ---------------------------- Make service call -------------------------------
    TSystem tSystem;
    try
    {
      tSystem = service.getSystem(rUser, systemId, authnMethod, requireExecPerm, getCreds, impersonationId,
                                  sharedAppCtx, resourceTenant, fetchShareInfo);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }
    // Resource was not found.
    if (tSystem == null) throw new NotFoundException(ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the system information.
    RespSystem resp1 = new RespSystem(tSystem, selectList);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "System", systemId), resp1);
  }

  /**
   * getSystems
   * Retrieve all systems accessible by requester and matching any search conditions provided.
   * NOTE: The query parameters search, limit, orderBy, skip, startAfter are all handled in the filter
   *       QueryParametersRequestFilter. No need to use @QueryParam here.
   * @param securityContext - user identity
   * @param showDeleted - flag indicating resources marked as deleted should be included.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth and
   *                          resolving effectiveUserId
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSystems(@Context SecurityContext securityContext,
                             @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted,
                             @QueryParam("listType") @DefaultValue("OWNED") String listType,
                             @QueryParam("impersonationId") String impersonationId) throws TapisClientException
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
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                                   "listType="+listType,
                                                   "impersonationId="+impersonationId);

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, null, srchParms, showDeleted, listType, impersonationId);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }
    return successResponse;
  }

  /**
   * searchSystemsQueryParameters
   * Dedicated search endpoint for System resource. Search conditions provided as query parameters.
   * @param securityContext - user identity
   * @param showDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @GET
  @Path("search")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsQueryParameters(@Context SecurityContext securityContext,
                                               @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted,
                                               @QueryParam("listType") @DefaultValue("OWNED") String listType)
          throws TapisClientException
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
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                                   "listType="+listType);

    // Create search list based on query parameters
    // Note that some validation is done for each condition but the back end will handle translating LIKE wildcard
    //   characters (* and !) and deal with escaped characters.
    List<String> searchList;
    try
    {
      searchList = SearchUtils.buildListFromQueryParms(_uriInfo.getQueryParameters());
    }
    catch (IllegalArgumentException e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SEARCH_ERROR", rUser, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();
    srchParms.setSearchList(searchList);

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, null, srchParms, showDeleted, listType, null);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
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
   * @param showDeleted - whether to included resources that have been marked as deleted.
   * @param listType - allows for filtering results based on authorization: OWNED, SHARED_PUBLIC, ALL
   * @return - list of systems accessible by requester and matching search conditions.
   */
  @POST
  @Path("search")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response searchSystemsRequestBody(InputStream payloadStream,
                                           @Context SecurityContext securityContext,
                                           @QueryParam("showDeleted") @DefaultValue("false") boolean showDeleted,
                                           @QueryParam("listType") @DefaultValue("OWNED") String listType)
          throws TapisClientException
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
    if (_log.isTraceEnabled()) ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                                                   "listType="+listType);

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String rawJson;
    String msg;
    try { rawJson = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_SEARCH_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
      _log.error(msg, e);
      throw new BadRequestException(msg, e);
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
      throw new BadRequestException(msg, e);
    }

    // ThreadContext designed to never return null for SearchParameters
    SearchParameters srchParms = threadContext.getSearchParameters();

    // ------------------------- Retrieve records -----------------------------
    Response successResponse;
    try
    {
      successResponse = getSearchResponse(rUser, sqlSearchStr, srchParms, showDeleted, listType, null);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
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
//                                   @Context SecurityContext securityContext) throws TapisClientException
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
//      throw new BadRequestException(msg, e);
//    }
//    // Create validator specification and validate the json against the schema
//    JsonValidatorSpec spec = new JsonValidatorSpec(rawJson, FILE_SYSTEM_MATCH_REQUEST);
//    try { JsonValidator.validate(spec); }
//    catch (TapisJSONException e)
//    {
//      msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
//      _log.error(msg, e);
//      throw new BadRequestException(msg, e);
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
//      throw new BadRequestException(msg, e);
//    }
//
//    // ------------------------- Retrieve records -----------------------------
//    List<TSystem> systems;
//    try {
//      systems = systemsService.getSystemsSatisfyingConstraints(rUser, matchStr);
//    }
//  // Pass through not found or not auth to let exception mapper handle it.
//    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
//  // As final fallback
//    catch (Exception e)
//    {
//      msg = ApiUtils.getMsgAuth(SELECT_ERR, rUser, e.getMessage());
//      _log.error(msg, e);
//      throw new WebApplicationException(msg);
//    }
//
//    if (systems == null) systems = Collections.emptyList();
//
//    // ---------------------------- Success -------------------------------
//    RespSystems resp1 = new RespSystems(systems);
//    String itemCountStr = String.format(SYS_CNT_STR, systems.size());
//    return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, SYSTEMS_SVC, itemCountStr), resp1);
//  }

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
                             @Context SecurityContext securityContext) throws TapisClientException
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
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // System or history not found
    if (systemHistory == null || systemHistory.size()==0)
      throw new NotFoundException(ApiUtils.getMsgAuth(NOT_FOUND, rUser, systemId));

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
                            @Context SecurityContext securityContext) throws TapisClientException
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
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }

    // ---------------------------- Success -------------------------------
    // Success means we made the check
    ResultBoolean respResult = new ResultBoolean();
    respResult.aBool = isEnabled;
    RespBoolean resp1 = new RespBoolean(respResult);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg("TAPIS_FOUND", "System", systemId), resp1);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * changeOwner, enable, disable, delete and undelete follow same pattern
   * Note that userName only used for changeOwner
   * @param opName Name of operation.
   * @param systemId Id of system to update
   * @param additionalArg pair representing the name and value of one additional argument.
   *                      new owner name for op changeOwner
   *                      parent system id for unlinkChild
   * @param securityContext Security context from client call
   * @return Response to be returned to the client.
   */
  private Response postSystemSingleUpdate(String opName, String systemId, Pair<ARGUMENT_TYPE, Object> additionalArg,
                                          SecurityContext securityContext)
          throws TapisClientException
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
      if (additionalArg!=null)
        ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, additionalArg.getLeft() + "=" + additionalArg.getRight());
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
      else if (OP_UNLINK_FROM_PARENT.equals(opName))
        changeCount = service.unlinkFromParent(rUser, systemId);
      else if (OP_UNLINK_CHILDREN.equals(opName))
        changeCount = service.unlinkChildren(rUser, systemId, (List<String>)additionalArg.getRight());
      else if (OP_UNLINK_ALL_CHILDREN.equals(opName))
        changeCount = service.unlinkAllChildren(rUser, systemId);
      else {
        String userName = ARGUMENT_TYPE.ARG_USER_NAME.equals(additionalArg.getLeft()) ? (String) additionalArg.getRight() : null;
        changeCount = service.changeSystemOwner(rUser, systemId, userName);
      }
    }
    catch (IllegalStateException e)
    {
      // IllegalStateException indicates an Invalid PatchSystem was passed in
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    catch (IllegalArgumentException e)
    {
      // IllegalArgumentException indicates somehow a bad argument made it this far
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg);
      throw new BadRequestException(msg, e);
    }
    // Pass through not found or not auth to let exception mapper handle it.
    catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) { throw e; }
    // As final fallback
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth(UPDATE_ERR, rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
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
                       req.dtnSystemId,
                       req.canExec, req.jobRuntimes, req.jobWorkingDir, req.jobEnvVariables, req.jobMaxJobs,
                       req.jobMaxJobsPerUser, req.canRunBatch, req.enableCmdPrefix, req.mpiCmd, req.batchScheduler,
                       req.batchLogicalQueues, req.batchDefaultLogicalQueue, req.batchSchedulerProfile, req.jobCapabilities,
                       req.tags, notes, req.importRefId, null, false,
                       req.allowChildren, null, null, null);
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
    TSystem.SystemType systemTypeNull = null;
    String ownerNull = null;
    boolean enabledTrue = true;
    String bucketNameNull = null;
    String rootDirNull = null;
    String effectiveUserIdNull = null;
    boolean canExecTrue = true;
    var tSystem = new TSystem(-1, tenantId, systemId, req.description, systemTypeNull, ownerNull, req.host,
            enabledTrue, effectiveUserIdNull, req.defaultAuthnMethod, bucketNameNull, rootDirNull,
            req.port, req.useProxy, req.proxyHost, req.proxyPort,
            req.dtnSystemId,
            canExecTrue, req.jobRuntimes, req.jobWorkingDir, req.jobEnvVariables, req.jobMaxJobs, req.jobMaxJobsPerUser,
            req.canRunBatch, req.enableCmdPrefix, req.mpiCmd, req.batchScheduler, req.batchLogicalQueues, req.batchDefaultLogicalQueue,
            req.batchSchedulerProfile, req.jobCapabilities, req.tags, notes, req.importRefId, null, false,
            req.allowChildren, req.parentId, null, null);
    tSystem.setAuthnCredential(req.authnCredential);
    return tSystem;
  }

  /*
   * Extract notes from the incoming json
   * This explicit method to extract is needed because notes is an unstructured object and other seemingly simpler
   * approaches caused problems with the json marshalling. This method ensures notes end up as a JsonObject rather
   * than a LinkedTreeMap.
   */
  private static JsonObject extractNotes(String rawJson)
  {
    JsonObject notes = null;
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
    maskSecret(credObj, CredentialResource.ACCESS_TOKEN_FIELD);
    maskSecret(credObj, CredentialResource.REFRESH_TOKEN_FIELD);
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
  private Response getSearchResponse(ResourceRequestUser rUser, String sqlSearchStr, SearchParameters srchParms,
                                     boolean showDeleted, String listType, String impersonationId)
          throws TapisException, TapisClientException
  {
    RespAbstract resp1;
    List<TSystem> systems;
    int totalCount = -1;
    String itemCountStr;

    List<String> searchList = srchParms.getSearchList();
    List<String> selectList = srchParms.getSelectList();
    if (selectList == null || selectList.isEmpty()) selectList = SUMMARY_ATTRS;

    // If limit or skip not specified then use defaults
    int limit = (srchParms.getLimit() == null) ? SearchParameters.DEFAULT_LIMIT : srchParms.getLimit();
    int skip = (srchParms.getSkip() == null) ? SearchParameters.DEFAULT_SKIP : srchParms.getSkip();
    // Set some variables to make code easier to read
    String startAfter = srchParms.getStartAfter();
    boolean computeTotal = srchParms.getComputeTotal();
    String orderBy = srchParms.getOrderBy();
    List<OrderBy> orderByList = srchParms.getOrderByList();

    // Determine if select contains shareInfo
    boolean fetchShareInfo = isShareInfoRequested(selectList);

    // Call service method to fetch systems
    if (StringUtils.isBlank(sqlSearchStr))
      systems = service.getSystems(rUser, searchList, limit, orderByList, skip, startAfter, showDeleted,
                                   listType, fetchShareInfo, impersonationId);
    else
      systems = service.getSystemsUsingSqlSearchStr(rUser, sqlSearchStr, limit, orderByList, skip, startAfter,
                                                    showDeleted, listType, fetchShareInfo);
    if (systems == null) systems = Collections.emptyList();
    itemCountStr = String.format(SYS_CNT_STR, systems.size());
    if (computeTotal && limit <= 0) totalCount = systems.size();

    // If we need the count and there was a limit then we need to make a call
    // This is a separate call from getSystems() because unlike getSystems() we do not want to include the limit or skip,
    //   and we do not need to fetch all the data. One benefit is that the method is simpler and easier to follow
    //   compared to attempting to fold everything into getSystems().
    if (computeTotal && limit > 0)
    {
      totalCount = service.getSystemsTotalCount(rUser, searchList, orderByList, startAfter, showDeleted,
                                                listType, impersonationId);
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

  /*
   * Determine if a system is a child of some parent system.
   */
  private boolean isChildSystem(ResourceRequestUser rUser, String systemId) throws TapisClientException {
    try {
      return !StringUtils.isBlank(service.getParentId(rUser, systemId));
    } catch (NotFoundException | NotAuthorizedException | ForbiddenException | TapisClientException e) {
      // Pass through not found or not auth so let exception mapper handle it.
      throw e;
    } catch (Exception e) {
      // As final fallback
      String msg = ApiUtils.getMsgAuth("SYSAPI_SYS_GET_ERROR", rUser, systemId, e.getMessage());
      _log.error(msg, e);
      throw new WebApplicationException(msg);
    }
  }

  /*
   * Determine if selectList will trigger need to fetch shareInfo
   */
  private static boolean isShareInfoRequested(List<String> selectList)
  {
    if (selectList == null || selectList.isEmpty()) selectList = Collections.emptyList();
    return (selectList.contains(IS_PUBLIC_FIELD) ||
            selectList.contains(SHARED_WITH_USERS_FIELD) ||
            selectList.contains(SEL_ALL_ATTRS));
  }
}
