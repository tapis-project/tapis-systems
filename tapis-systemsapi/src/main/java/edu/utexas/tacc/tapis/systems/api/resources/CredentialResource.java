package edu.utexas.tacc.tapis.systems.api.resources;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.api.requests.ReqPostCredential;
import edu.utexas.tacc.tapis.systems.api.responses.RespCredential;
import edu.utexas.tacc.tapis.systems.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.service.SystemsService;

/*
 * JAX-RS REST resource for Tapis System credentials
 * NOTE: Annotations for generating OpenAPI specification not currently used.
 *       Please see tapis-systemsapi/src/main/resources/SystemsAPI.yaml
 *       and note at top of SystemsResource.java
 * Annotations map HTTP verb + endpoint to method invocation.
 * Secrets are stored in the Security Kernel
 *
 */
@Path("/v3/systems/credential")
public class CredentialResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(CredentialResource.class);

  // Json schema resource files.
  private static final String FILE_CRED_REQUEST = "/edu/utexas/tacc/tapis/systems/api/jsonschema/CredentialCreateRequest.json";

  // Field names used in Json
  public static final String PASSWORD_FIELD = "password";
  public static final String PRIVATE_KEY_FIELD = "privateKey";
  public static final String PUBLIC_KEY_FIELD = "publicKey";
  public static final String ACCESS_KEY_FIELD = "accessKey";
  public static final String ACCESS_SECRET_FIELD = "accessSecret";
  public static final String CERTIFICATE_FIELD = "certificate";

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
   * Store or update credentials for given system and userName.
   * The Systems service does not store the secrets, they are persisted in the Security Kernel.
   * The secrets are stored in the Security Kernel under userName.
   * For a System with a dynamic effectiveUserId (i.e. equal to $apiUserId):
   *   In addition to secrets the request body may contain a login user.
   *   If the login user is not provided then it defaults to requesting Tapis user.
   *   If loginUser != tapisUser then a mapping between the Tapis userName and the login user is recorded.
   *
   * @param systemId - System associated with the credentials
   * @param userName - User associated with the credentials
   * @param payloadStream - request body
   * @return basic response
   */
  @POST
  @Path("/{systemId}/user/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createUserCredential(@PathParam("systemId") String systemId,
                                       @PathParam("userName") String userName,
                                       @QueryParam("skipCredentialCheck") @DefaultValue("false") boolean skipCredCheck,
                                       InputStream payloadStream,
                                       @Context SecurityContext securityContext)
  {
    String opName = "createUserCredential";
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
                          "userName="+userName,"skipCredentialCheck="+skipCredCheck);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "createUserCredential");
    if (resp != null) return resp;

    // ------------------------- Extract and validate payload -------------------------
    // Read the payload into a string.
    String json;
    String msg;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_JSON_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_CRED_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_JSON_INVALID", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Populate credential from payload
    ReqPostCredential req = TapisGsonUtils.getGson().fromJson(json, ReqPostCredential.class);
    // If no loginUser provided default to userName
    String loginUser = (StringUtils.isBlank(req.loginUser)) ? userName : req.loginUser;
    // We do not care about authnMethod here so set to null
    AuthnMethod authnMethod = null;
    Credential credential = new Credential(authnMethod, loginUser, req.password, req.privateKey,
                                           req.publicKey, req.accessKey, req.accessSecret, req.certificate);

    // If one of PKI keys is missing then reject
    resp = ApiUtils.checkSecrets(rUser, systemId, userName, PRETTY, AuthnMethod.PKI_KEYS.name(), PRIVATE_KEY_FIELD, PUBLIC_KEY_FIELD,
                                 credential.getPrivateKey(), credential.getPublicKey());
    if (resp != null) return resp;
    // If one of Access key or Access secret is missing then reject
    resp = ApiUtils.checkSecrets(rUser, systemId, userName, PRETTY, AuthnMethod.ACCESS_KEY.name(), ACCESS_KEY_FIELD, ACCESS_SECRET_FIELD,
                                 credential.getAccessKey(), credential.getAccessSecret());
    if (resp != null) return resp;

    // If PKI private key is not compatible with Tapis then reject
    if (!StringUtils.isBlank(credential.getPrivateKey()) && !credential.isValidPrivateSshKey())
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_INVALID_PRIVATE_SSHKEY", rUser, systemId, userName);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Create json with secrets masked out. This is recorded by the service as part of the update record.
    Credential maskedCredential = Credential.createMaskedCredential(credential);
    String scrubbedJson = TapisGsonUtils.getGson().toJson(maskedCredential);

    // ------------------------- Perform the operation -------------------------
    // Make the service call to create or update the credential
    try
    {
      service.createUserCredential(rUser, systemId, userName, credential, skipCredCheck, scrubbedJson);
    }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsgAuth("SYSAPI_CRED_UPDATED", rUser, systemId, userName),
                                                   PRETTY, resp1))
      .build();
  }

  /**
   * getUserCredential
   * @param authnMethodStr - authn method to use instead of default
   * @return Response
   */
  @GET
  @Path("/{systemId}/user/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUserCredential(@PathParam("systemId") String systemId,
                                    @PathParam("userName") String userName,
                                    @QueryParam("authnMethod") @DefaultValue("") String authnMethodStr,
                                    @Context SecurityContext securityContext)
  {
    String opName = "getUserCredential";
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    // Check that we have all we need from the context
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (_log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
                          "userName="+userName,"authnMethod="+authnMethodStr);

    // ------------------------- Check prerequisites -------------------------
    // Check that the system exists
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "getUserCredential");
    if (resp != null) return resp;

    // Check that authnMethodStr is valid if it is passed in
    AuthnMethod authnMethod = null;
    String msg;
    try { if (!StringUtils.isBlank(authnMethodStr)) authnMethod =  AuthnMethod.valueOf(authnMethodStr); }
    catch (IllegalArgumentException e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_ACCMETHOD_ENUM_ERROR", rUser, systemId, authnMethodStr, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Perform the operation -------------------------
    // Make the service call to get the credentials
    Credential credential;
    try { credential = service.getUserCredential(rUser, systemId, userName, authnMethod); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(TapisRestUtils.getStatus(e)).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // Resource was not found.
    if (credential == null)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CRED_NOT_FOUND", rUser, systemId, userName);
      _log.warn(msg);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the information.
    RespCredential resp1 = new RespCredential(credential);
    return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(
            ApiUtils.getMsgAuth("SYSAPI_CRED_FOUND", rUser, systemId, userName), PRETTY, resp1)).build();
  }

  /**
   * Remove credential for given system and user.
   * @return basic response
   */
  @DELETE
  @Path("/{systemId}/user/{userName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response removeUserCredential(@PathParam("systemId") String systemId,
                                       @PathParam("userName") String userName,
                                       @Context SecurityContext securityContext)
  {
    String opName = "removeUserCredential";
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
    resp = ApiUtils.checkSystemExists(service, rUser, systemId, PRETTY, "removeUserCredential");
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Make the service call to remove the credential
    try
    {
      service.deleteUserCredential(rUser, systemId, userName);
    }
    catch (Exception e)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_ERROR", rUser, systemId, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(ApiUtils.getMsgAuth("SYSAPI_CRED_DELETED", rUser, systemId,
                                                                       userName), PRETTY, resp1))
      .build();
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

}
