package edu.utexas.tacc.tapis.systems.api.utils;

import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import javax.ws.rs.core.Response;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import static edu.utexas.tacc.tapis.systems.api.resources.SystemResource.PRETTY;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class ApiUtils
{
  // Private constructor to make it non-instantiable
  private ApiUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(ApiUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.systems.api.SysApiMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsgAuth(String key, ResourceRequestUser rUser, Object... parms)
  {
    // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
    var newParms = new Object[4 + parms.length];
    newParms[0] = rUser.getJwtTenantId();
    newParms[1] = rUser.getJwtUserId();
    newParms[2] = rUser.getOboTenantId();
    newParms[3] = rUser.getOboUserId();
    System.arraycopy(parms, 0, newParms, 4, parms.length);
    return getMsg(key, newParms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale - Locale to use when building message. If null use default locale
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsg(String key, Locale locale, Object... parms)
  {
    String msgValue = null;

    if (locale == null) locale = Locale.getDefault();

    ResourceBundle bundle = null;
    try { bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE, locale); }
    catch (Exception e)
    {
      _log.error("Unable to find resource message bundle: " + MESSAGE_BUNDLE, e);
    }
    if (bundle != null) try { msgValue = bundle.getString(key); }
    catch (Exception e)
    {
      _log.error("Unable to find key: " + key + " in resource message bundle: " + MESSAGE_BUNDLE, e);
    }

    if (msgValue != null)
    {
      // No problems. If needed fill in any placeholders in the message.
      if (parms != null && parms.length > 0) msgValue = MessageFormat.format(msgValue, parms);
    }
    else
    {
      // There was a problem. Build a message with as much info as we can give.
      StringBuilder sb = new StringBuilder("Key: ").append(key).append(" not found in bundle: ").append(MESSAGE_BUNDLE);
      if (parms != null && parms.length > 0)
      {
        sb.append("Parameters:[");
        for (Object parm : parms) {sb.append(parm.toString()).append(",");}
        sb.append("]");
      }
      msgValue = sb.toString();
    }
    return msgValue;
  }

  /**
   * Return single json element as a string
   * @param jelem Json element
   * @param defaultVal string value to use as a default if element is null
   * @return json element as string
   */
  public static String getValS(JsonElement jelem, String defaultVal)
  {
    if (jelem == null) return defaultVal;
    else return jelem.getAsString();
  }

  /**
   * Validate call checks for tenantId, user and accountType
   * If all OK return null, else return error response.
   * @param threadContext thread context to check
   * @return null if all OK else error response
   */
  public static Response checkContext(TapisThreadContext threadContext, boolean prettyPrint)
  {
    if (threadContext.validate()) return null;
    String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
    _log.error(msg);
    return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
  }

  /**
   * Check that system exists
   * @param rUser - principal user containing tenant and user info
   * @param systemId - name of the system to check
   * @param prettyPrint - print flag used to construct response
   * @param opName - operation name, for constructing response msg
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSystemExists(SystemsService systemsService, ResourceRequestUser rUser,
                                           String systemId, boolean prettyPrint, String opName)
  {
    String msg;
    boolean systemExists;
    try { systemExists = systemsService.checkForSystem(rUser, systemId); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CHECK_ERROR", rUser, systemId, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!systemExists)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOSYSTEM", rUser, systemId, opName);
      _log.error(msg);
      return Response.status(Response.Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }

  /**
   * Check that both or neither of the secrets are blank.
   * This is for PKI_KEYS and ACCESS_KEY where if one part of the secret is supplied the other must also be supplied
   * @param systemName - name of the system, for constructing response msg
   * @param userName - name of user associated with the secrets request, for constructing response msg
   * @param prettyPrint - print flag used to construct response
   * @param secretType - secret type (PKI_KEYS, API_KEY), for constructing response msg
   * @param secretName1 - secret name, for constructing response msg
   * @param secretName2 - secret name, for constructing response msg
   * @param secretVal1 - first secret
   * @param secretVal2 - second secret
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSecrets(ResourceRequestUser rUser, String systemName, String userName, boolean prettyPrint,
                                      String secretType, String secretName1, String secretName2, String secretVal1, String secretVal2)
  {
    if ((!StringUtils.isBlank(secretVal1) && StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_SECRET_MISSING", rUser, systemName, secretType, secretName2, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if ((StringUtils.isBlank(secretVal1) && !StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_SECRET_MISSING", rUser, systemName, secretType, secretName1, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }

  /**
   * Trace the incoming request, include info about requesting user, op name and request URL
   * @param rUser resource user
   * @param opName name of operation
   */
  public static void logRequest(ResourceRequestUser rUser, String className, String opName, String reqUrl, String... strParms)
  {
    // Build list of args passed in
    String argListStr = "";
    if (strParms != null && strParms.length > 0) argListStr = String.join(",", strParms);
    String msg = ApiUtils.getMsgAuth("SYSAPI_TRACE_REQUEST", rUser, className, opName, reqUrl, argListStr);
    _log.trace(msg);
  }

  /**
   * Construct message containing list of errors
   */
  public static String getListOfErrors(List<String> msgList, ResourceRequestUser rUser, Object... parms) {
    if (msgList == null || msgList.isEmpty()) return "";
    var sb = new StringBuilder(ApiUtils.getMsgAuth("SYSAPI_CREATE_INVALID_ERRORLIST", rUser, parms));
    sb.append(System.lineSeparator());
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  /*
   * Check that credentials are valid.
   * If valid return null. If invalid build a bad request response and return it.
   * Used by CredentialResource create and check endpoints as well as SystemResource create and put endpoints.
   */
  public static Response checkCredValidationResult(ResourceRequestUser rUser, String systemId, String userName, Credential cred,
                                                   AuthnMethod authnMethod, boolean skipCredCheck)
  {
    // If skipping we are done
    if (skipCredCheck) return null;

    // If we are not given an authnMethod use the one in the credential if it is present.
    // Used only for logging.
    if (authnMethod == null && cred != null && cred.getAuthnMethod() != null) authnMethod = cred.getAuthnMethod();

    // Call that validated credentials should never return null credential or null validationResult
    if (cred == null || cred.getValidationResult() == null)
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_CHECK_ERROR", rUser, systemId, userName, authnMethod, "Invalid null return");
      _log.error(msg);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // If credential validation failed return UNAUTHORIZED = 401
    if (Boolean.FALSE.equals(cred.getValidationResult()))
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_VALID_FAIL", rUser, systemId, userName, authnMethod, cred.getValidationMsg());
      return Response.status(Response.Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    return null;
  }

// NOTE: If this is ever used it will strip off the description
//  /**
//   * Return String[] array of jobEnvVariables given list of KeyValuePair
//   */
//  public static String[] getKeyValuesAsArray(List<KeyValuePair> kvList)
//  {
//    if (kvList == null) return null;
//    if (kvList.size() == 0) return TSystem.EMPTY_STR_ARRAY;
//    return kvList.stream().map(KeyValuePair::toString).toArray(String[]::new);
//  }
//
//  /**
//   * Return list of KeyValuePair given String[] array of jobEnvVariables given
//   */
//  public static List<KeyValuePair> getKeyValuesAsList(String[] kvArray)
//  {
//    if (kvArray == null || kvArray.length == 0) return Collections.emptyList();
//    List<KeyValuePair> kvList = Arrays.stream(kvArray).map(KeyValuePair::fromString).collect(Collectors.toList());
//    return kvList;
//  }
}
