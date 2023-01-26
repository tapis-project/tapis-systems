package edu.utexas.tacc.tapis.systems.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ResourceBundle;
import java.util.Set;

import com.google.gson.JsonObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.PatchSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_DEFAULT_LOGICAL_QUEUE_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_LOGICAL_QUEUES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_SCHEDULER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BATCH_SCHEDULER_PROFILE_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.BUCKET_NAME_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.CAN_EXEC_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.CAN_RUN_BATCH_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_AUTHN_METHOD_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DELETED_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DESCRIPTION_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_MOUNT_POINT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_MOUNT_SOURCE_PATH_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DTN_SYSTEM_ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.EFFECTIVE_USER_ID_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ENABLED_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ENABLE_CMD_PREFIX_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.HOST_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.IS_DTN_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_CAPABILITIES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_ENV_VARIABLES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_MAX_JOBS_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_MAX_JOBS_PER_USER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_RUNTIMES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.JOB_WORKING_DIR_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.MPI_CMD_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.NOTES_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.OWNER_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PORT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PROXY_HOST_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.PROXY_PORT_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.ROOT_DIR_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.SYSTEM_TYPE_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.TAGS_FIELD;
import static edu.utexas.tacc.tapis.systems.model.TSystem.USE_PROXY_FIELD;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class LibUtils
{
  // Private constructor to make it non-instantiable
  private LibUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(LibUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.systems.SysLibMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
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
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale Locale for message
   * @param key message key
   * @param parms message parameters
   * @return localized message
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

//  /**
//   * Return List of transfer methods as a comma delimited list of strings surrounded by curly braces.
//   */
//  public static String getTransferMethodsAsString(List<TransferMethod> txfrMethods)
//  {
//    if (txfrMethods == null || txfrMethods.isEmpty()) return TSystem.EMPTY_TRANSFER_METHODS_STR;
//    StringBuilder sb = new StringBuilder("{");
//    for (int i = 0; i < txfrMethods.size()-1; i++)
//    {
//      sb.append(txfrMethods.get(i).name()).append(",");
//    }
//    sb.append(txfrMethods.get(txfrMethods.size()-1).name());
//    sb.append("}");
//    return sb.toString();
//  }
//
//  /**
//   * Return String[] array of transfer methods
//   */
//  public static String[] getTransferMethodsAsStringArray(List<TransferMethod> txfrMethods)
//  {
//    if (txfrMethods == null || txfrMethods.size() == 0) return TSystem.EMPTY_STR_ARRAY;
//    return txfrMethods.stream().map(TransferMethod::name).toArray(String[]::new);
//  }
//
//  /**
//   * Return TransferMethod[] from String[]
//   */
//  public static List<TransferMethod> getTransferMethodsFromStringArray(String[] txfrMethods)
//  {
//    if (txfrMethods == null || txfrMethods.length == 0) return Collections.emptyList();
//    return Arrays.stream(txfrMethods).map(TransferMethod::valueOf).collect(Collectors.toList());
//  }

  /**
   * Log a TAPIS_NULL_PARAMETER exception and throw a TapisException
   */
  public static void logAndThrowNullParmException(String opName, String parmName) throws TapisException
  {
    String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", opName, parmName);
    _log.error(msg);
    throw new TapisException(msg);
  }

  // =============== DB Transaction Management ============================
  /**
   * Close any DB connection related artifacts that are not null
   * @throws SQLException - on sql error
   */
  public static void closeAndCommitDB(Connection conn, PreparedStatement pstmt, ResultSet rs) throws SQLException
  {
    if (rs != null) rs.close();
    if (pstmt != null) pstmt.close();
    if (conn != null) conn.commit();
  }

  /**
   * Roll back a DB transaction and throw an exception
   * This method always throws an exception, either IllegalStateException or TapisException
   */
  public static void rollbackDB(Connection conn, Exception e, String msgKey, Object... parms) throws TapisException
  {
    try
    {
      if (conn != null) conn.rollback();
    }
    catch (Exception e1)
    {
      _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
    }

    // If IllegalStateException or TapisException pass it back up
    if (e instanceof IllegalStateException) throw (IllegalStateException) e;
    if (e instanceof TapisException) throw (TapisException) e;

    // Log the exception.
    String msg = MsgUtils.getMsg(msgKey, parms);
    _log.error(msg, e);
    throw new TapisException(msg, e);
  }

  /**
   * Close DB connection, typically called from finally block
   */
  public static void finalCloseDB(Connection conn)
  {
    // Always return the connection back to the connection pool.
    try
    {
      if (conn != null) conn.close();
    }
    catch (Exception e)
    {
      // If commit worked, we can swallow the exception.
      // If not, the commit exception will have been thrown.
      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
      _log.error(msg, e);
    }
  }

  /**
   * Compare original and modified systems to detect changes and produce a complete and succinct description
   *   of the changes. If no changes then return null.
   * NOTE that although some attributes should never change in this code path we include them here in case there is
   *   a bug or the design changes and this code path does include them.
   * Attributes that should not change: systemType, isEnabled, owner, bucketName, rootDir, isDtn, canExec, isDeleted
   *
   * @param o - original TSystem
   * @param n - new TSystem
   * @param p - incoming PatchSystem if this was a PATCH operation
   * @return Description of the changes or null if no changes detected.
   */
  public static String getChangeDescriptionSystemUpdate(TSystem o, TSystem n, PatchSystem p)
  {
    boolean noChanges = true;
    boolean notPatch = (p == null);
    var jo = new JSONObject();
    if (!Objects.equals(o.getDescription(), n.getDescription()))
      {noChanges=false;addChange(jo, DESCRIPTION_FIELD, o.getDescription(), n.getDescription());}
    if (!Objects.equals(o.getSystemType(),n.getSystemType()))
      {noChanges=false;addChange(jo, SYSTEM_TYPE_FIELD, o.getSystemType().name(), n.getSystemType().name());}
    if (!Objects.equals(o.getOwner(),n.getOwner()))
      {noChanges=false;addChange(jo, OWNER_FIELD, o.getOwner(), n.getOwner());}
    if (!Objects.equals(o.getHost(),n.getHost()))
      {noChanges=false;addChange(jo, HOST_FIELD, o.getHost(), n.getHost());}
    if (!(o.isEnabled() == n.isEnabled()))
      {noChanges=false;addChange(jo, ENABLED_FIELD, o.isEnabled(), n.isEnabled());}
    if (!Objects.equals(o.getEffectiveUserId(),n.getEffectiveUserId()))
      {noChanges=false;addChange(jo, EFFECTIVE_USER_ID_FIELD, o.getEffectiveUserId(), n.getEffectiveUserId());}
    if (!Objects.equals(o.getDefaultAuthnMethod(),n.getDefaultAuthnMethod()))
      {noChanges=false;addChange(jo, DEFAULT_AUTHN_METHOD_FIELD, o.getDefaultAuthnMethod().name(), n.getDefaultAuthnMethod().name());}
    if (!Objects.equals(o.getBucketName(),n.getBucketName()))
      {noChanges=false;addChange(jo, BUCKET_NAME_FIELD, o.getBucketName(), n.getBucketName());}
    if (!Objects.equals(o.getRootDir(),n.getRootDir()))
      {noChanges=false;addChange(jo, ROOT_DIR_FIELD, o.getRootDir(), n.getRootDir());}
    if (!Objects.equals(o.getPort(),n.getPort()))
      {noChanges=false;addChange(jo, PORT_FIELD, o.getPort(), n.getPort());}
    if (!Objects.equals(o.isUseProxy(),n.isUseProxy()))
      {noChanges=false;addChange(jo, USE_PROXY_FIELD, o.isUseProxy(), n.isUseProxy());}
    if (!Objects.equals(o.getProxyHost(),n.getProxyHost()))
      {noChanges=false;addChange(jo, PROXY_HOST_FIELD, o.getProxyHost(), n.getProxyHost());}
    if (!Objects.equals(o.getProxyPort(),n.getProxyPort()))
      {noChanges=false;addChange(jo, PROXY_PORT_FIELD, o.getProxyPort(), n.getProxyPort());}
    if (!Objects.equals(o.getDtnMountPoint(),n.getDtnMountPoint()))
      {noChanges=false;addChange(jo, DTN_MOUNT_POINT_FIELD, o.getDtnMountPoint(), n.getDtnMountPoint());}
    if (!Objects.equals(o.getDtnMountSourcePath(),n.getDtnMountSourcePath()))
      {noChanges=false;addChange(jo, DTN_MOUNT_SOURCE_PATH_FIELD, o.getDtnMountSourcePath(), n.getDtnMountSourcePath());}
    if (!Objects.equals(o.getDtnSystemId(),n.getDtnSystemId()))
      {noChanges=false;addChange(jo, DTN_SYSTEM_ID_FIELD, o.getDtnSystemId(), n.getDtnSystemId());}
    if (!Objects.equals(o.isDtn(),n.isDtn()))
      {noChanges=false;addChange(jo, IS_DTN_FIELD, o.isDtn(), n.isDtn());}
    if (!Objects.equals(o.getCanExec(),n.getCanExec()))
      {noChanges=false;addChange(jo, CAN_EXEC_FIELD, o.getCanExec(), n.getCanExec());}
    if (!Objects.equals(o.getCanRunBatch(),n.getCanRunBatch()))
      {noChanges=false;addChange(jo, CAN_RUN_BATCH_FIELD, o.getCanRunBatch(), n.getCanRunBatch());}
    if (!Objects.equals(o.isEnableCmdPrefix(),n.isEnableCmdPrefix()))
      {noChanges=false;addChange(jo, ENABLE_CMD_PREFIX_FIELD, o.isEnableCmdPrefix(), n.isEnableCmdPrefix());}
    if (!Objects.equals(o.getMpiCmd(),n.getMpiCmd()))
      {noChanges=false;addChange(jo, MPI_CMD_FIELD, o.getMpiCmd(), n.getMpiCmd());}
    if (!Objects.equals(o.getJobWorkingDir(),n.getJobWorkingDir()))
      {noChanges=false;addChange(jo, JOB_WORKING_DIR_FIELD, o.getJobWorkingDir(), n.getJobWorkingDir());}
    if (!Objects.equals(o.getJobMaxJobs(),n.getJobMaxJobs()))
      {noChanges=false;addChange(jo, JOB_MAX_JOBS_FIELD, o.getJobMaxJobs(), n.getJobMaxJobs());}
    if (!Objects.equals(o.getJobMaxJobsPerUser(),n.getJobMaxJobsPerUser()))
      {noChanges=false;addChange(jo, JOB_MAX_JOBS_PER_USER_FIELD, o.getJobMaxJobsPerUser(), n.getJobMaxJobsPerUser());}
    if (!Objects.equals(o.getBatchScheduler(),n.getBatchScheduler()))
      {noChanges=false;addChange(jo, BATCH_SCHEDULER_FIELD, o.getBatchScheduler().name(), n.getBatchScheduler().name());}
    if (!Objects.equals(o.getBatchDefaultLogicalQueue(),n.getBatchDefaultLogicalQueue()))
      {noChanges=false;addChange(jo, BATCH_DEFAULT_LOGICAL_QUEUE_FIELD, o.getBatchDefaultLogicalQueue(), n.getBatchDefaultLogicalQueue());}
    if (!Objects.equals(o.getBatchSchedulerProfile(),n.getBatchSchedulerProfile()))
      {noChanges=false;addChange(jo, BATCH_SCHEDULER_PROFILE_FIELD, o.getBatchSchedulerProfile(), n.getBatchSchedulerProfile());}
    if (!(o.isDeleted() == n.isDeleted()))
      {noChanges=false;addChange(jo, DELETED_FIELD, o.isDeleted(), n.isDeleted());}

    // ------------------------------------------------------
    // Following attributes require more complex handling
    // NOTE that the "if (notPatch ... )" below just means we avoid the compare if it is a patch and
    //   the patch did not update the attribute.
    // ------------------------------------------------------
    // JOB_RUNTIMES - If not a patch or patch value not null then need to compare
    if (notPatch || (p!=null && p.getJobRuntimes() != null))
    {
      // We can use Objects.equals() because JobRuntime supports equals, but order will be important.
      if (!Objects.equals(o.getJobRuntimes(), n.getJobRuntimes()))
        { noChanges = false; addChange(jo, JOB_RUNTIMES_FIELD, o.getJobRuntimes(), n.getJobRuntimes()); }
    }
    // JOB_ENV_VARIABLES
    // If not a patch or patch value not null then need to compare
    if (notPatch || (p!=null && p.getJobEnvVariables() != null))
    {
      // We can use Objects.equals() because KeyValuePair supports equals, but order will be important.
      if (!Objects.equals(o.getJobEnvVariables(), n.getJobEnvVariables()))
      {
        noChanges = false; addChange(jo, JOB_ENV_VARIABLES_FIELD, o.getJobEnvVariables(), n.getJobEnvVariables());
      }
    }
    // BATCH_LOGICAL_QUEUES
    // If not a patch or patch value not null then need to compare
    if (notPatch || (p!=null && p.getBatchLogicalQueues() != null))
    {
      // We can use Objects.equals() because LogicalQueue supports equals, but order will be important.
      if (!Objects.equals(o.getBatchLogicalQueues(),n.getBatchLogicalQueues()))
      {
        noChanges=false; addChange(jo, BATCH_LOGICAL_QUEUES_FIELD, o.getBatchLogicalQueues(), n.getBatchLogicalQueues());
      }
    }
    // JOB_CAPABILITIES
    // If not a patch or patch value not null then need to compare
    if (notPatch || (p!=null && p.getJobCapabilities() != null))
    {
      // We can use Objects.equals() because Capability supports equals, but order will be important.
      if (!Objects.equals(o.getJobCapabilities(), n.getJobCapabilities()))
      {
        noChanges = false; addChange(jo, JOB_CAPABILITIES_FIELD, o.getJobCapabilities(), n.getJobCapabilities());
      }
    }
    // TAGS - If it is a patch and the patch value was null then no need to compare
    //   i.e. if not a patch or patch value was not null then do need to compare.
    // Since TAGS are just strings we can use Objects.equals()
    if (notPatch || (p!=null && p.getTags() != null))
    {
      // Sort so it does not matter if order is different
      List<String> oldSortedTags = Arrays.asList(o.getTags());
      List<String> newSortedTags = Arrays.asList(n.getTags());
      Collections.sort(oldSortedTags);
      Collections.sort(newSortedTags);
      if (!Objects.equals(oldSortedTags, newSortedTags))
      {
        noChanges = false;
        addChange(jo, TAGS_FIELD, oldSortedTags, newSortedTags);
      }
    }
    // NOTES - If not a patch or patch value not null then need to compare
    if (notPatch || (p!=null && p.getNotes() != null))
    {
      if (!compareNotes(o.getNotes(), n.getNotes()))
      {
          noChanges=false;
          addChange(jo, NOTES_FIELD, (JsonObject) o.getNotes(), (JsonObject) n.getNotes());
      }
    }

    // If nothing has changed we are done.
    if (noChanges) return null;

    var joFinal = new JSONObject();
    joFinal.put("SystemId", o.getId());
    joFinal.put("AttributeChanges", jo);
    return joFinal.toString();
  }

  /**
   * Create a change description for a credential update.
   */
  public static String getChangeDescriptionCredCreate(String systemId, String user, boolean skipCredCheck, Credential cred)
  {
    var o = new JSONObject();
    o.put("System", systemId);
    o.put("TargetUser", user);
    o.put("SkipCredCheck", skipCredCheck);
    var oCred = new JSONObject();
    var cEntry = new JSONObject();
    cEntry.put("Password", cred.getPassword());
    cEntry.put("PublicKey", cred.getPublicKey());
    cEntry.put("PrivateKey", cred.getPrivateKey());
    cEntry.put("AccessKey", cred.getAccessKey());
    cEntry.put("AccessSecret", cred.getAccessSecret());
//    cEntry.put("AccessToken", cred.getAccessToken()); //TODO Globus
//    cEntry.put("RefreshToken", cred.getRefreshToken());
    cEntry.put("Certificate", cred.getCertificate());
    o.put("Credential", oCred);
    return o.toString();
  }

  /**
   * Create a change description for a credential delete.
   */
  public static String getChangeDescriptionCredDelete(String systemId, String user)
  {
    JSONObject o = new JSONObject();
    o.put("System", systemId);
    o.put("TargetUser", user);
    return o.toString();
  }

  /**
   * Create a change description for a permissions grant or revoke.
   */
  public static String getChangeDescriptionPermsUpdate(String systemId, String user, Set<TSystem.Permission> permissions)
  {
    var o = new JSONObject();
    o.put("System", systemId);
    o.put("TargetUser", user);
    var perms = new JSONArray();
    for (TSystem.Permission p : permissions) { perms.put(p.toString()); }
    o.put("Permissions", perms);
    return o.toString();
  }

  /**
   * Create a change description for update of owner.
   */
  public static String getChangeDescriptionUpdateOwner(String systemId, String oldOwner, String newOwner)
  {
    var o = new JSONObject();
    o.put("System", systemId);
    addChange(o, OWNER_FIELD, oldOwner, newOwner);
    return o.toString();
  }
  /*
   * Methods to add change entries for TSystem updates.
   */
  public static void addChange(JSONObject jo, String field, String o, String n)
  {
    var jo1 = new JSONObject();
    jo1.put("oldValue", o);
    jo1.put("newValue", n);
    jo.put(field, jo1);
  }
  public static void addChange(JSONObject jo, String field, boolean o, boolean n)
  {
    var jo1 = new JSONObject();
    jo1.put("oldValue", o);
    jo1.put("newValue", n);
    jo.put(field, jo1);
  }
  public static void addChange(JSONObject jo, String field, int o, int n)
  {
    var jo1 = new JSONObject();
    jo1.put("oldValue", o);
    jo1.put("newValue", n);
    jo.put(field, jo1);
  }
  public static void addChange(JSONObject jo, String field, String[] o, String[] n)
  {
    var jo1 = new JSONObject();
    // TODO/TBD how does this look?
    jo1.put("oldValue", o);
    jo1.put("newValue", n);
    jo.put(field, jo1);
  }
  public static void addChange(JSONObject jo, String field, JsonObject o, JsonObject n)
  {
    var jo1 = new JSONObject();
    // Convert gson.JsonObject to org.json.JSONObject
    var oj = new JSONObject(o.toString());
    var nj = new JSONObject(n.toString());
    jo1.put("oldValue", oj);
    jo1.put("newValue", nj);
    jo.put(field, jo1);
  }
  public static void addChange(JSONObject jo, String field, List<?> o, List<?> n)
  {
    var jo1 = new JSONObject();
    // TODO/TBD how does this look?
    jo1.put("oldValue", o);
    jo1.put("newValue", n);
    jo.put(field, jo1);
  }

  /*
   * To compare notes cast the Objects to gson's JsonObject and let gson do the compare
   */
  private static boolean compareNotes(Object o, Object n)
  {
    JsonObject oj = (JsonObject) o;
    JsonObject nj = (JsonObject) n;
    return Objects.equals(oj, nj);
  }
}
