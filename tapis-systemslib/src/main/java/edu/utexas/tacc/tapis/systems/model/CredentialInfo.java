package edu.utexas.tacc.tapis.systems.model;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/*
 * Class representing metadata for credentials stored in the Security Kernel.
 * Credentials are tied to a specific system and user.
 * Also includes login user associated with the credential.
 *
 * Secrets are not persisted by the Systems Service. Actual secrets are managed by the Security Kernel.
 *
 * Systems service does store a mapping of tapis user to login user if they are different.
 * If a System has a static effectiveUserId then there will be no mapping.
 *
 * Note that we do not make this class fully immutable because we need to keep the in-memory object in sync
 *   with the DB record
 */
public class CredentialInfo
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                               Enums                                    */
  /* ********************************************************************** */
  // States:
  //   --   sync_status - indicates current status of synchronization between SK and Systems service.
  //--      PENDING - Record requires synchronization (Initial state)
  //--      IN_PROGRESS - Systems service is in the process of synchronizing the record
  //--      FAILED - Synchronization failed.
  //--      DELETED - Record marked as deleted, but not yet removed from DB. Could potentially get re-created.
  //--      COMPLETED - Synchronization completed successfully.
  public enum SyncStatus {PENDING, IN_PROGRESS, FAILED, DELETED, COMPLETED}

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */

  // Mutex for locking record during an update
  // set fairness to true, meaning under contention, locks favor granting access to the longest-waiting thread.
  public final ReentrantLock mutex = new ReentrantLock(true);

  private final int systemSeqId; // Sequence id associated with the system id
  private final String tenant; // Name of tenant associated with the credential
  private final String systemId; // Name of the system associated with the credential
  private final String tapisUser; // Tapis user associated with the credential
  private final String loginUser; // For a system with a dynamic effectiveUserId, this is the host login user.
  private final boolean isStatic; // Indicates if record is for the static or dynamic effectiveUserId case.
  private boolean hasCredentials; // Indicates if system has credentials registered for the current defaultAuthnMethod
  private boolean hasPassword; // Indicates if credentials for PASSWORD have been registered.
  private boolean hasPkiKeys; // Indicates if credentials for PKI_KEYS have been registered.
  private boolean hasAccessKey; // Indicates if credentials for ACCESS_KEY have been registered.
  private boolean hasToken; // Indicates if credentials for TOKEN have been registered.
  private SyncStatus syncStatus; // Indicates current status of synchronization between SK and Systems service.
  private int syncFailCount; // Number of sync attempts that have failed
  private String syncFailMessage; // Message indicating why last sync attempt failed
  private Instant syncFailed; // UTC time for time of last sync failure. Null if no failures.
  private final Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Simple constructor to populate all attributes
   */
  public CredentialInfo(int systemSeqId1, String tenant1, String systemId1, String tapisUser1, String loginUser1,
                        boolean isStatic1, boolean hasCredentials1, boolean hasPassword1, boolean hasPkiKeys1,
                        boolean hasAccessKey1, boolean hasToken1, SyncStatus syncStatus1, int syncFailCount1,
                        String syncFailMessage1, Instant syncFailed1,  Instant created1, Instant updated1)
  {
    systemSeqId = systemSeqId1;
    tenant = tenant1;
    systemId = systemId1;
    tapisUser = tapisUser1;
    loginUser = loginUser1;
    isStatic = isStatic1;
    hasCredentials = hasCredentials1;
    hasPassword = hasPassword1;
    hasPkiKeys = hasPkiKeys1;
    hasAccessKey = hasAccessKey1;
    hasToken = hasToken1;
    syncStatus = syncStatus1;
    syncFailCount = syncFailCount1;
    syncFailMessage = syncFailMessage1;
    syncFailed = syncFailed1;
    created = created1;
    updated = updated1;
  }

  /**
   * Constructor using only required attributes.
   * For initial state of the record.
   */
  public CredentialInfo(int systemSeqId1, String tenant1, String systemId1, String tapisUser1, boolean isStatic1,
                        SyncStatus syncStatus1)
  {
    systemSeqId = systemSeqId1;
    tenant = tenant1;
    systemId = systemId1;
    tapisUser = tapisUser1;
    isStatic = isStatic1;
    loginUser = null;
    hasCredentials = false;
    hasPassword = false;
    hasPkiKeys = false;
    hasAccessKey = false;
    hasToken = false;
    syncStatus = syncStatus1;
    syncFailCount = 0;
    syncFailMessage = null;
    syncFailed = null;
    created = null;
    updated = null;
  }

// TODO/TBD Order of columns is
//     seq_id, tenant, system_id, tapis_user, login_user, created, updated, has_credentials, is_static, has_password,
//     has_pki_keys, has_access_key, has_token, sync_status, sync_failed, sync_fail_count, sync_fail_message

//  /**
//   * Constructor for jOOQ with input parameter matching order of columns in DB
//   * Also useful for testing
//   */
//  public TSystem(int seqId1, String tenant1, String id1, String description1, TSystem.SystemType systemType1,


  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */

  public void incrementSyncFailCount() { syncFailCount++; }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public int getSystemSeqId() { return systemSeqId; }
  public String getTenant() { return tenant; }
  public String getSystemId() { return systemId; }
  public String getTapisUser() { return tapisUser; }
  public String getLoginUser() { return loginUser; }
  public boolean isStatic() { return isStatic; }
  public boolean hasCredentials() { return hasCredentials; }
  public void setHasCredentials(boolean b) { hasCredentials = b; }

  public boolean hasPassword() { return hasPassword; }
  public void setHasPassword(boolean b) { hasPassword = b; }

  public boolean hasPkiKeys() { return hasPkiKeys; }
  public void setHasPkiKeys(boolean b) { hasPkiKeys = b; }

  public boolean hasAccessKey() { return hasAccessKey; }
  public void setHasAccessKey(boolean b) { hasAccessKey = b; }

  public boolean hasToken() { return hasToken; }
  public void setHasToken(boolean b) { hasToken = b; }

  public SyncStatus getSyncStatus() { return syncStatus; }
  public void setSyncStatus(SyncStatus s) { syncStatus = s; }

  public int getSyncFailCount() { return syncFailCount; }
  public void setSyncFailCount(int i) { syncFailCount =i; }

  public String getSyncFailMessage() { return syncFailMessage; }
  public void setSyncFailMessage(String s) { syncFailMessage = s; }

  @Schema(type = "string")
  public Instant getSyncFailed() { return syncFailed; }
  public void setSyncFailed(Instant t) { syncFailed = t; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }
  public void setUpdated(Instant t) { updated = t; }
}
