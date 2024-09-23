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
 * Immutable
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 */
public final class CredentialInfo
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

  private final String tenant; // Name of tenant associated with the credential
  private final String systemId; // Name of the system associated with the credential
  private final String tapisUser; // Tapis user associated with the credential
  private final String loginUser; // For a system with a dynamic effectiveUserId, this is the host login user.
  private final boolean isStatic; // Indicates if record is for the static or dynamic effectiveUserId case.
  private final boolean hasCredentials; // Indicates if system has credentials registered for the current defaultAuthnMethod
  private final boolean hasPassword; // Indicates if credentials for PASSWORD have been registered.
  private final boolean hasPkiKeys; // Indicates if credentials for PKI_KEYS have been registered.
  private final boolean hasAccessKey; // Indicates if credentials for ACCESS_KEY have been registered.
  private final boolean hasToken; // Indicates if credentials for TOKEN have been registered.
  private final SyncStatus syncStatus; // Indicates current status of synchronization between SK and Systems service.
  private final long syncFailCount; // Number of sync attempts that have failed
  private final String syncFailMessage; // Message indicating why last sync attempt failed
  private final Instant syncFailed; // UTC time for time of last sync failure. Null if no failures.
  private final Instant created; // UTC time for when record was created
  private final Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Simple constructor to populate all attributes
  public CredentialInfo(String tenant1, String systemId1, String tapisUser1, String loginUser1,
                        boolean isStatic1, boolean hasCredentials1, boolean hasPassword1, boolean hasPkiKeys1,
                        boolean hasAccessKey1, boolean hasToken1, SyncStatus syncStatus1, long syncFailCount1,
                        String syncFailMessage1, Instant syncFailed1,  Instant created1, Instant updated1)
  {
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
  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getTenant() { return tenant; }
  public String getSystemId() { return systemId; }
  public String getTapisUser() { return tapisUser; }
  public String getLoginUser() { return loginUser; }
  public boolean isStatic() { return isStatic; }
  public boolean hasCredentials() { return hasCredentials; }
  public boolean hasPassword() { return hasPassword; }
  public boolean hasPkiKeys() { return hasPkiKeys; }
  public boolean hasAccessKey() { return hasAccessKey; }
  public boolean hasToken() { return hasToken; }
  public SyncStatus getSyncStatus() { return syncStatus; }
  public long getSyncFailCount() { return syncFailCount; }
  public String getSyncFailMessage() { return syncFailMessage; }
  @Schema(type = "string")
  public Instant getSyncFailed() { return syncFailed; }
  @Schema(type = "string")
  public Instant getCreated() { return created; }
  @Schema(type = "string")
  public Instant getUpdated() { return updated; }
}
