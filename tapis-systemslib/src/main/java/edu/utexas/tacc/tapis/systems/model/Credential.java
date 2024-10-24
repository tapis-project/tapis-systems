package edu.utexas.tacc.tapis.systems.model;

import org.apache.commons.lang3.StringUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
/*
 * Credential class representing an authn credential stored in the Security Kernel.
 * Also includes login user associated with the credential.
 *
 * Secrets are not persisted by the Systems Service. Actual secrets are managed by the Security Kernel.
 * The secret information will depend on the system type and authn method.
 *
 * Systems service does store a mapping of tapis user to login user if they are different.
 * If a System has a static effectiveUserId then there will be no mapping.
 *
 * Immutable
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 */
public final class Credential
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  // Top level name for storing system secrets
  public static final String TOP_LEVEL_SECRET_NAME = "S1";
  // String used to mask secrets
  public static final String SECRETS_MASK = "***";

  // Keys for constructing map when writing secrets to Security Kernel
  public static final String SK_KEY_PASSWORD = "password";
  public static final String SK_KEY_PUBLIC_KEY = "publicKey";
  public static final String SK_KEY_PRIVATE_KEY = "privateKey";
  public static final String SK_KEY_ACCESS_KEY = "accessKey";
  public static final String SK_KEY_ACCESS_SECRET = "accessSecret";
  public static final String SK_KEY_ACCESS_TOKEN = "accessToken";
  public static final String SK_KEY_REFRESH_TOKEN = "refreshToken";
  public static final String SK_KEY_TMS_PUBLIC_KEY = "tmsPublicKey";
  public static final String SK_KEY_TMS_PRIVATE_KEY = "tmsPrivateKey";
  public static final String SK_KEY_TMS_FINGERPRINT = "tmsFingerprint";

  // Default validation message
  public static final String VALIDATION_MSG_DEFAULT = "Error. Validation message not updated";

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */

  private final AuthnMethod authnMethod; // Authentication method associated with a retrieved credential
  private final String loginUser;
  private final String password; // Password authnMethod PASSWORD
  private final String privateKey; // Private key for authnMethod PKI_KEYS
  private final String publicKey; // Public key for authnMethod PKI_KEYS
  private final String accessKey; // Access key for authnMethod ACCESS_KEY
  private final String accessSecret; // Access secret for authnMethod is ACCESS_KEY
  private final String accessToken; // Access token for authnMethod TOKEN
  private final String refreshToken; // Refresh token for authnMethod TOKEN
  private final String tmsPrivateKey; // Private key for authnMethod TMS_KEYS
  private final String tmsPublicKey; // Public key for authnMethod TMS_KEYS
  private final String tmsFingerprint; // Fingerprint of TMS private key
  private final String certificate; // SSH certificate for authnMethod is CERT
  private final Boolean validationResult; // Result of validation, if performed. null if no validation
  private final String validationMsg; // Reason validation failed. Null if no validation or validation succeeded.

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Simple constructor to populate all attributes
  public Credential(AuthnMethod authnMethod1, String loginUser1, String password1, String privateKey1, String publicKey1,
                    String accessKey1, String accessSecret1, String accessToken1, String refreshToken1,
                    String tmsPrivateKey1, String tmsPublicKey1, String tmsFingerprint1, String cert1,
                    Boolean validationResult1, String validationMsg1)
  {
    authnMethod = authnMethod1;
    loginUser = loginUser1;
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
    accessToken = accessToken1;
    refreshToken = refreshToken1;
    tmsPrivateKey = tmsPrivateKey1;
    tmsPublicKey = tmsPublicKey1;
    tmsFingerprint = tmsFingerprint1;
    certificate = cert1;
    validationResult = validationResult1;
    validationMsg = validationMsg1;
  }
  // Simple constructor to populate all attributes except validation result and message.
  // Validation result defaults to FALSE and validation message set to a default value.
  public Credential(AuthnMethod authnMethod1, String loginUser1, String password1, String privateKey1,
                    String publicKey1, String accessKey1, String accessSecret1, String accessToken1,
                    String refreshToken1, String tmsPrivateKey1, String tmsPublicKey1, String tmsFingerprint1,
                    String cert1)
  {
    authnMethod = authnMethod1;
    loginUser = loginUser1;
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
    accessToken = accessToken1;
    refreshToken = refreshToken1;
    tmsPrivateKey = tmsPrivateKey1;
    tmsPublicKey = tmsPublicKey1;
    tmsFingerprint = tmsFingerprint1;
    certificate = cert1;
    validationResult = Boolean.FALSE;
    validationMsg = VALIDATION_MSG_DEFAULT;
  }

  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */
  /**
   * Create a credential with secrets masked out
   */
  public static Credential createMaskedCredential(Credential cred)
  {
    if (cred == null) return null;
    String accessToken, refreshToken, accessKey, accessSecret, password, privateKey, publicKey,
           tmsPrivateKey, tmsPublicKey, tmsFingerprint, cert;
    accessToken = (!StringUtils.isBlank(cred.getAccessToken())) ? SECRETS_MASK : cred.getAccessToken();
    refreshToken = (!StringUtils.isBlank(cred.getRefreshToken())) ? SECRETS_MASK : cred.getRefreshToken();
    accessKey = (!StringUtils.isBlank(cred.getAccessKey())) ? SECRETS_MASK : cred.getAccessKey();
    accessSecret = (!StringUtils.isBlank(cred.getAccessSecret())) ? SECRETS_MASK : cred.getAccessSecret();
    password = (!StringUtils.isBlank(cred.getPassword())) ? SECRETS_MASK : cred.getPassword();
    privateKey = (!StringUtils.isBlank(cred.getPrivateKey())) ? SECRETS_MASK : cred.getPrivateKey();
    publicKey = (!StringUtils.isBlank(cred.getPublicKey())) ? SECRETS_MASK : cred.getPublicKey();
    tmsPrivateKey = (!StringUtils.isBlank(cred.getTmsPrivateKey())) ? SECRETS_MASK : cred.getTmsPrivateKey();
    tmsPublicKey = (!StringUtils.isBlank(cred.getTmsPublicKey())) ? SECRETS_MASK : cred.getTmsPublicKey();
    tmsFingerprint = (!StringUtils.isBlank(cred.getTmsFingerprint())) ? SECRETS_MASK : cred.getTmsFingerprint();
    cert = (!StringUtils.isBlank(cred.getCertificate())) ? SECRETS_MASK : cred.getCertificate();
    return new Credential(cred.getAuthnMethod(), cred.getLoginUser(), password, privateKey, publicKey,
                          accessKey, accessSecret, accessToken, refreshToken, tmsPrivateKey, tmsPublicKey,
                          tmsFingerprint, cert, cred.getValidationResult(), cred.getValidationMsg());
  }

  /**
   * Check if private key is compatible with Tapis.
   * SSH key-pairs that have a private key starting with: --- BEGIN OPENSSH PRIVATE KEY ---
   * cannot be used in TapisV3. the Jsch library does not yet support them.
   * Instead, a private key starting with:{{ — BEGIN RSA PRIVATE KEY ---}}
   * should be used. Recent openssh versions generate OPENSSH type keys.
   * To generate compatible keys one should use the option -m PEM with ssh-keygen, e.g.
   * ssh-keygen -t rsa -b 4096 -m PEM
   *
   * @return  true if private key is compatible
   */
  public boolean isValidPrivateSshKey()
  {
    if (StringUtils.isBlank(privateKey)) return false;
    if (privateKey.contains("BEGIN OPENSSH PRIVATE KEY")) return false;
    return true;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public AuthnMethod getAuthnMethod() { return authnMethod; }
  public String getLoginUser() { return loginUser; }
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }
  public String getAccessToken() { return accessToken; }
  public String getRefreshToken() { return refreshToken; }
  public String getTmsPrivateKey() { return tmsPrivateKey; }
  public String getTmsPublicKey() { return tmsPublicKey; }
  public String getTmsFingerprint() { return tmsFingerprint; }
  public String getCertificate() { return certificate; }
  public Boolean getValidationResult() { return validationResult; }
  public String getValidationMsg() { return validationMsg; }

  @Override
  public String toString()
  {
    String l = StringUtils.isBlank(loginUser) ? "<empty>" : loginUser;
    String p = StringUtils.isBlank(password) ? "<empty>" : "*********";
    String privKey = StringUtils.isBlank(privateKey) ? "<empty>" : "*********";
    String pubKey = StringUtils.isBlank(publicKey) ? "<empty>" : "*********";
    String aKey = StringUtils.isBlank(accessKey) ? "<empty>" : "*********";
    String aSecret = StringUtils.isBlank(accessSecret) ? "<empty>" : "*********";
    String aTok = StringUtils.isBlank(accessToken) ? "<empty>" : "*********";
    String aRefresh = StringUtils.isBlank(refreshToken) ? "<empty>" : "*********";
    String tprivKey = StringUtils.isBlank(tmsPrivateKey) ? "<empty>" : "*********";
    String tpubKey = StringUtils.isBlank(tmsPublicKey) ? "<empty>" : "*********";
    String tfingerprint = StringUtils.isBlank(tmsFingerprint) ? "<empty>" : "*********";
    return String.format("Credential:%n  AuthnMethod: %s%n  loginUser: %s%n  password: %s%n  privateKey: %s%n  publicKey: %s%n  accessKey: %s%n  accessSecret: %s%n accessToken: %s%n refreshToken: %s%n  tmsPrivateKey: %s%n  tmsPublicKey: %s%n  tmsFingerPrint: %s%n  validationResult: %B%n  validationMsg: %s%n",
                         authnMethod, l, p, privKey, pubKey, aKey, aSecret, aTok, aRefresh, tprivKey, tpubKey, tfingerprint, validationResult, validationMsg);
  }
}
