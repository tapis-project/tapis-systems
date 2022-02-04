package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import org.apache.commons.lang3.StringUtils;

/*
 * Credential class representing an authn credential stored in the Security Kernel.
 * Credentials are not persisted by the Systems Service. Actual secrets are managed by
 *   the Security Kernel.
 * The secret information will depend on the system type and authn method.
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


  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */

  private final AuthnMethod authnMethod; // Authentication method associated with a retrieved credential
  private final String password; // Password for when authnMethod is PASSWORD
  private final String privateKey; // Private key for when authnMethod is PKI_KEYS or CERT
  private final String publicKey; // Public key for when authnMethod is PKI_KEYS or CERT
  private final String accessKey; // Access key for when authnMethod is ACCESS_KEY
  private final String accessSecret; // Access secret for when authnMethod is ACCESS_KEY
  private final String accessToken;
  private final String refreshToken;
  private final String certificate; // SSH certificate for authnMethod is CERT

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Simple constructor to populate all attributes
   */
  public Credential(AuthnMethod authnMethod1, String password1, String privateKey1, String publicKey1,
                    String accessKey1, String accessSecret1, String accessToken1, String refreshToken1, String cert1)
  {
    authnMethod = authnMethod1;
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
    accessToken = accessToken1;
    refreshToken = refreshToken1;
    certificate = cert1;
  }

  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */
  /**
   * Create a credential with secrets masked out
   */
  public static Credential createMaskedCredential(Credential credential)
  {
    if (credential == null) return null;
    String accessToken, refreshToken, accessKey, accessSecret, password, privateKey, publicKey, cert;
    accessToken = (!StringUtils.isBlank(credential.getAccessToken())) ? SECRETS_MASK : credential.getAccessToken();
    refreshToken = (!StringUtils.isBlank(credential.getRefreshToken())) ? SECRETS_MASK : credential.getRefreshToken();
    accessKey = (!StringUtils.isBlank(credential.getAccessKey())) ? SECRETS_MASK : credential.getAccessKey();
    accessSecret = (!StringUtils.isBlank(credential.getAccessSecret())) ? SECRETS_MASK : credential.getAccessSecret();
    password = (!StringUtils.isBlank(credential.getPassword())) ? SECRETS_MASK : credential.getPassword();
    privateKey = (!StringUtils.isBlank(credential.getPrivateKey())) ? SECRETS_MASK : credential.getPrivateKey();
    publicKey = (!StringUtils.isBlank(credential.getPublicKey())) ? SECRETS_MASK : credential.getPublicKey();
    cert = (!StringUtils.isBlank(credential.getCertificate())) ? SECRETS_MASK : credential.getCertificate();
    return new Credential(credential.getAuthnMethod(), password, privateKey, publicKey, accessKey, accessSecret,
                          accessToken, refreshToken, cert);
  }

  /**
   * Check if private key is compatible with Tapis.
   * SSH key-pairs that have a private key starting with: --- BEGIN OPENSSH PRIVATE KEY ---
   * cannot be used in in TapisV3. the Jsch library does not yet support them.
   * Instead a private key starting with:{{ — BEGIN RSA PRIVATE KEY ---}}
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
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }
  public String getAccessToken() { return accessToken; }
  public String getRefreshToken() { return refreshToken; }
  public String getCertificate() { return certificate; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
