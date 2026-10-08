package edu.utexas.tacc.tapis.systems.model;

import org.apache.commons.lang3.StringUtils;

import static edu.utexas.tacc.tapis.systems.model.Credential.SECRETS_MASK;

/*
 * Class representing request attributes that can be contained in a credential creation request.
 * Immutable
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 */
public final class ReqCreateCredential
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final String password; // Password for authnMethod PASSWORD
  private final String privateKey; // Private key for authnMethod PKI_KEYS
  private final String publicKey; // Public key for authnMethod PKI_KEYS
  private final String accessKey; // Access key for authnMethod ACCESS_KEY
  private final String accessSecret; // Access secret for authnMethod is ACCESS_KEY
  private final String accessToken; // Access token for authnMethod TOKEN
  private final String refreshToken; // Refresh token for authnMethod TOKEN
  private final String certificate; // SSH certificate for authnMethod is CERT
  private final String loginUser; // For a system with a dynamic effectiveUserId, this is the host login user.
  private final String tmsResourceProvider; // For a system using TMS_KEYS, this is the resource provider (tacc, sdsc, etc)
  private final String tmsResourceProviderAccount; // For a system using TMS_KEYS, this is resource provider account (e.g. someuser@sdsc)

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  // Simple constructor to populate all attributes
  public ReqCreateCredential(String password1, String privateKey1, String publicKey1, String accessKey1,
                             String accessSecret1, String accessToken1, String refreshToken1, String cert1,
                             String loginUser1, String tmsResourceProvider1, String tmsResourceProviderAccount1)
  {
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
    accessToken = accessToken1;
    refreshToken = refreshToken1;
    certificate = cert1;
    loginUser = loginUser1;
    tmsResourceProvider = tmsResourceProvider1;
    tmsResourceProviderAccount = tmsResourceProviderAccount1;
  }

  /* ********************************************************************** */
  /*                        Public methods                                  */
  /* ********************************************************************** */

  /**
   * Create a credential with secrets masked out
   */
  public static ReqCreateCredential createMaskedReqCreateCredential(ReqCreateCredential reqCreateCred)
  {
    if (reqCreateCred == null) return null;
    String accessToken, refreshToken, accessKey, accessSecret, password, privateKey, publicKey, cert;
    accessToken = (!StringUtils.isBlank(reqCreateCred.getAccessToken())) ? SECRETS_MASK : reqCreateCred.getAccessToken();
    refreshToken = (!StringUtils.isBlank(reqCreateCred.getRefreshToken())) ? SECRETS_MASK : reqCreateCred.getRefreshToken();
    accessKey = (!StringUtils.isBlank(reqCreateCred.getAccessKey())) ? SECRETS_MASK : reqCreateCred.getAccessKey();
    accessSecret = (!StringUtils.isBlank(reqCreateCred.getAccessSecret())) ? SECRETS_MASK : reqCreateCred.getAccessSecret();
    password = (!StringUtils.isBlank(reqCreateCred.getPassword())) ? SECRETS_MASK : reqCreateCred.getPassword();
    privateKey = (!StringUtils.isBlank(reqCreateCred.getPrivateKey())) ? SECRETS_MASK : reqCreateCred.getPrivateKey();
    publicKey = (!StringUtils.isBlank(reqCreateCred.getPublicKey())) ? SECRETS_MASK : reqCreateCred.getPublicKey();
    cert = (!StringUtils.isBlank(reqCreateCred.getCertificate())) ? SECRETS_MASK : reqCreateCred.getCertificate();
    return new ReqCreateCredential(password, privateKey, publicKey, accessKey, accessSecret, accessToken, refreshToken, cert,
            reqCreateCred.getLoginUser(), reqCreateCred.getTmsResourceProvider(), reqCreateCred.getTmsResourceProviderAccount());
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }
  public String getAccessToken() { return accessToken; }
  public String getRefreshToken() { return refreshToken; }
  public String getCertificate() { return certificate; }
  public String getLoginUser() { return loginUser; }
  public String getTmsResourceProvider() { return tmsResourceProvider; }
  public String getTmsResourceProviderAccount() { return tmsResourceProviderAccount; }

  @Override
  public String toString()
  {
    String p = StringUtils.isBlank(password) ? "<empty>" : "*********";
    String pPrivKey = StringUtils.isBlank(privateKey) ? "<empty>" : "*********";
    String pPubKey = StringUtils.isBlank(publicKey) ? "<empty>" : "*********";
    String aKey = StringUtils.isBlank(accessKey) ? "<empty>" : "*********";
    String aSecret = StringUtils.isBlank(accessSecret) ? "<empty>" : "*********";
    String aTok = StringUtils.isBlank(accessToken) ? "<empty>" : "*********";
    String aRefresh = StringUtils.isBlank(refreshToken) ? "<empty>" : "*********";
    String l = StringUtils.isBlank(loginUser) ? "<empty>" : loginUser;
    String tms1 = StringUtils.isBlank(getTmsResourceProvider()) ? "<empty>" : tmsResourceProvider;
    String tms2 = StringUtils.isBlank(getTmsResourceProviderAccount()) ? "<empty>" : tmsResourceProviderAccount;
    String fmtStr = """
            ReqCreateCredential:%n
              password: %s%n  privateKey: %s%n publicKey: %s%n  accessKey: %s%n  accessSecret: %s%n accessToken: %s%n
                refreshToken: %s%n  loginUser: %s%n  tmsResourceProvider: %s%n  tmsResourceProviderAccount: %s%n
            """;
    return String.format(fmtStr, p, pPrivKey, pPubKey, aKey, aSecret, aTok, aRefresh, l, tms1, tms2);
  }
}
