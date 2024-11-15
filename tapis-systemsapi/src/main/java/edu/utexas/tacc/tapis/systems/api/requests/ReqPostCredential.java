package edu.utexas.tacc.tapis.systems.api.requests;

/*
 * Class representing Credential attributes that can be set in an incoming create request json body
 */
public final class ReqPostCredential
{
  public String loginUser;
  public String password; // Password for authnMethod PASSWORD
  public String privateKey; // Private key for authnMethod PKI_KEYS
  public String publicKey; // Public key for authnMethod PKI_KEYS
  public String accessKey; // Access key for authnMethod ACCESS_KEY
  public String accessSecret; // Access secret for authnMethod ACCESS_KEY
  public String accessToken; // Access secret for authnMethod TOKEN
  public String refreshToken; // Access secret for authnMethod TOKEN
  public String certificate; // SSH certificate for authnMethod CERT
}
