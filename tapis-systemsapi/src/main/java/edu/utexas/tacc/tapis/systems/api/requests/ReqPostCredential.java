package edu.utexas.tacc.tapis.systems.api.requests;

/*
 * Class representing Credential attributes that can be set in an incoming create request json body
 */
public final class ReqPostCredential
{
  public String loginUser;
  public String password; // Password for when authnMethod is PASSWORD
  public String privateKey; // Private key for when authnMethod is PKI_KEYS or CERT
  public String publicKey; // Public key for when authnMethod is PKI_KEYS or CERT
  public String accessKey; // Access key for when authnMethod is ACCESS_KEY
  public String accessSecret; // Access secret for when authnMethod is ACCESS_KEY
  public String accessToken; // Access secret for when authnMethod is TOKEN
  public String refreshToken; // Access secret for when authnMethod is TOKEN
  public String certificate; // SSH certificate for authnMethod is CERT
}
