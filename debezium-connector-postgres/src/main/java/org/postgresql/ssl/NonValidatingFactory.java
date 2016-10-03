/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ssl;

import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Provide a SSLSocketFactory that allows SSL connections to be made without validating the server's
 * certificate. This is more convenient for some applications, but is less secure as it allows "man
 * in the middle" attacks.
 */
public class NonValidatingFactory extends WrappedFactory {

  /**
   * We provide a constructor that takes an unused argument solely because the ssl calling code will
   * look for this constructor first and then fall back to the no argument constructor, so we avoid
   * an exception and additional reflection lookups.
   *
   * @param arg input argument
   * @throws GeneralSecurityException if something goes wrong
   */
  public NonValidatingFactory(String arg) throws GeneralSecurityException {
    SSLContext ctx = SSLContext.getInstance("TLS"); // or "SSL" ?

    ctx.init(null, new TrustManager[]{new NonValidatingTM()}, null);

    _factory = ctx.getSocketFactory();
  }

  public static class NonValidatingTM implements X509TrustManager {

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }

    public void checkClientTrusted(X509Certificate[] certs, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] certs, String authType) {
    }
  }
}
