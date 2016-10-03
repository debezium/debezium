/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ssl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.ssl.SSLSocketFactory;

/**
 * Provide a wrapper to a real SSLSocketFactory delegating all calls to the contained instance. A
 * subclass needs only provide a constructor for the wrapped SSLSocketFactory.
 */
public abstract class WrappedFactory extends SSLSocketFactory {

  protected SSLSocketFactory _factory;

  public Socket createSocket(InetAddress host, int port) throws IOException {
    return _factory.createSocket(host, port);
  }

  public Socket createSocket(String host, int port) throws IOException {
    return _factory.createSocket(host, port);
  }

  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return _factory.createSocket(host, port, localHost, localPort);
  }

  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return _factory.createSocket(address, port, localAddress, localPort);
  }

  public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
      throws IOException {
    return _factory.createSocket(socket, host, port, autoClose);
  }

  public String[] getDefaultCipherSuites() {
    return _factory.getDefaultCipherSuites();
  }

  public String[] getSupportedCipherSuites() {
    return _factory.getSupportedCipherSuites();
  }
}
