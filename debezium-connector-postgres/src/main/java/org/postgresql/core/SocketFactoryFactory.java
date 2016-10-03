package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.util.GT;
import org.postgresql.util.ObjectFactory;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.util.Properties;

import javax.net.SocketFactory;

/**
 * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}.
 */
public class SocketFactoryFactory {

  /**
   * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}
   *
   * @param info connection properties
   * @return socket factory
   * @throws PSQLException if something goes wrong
   */
  public static SocketFactory getSocketFactory(Properties info) throws PSQLException {
    // Socket factory
    String socketFactoryClassName = PGProperty.SOCKET_FACTORY.get(info);
    if (socketFactoryClassName == null) {
      return SocketFactory.getDefault();
    }
    try {
      return (SocketFactory) ObjectFactory.instantiate(socketFactoryClassName, info, true,
          PGProperty.SOCKET_FACTORY_ARG.get(info));
    } catch (Exception e) {
      throw new PSQLException(
          GT.tr("The SocketFactory class provided {0} could not be instantiated.",
              socketFactoryClassName),
          PSQLState.CONNECTION_FAILURE, e);
    }
  }

}
