/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.core.v3.ConnectionFactoryImpl;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Handles protocol-specific connection setup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public abstract class ConnectionFactory {
  /**
   * Establishes and initializes a new connection.
   * <p>
   * If the "protocolVersion" property is specified, only that protocol version is tried. Otherwise,
   * all protocols are tried in order, falling back to older protocols as necessary.
   * <p>
   * Currently, protocol versions 3 (7.4+) and 2 (pre-7.4) are supported.
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param logger the logger to use for this connection
   * @return the new, initialized, connection
   * @throws SQLException if the connection could not be established.
   */
  public static QueryExecutor openConnection(HostSpec[] hostSpecs, String user,
      String database, Properties info, Logger logger) throws SQLException {
    String protoName = PGProperty.PROTOCOL_VERSION.get(info);

    if (protoName == null || "".equals(protoName)
        || "2".equals(protoName) || "3".equals(protoName)) {
      ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
      QueryExecutor queryExecutor =
          connectionFactory.openConnectionImpl(hostSpecs, user, database, info, logger);
      if (queryExecutor != null) {
        return queryExecutor;
      }
    }

    throw new PSQLException(
        GT.tr("A connection could not be made using the requested protocol {0}.", protoName),
        PSQLState.CONNECTION_UNABLE_TO_CONNECT);
  }

  /**
   * Implementation of {@link #openConnection} for a particular protocol version. Implemented by
   * subclasses of {@link ConnectionFactory}.
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param logger the logger to use for this connection
   * @return the new, initialized, connection, or <code>null</code> if this protocol version is not
   *         supported by the server.
   * @throws SQLException if the connection could not be established for a reason other than
   *         protocol version incompatibility.
   */
  public abstract QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user,
      String database, Properties info, Logger logger) throws SQLException;

  /**
   * Safely close the given stream.
   *
   * @param newStream The stream to close.
   */
  protected void closeStream(PGStream newStream) {
    if (newStream != null) {
      try {
        newStream.close();
      } catch (IOException e) {
      }
    }
  }
}
