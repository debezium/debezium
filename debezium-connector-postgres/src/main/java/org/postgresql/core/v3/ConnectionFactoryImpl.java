/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2016, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.PGProperty;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.Logger;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.SetupQueryRunner;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.core.Utils;
import org.postgresql.core.Version;
import org.postgresql.hostchooser.GlobalHostStatusTracker;
import org.postgresql.hostchooser.HostChooser;
import org.postgresql.hostchooser.HostChooserFactory;
import org.postgresql.hostchooser.HostRequirement;
import org.postgresql.hostchooser.HostStatus;
import org.postgresql.sspi.ISSPIClient;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.MD5Digest;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;
import org.postgresql.util.UnixCrypt;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import javax.net.SocketFactory;

/**
 * ConnectionFactory implementation for version 3 (7.4+) connections.
 *
 * @author Oliver Jowett (oliver@opencloud.com), based on the previous implementation
 */
public class ConnectionFactoryImpl extends ConnectionFactory {
  private static final int AUTH_REQ_OK = 0;
  private static final int AUTH_REQ_KRB4 = 1;
  private static final int AUTH_REQ_KRB5 = 2;
  private static final int AUTH_REQ_PASSWORD = 3;
  private static final int AUTH_REQ_CRYPT = 4;
  private static final int AUTH_REQ_MD5 = 5;
  private static final int AUTH_REQ_SCM = 6;
  private static final int AUTH_REQ_GSS = 7;
  private static final int AUTH_REQ_GSS_CONTINUE = 8;
  private static final int AUTH_REQ_SSPI = 9;

  /**
   * Marker exception; thrown when we want to fall back to using V2.
   */
  private static class UnsupportedProtocolException extends IOException {
  }

  private ISSPIClient createSSPI(PGStream pgStream,
      String spnServiceClass,
      boolean enableNegotiate,
      Logger logger) {
    try {
      Class c = Class.forName("org.postgresql.sspi.SSPIClient");
      Class[] cArg = new Class[]{PGStream.class, String.class, boolean.class, Logger.class};
      return (ISSPIClient) c.getDeclaredConstructor(cArg)
          .newInstance(pgStream, spnServiceClass, enableNegotiate, logger);
    } catch (Exception e) {
      // This catched quite a lot exceptions, but until Java 7 there is no ReflectiveOperationException
      throw new IllegalStateException("Unable to load org.postgresql.sspi.SSPIClient."
          + " Please check that SSPIClient is included in your pgjdbc distribution.", e);
    }
  }

  public QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user, String database,
      Properties info, Logger logger) throws SQLException {
    // Extract interesting values from the info properties:
    // - the SSL setting
    boolean requireSSL;
    boolean trySSL;
    String sslmode = PGProperty.SSL_MODE.get(info);
    if (sslmode == null) { // Fall back to the ssl property
      // assume "true" if the property is set but empty
      requireSSL = trySSL = PGProperty.SSL.getBoolean(info) || "".equals(PGProperty.SSL.get(info));
    } else {
      if ("disable".equals(sslmode)) {
        requireSSL = trySSL = false;
      } else if ("require".equals(sslmode) || "verify-ca".equals(sslmode)
          || "verify-full".equals(sslmode)) {
        requireSSL = trySSL = true;
      } else {
        throw new PSQLException(GT.tr("Invalid sslmode value: {0}", sslmode),
            PSQLState.CONNECTION_UNABLE_TO_CONNECT);
      }
    }

    // - the TCP keep alive setting
    boolean requireTCPKeepAlive = PGProperty.TCP_KEEP_ALIVE.getBoolean(info);

    // NOTE: To simplify this code, it is assumed that if we are
    // using the V3 protocol, then the database is at least 7.4. That
    // eliminates the need to check database versions and maintain
    // backward-compatible code here.
    //
    // Change by Chris Smith <cdsmith@twu.net>

    int connectTimeout = PGProperty.CONNECT_TIMEOUT.getInt(info) * 1000;

    // - the targetServerType setting
    HostRequirement targetServerType;
    try {
      targetServerType =
          HostRequirement.valueOf(info.getProperty("targetServerType", HostRequirement.any.name()));
    } catch (IllegalArgumentException ex) {
      throw new PSQLException(
          GT.tr("Invalid targetServerType value: {0}", info.getProperty("targetServerType")),
          PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

    SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(info);

    HostChooser hostChooser =
        HostChooserFactory.createHostChooser(hostSpecs, targetServerType, info);
    Iterator<HostSpec> hostIter = hostChooser.iterator();
    while (hostIter.hasNext()) {
      HostSpec hostSpec = hostIter.next();

      if (logger.logDebug()) {
        logger.debug("Trying to establish a protocol version 3 connection to " + hostSpec);
      }

      //
      // Establish a connection.
      //

      PGStream newStream = null;
      try {
        newStream = new PGStream(socketFactory, hostSpec, connectTimeout);

        // Construct and send an ssl startup packet if requested.
        if (trySSL) {
          newStream = enableSSL(newStream, requireSSL, info, logger, connectTimeout);
        }

        // Set the socket timeout if the "socketTimeout" property has been set.
        int socketTimeout = PGProperty.SOCKET_TIMEOUT.getInt(info);
        if (socketTimeout > 0) {
          newStream.getSocket().setSoTimeout(socketTimeout * 1000);
        }

        // Enable TCP keep-alive probe if required.
        newStream.getSocket().setKeepAlive(requireTCPKeepAlive);

        // Try to set SO_SNDBUF and SO_RECVBUF socket options, if requested.
        // If receiveBufferSize and send_buffer_size are set to a value greater
        // than 0, adjust. -1 means use the system default, 0 is ignored since not
        // supported.

        // Set SO_RECVBUF read buffer size
        int receiveBufferSize = PGProperty.RECEIVE_BUFFER_SIZE.getInt(info);
        if (receiveBufferSize > -1) {
          // value of 0 not a valid buffer size value
          if (receiveBufferSize > 0) {
            newStream.getSocket().setReceiveBufferSize(receiveBufferSize);
          } else {
            logger.info("Ignore invalid value for receiveBufferSize: " + receiveBufferSize);
          }
        }

        // Set SO_SNDBUF write buffer size
        int sendBufferSize = PGProperty.SEND_BUFFER_SIZE.getInt(info);
        if (sendBufferSize > -1) {
          if (sendBufferSize > 0) {
            newStream.getSocket().setSendBufferSize(sendBufferSize);
          } else {
            logger.info("Ignore invalid value for sendBufferSize: " + sendBufferSize);
          }
        }

        logger.info("Receive Buffer Size is " + newStream.getSocket().getReceiveBufferSize());
        logger.info("Send Buffer Size is " + newStream.getSocket().getSendBufferSize());

        List<String[]> paramList = new ArrayList<String[]>();
        paramList.add(new String[]{"user", user});
        paramList.add(new String[]{"database", database});
        paramList.add(new String[]{"client_encoding", "UTF8"});
        paramList.add(new String[]{"DateStyle", "ISO"});
        paramList.add(new String[]{"TimeZone", createPostgresTimeZone()});

        Version assumeVersion = ServerVersion.from(PGProperty.ASSUME_MIN_SERVER_VERSION.get(info));

        if (assumeVersion.getVersionNum()
            >= ServerVersion.v9_0.getVersionNum()) {
          // User is explicitly telling us this is a 9.0+ server so set properties here:
          paramList.add(new String[]{"extra_float_digits", "3"});
          String appName = PGProperty.APPLICATION_NAME.get(info);
          if (appName != null) {
            paramList.add(new String[]{"application_name", appName});
          }
        } else {
          // User has not explicitly told us that this is a 9.0+ server so stick to old default:
          paramList.add(new String[]{"extra_float_digits", "2"});
        }

        String replication = PGProperty.REPLICATION.get(info);
        if (replication != null && assumeVersion.getVersionNum() >= ServerVersion.v9_4.getVersionNum()) {
          paramList.add(new String[]{"replication", replication});
        }

        String currentSchema = PGProperty.CURRENT_SCHEMA.get(info);
        if (currentSchema != null) {
          paramList.add(new String[]{"search_path", currentSchema});
        }
        sendStartupPacket(newStream, paramList, logger);

        // Do authentication (until AuthenticationOk).
        doAuthentication(newStream, hostSpec.getHost(), user, info, logger);

        int cancelSignalTimeout = PGProperty.CANCEL_SIGNAL_TIMEOUT.getInt(info) * 1000;

        // Do final startup.
        QueryExecutor queryExecutor = new QueryExecutorImpl(newStream, user, database,
            cancelSignalTimeout, info, logger);

        // Check Master or Slave
        HostStatus hostStatus = HostStatus.ConnectOK;
        if (targetServerType != HostRequirement.any) {
          hostStatus = isMaster(queryExecutor, logger) ? HostStatus.Master : HostStatus.Slave;
        }
        GlobalHostStatusTracker.reportHostStatus(hostSpec, hostStatus);
        if (!targetServerType.allowConnectingTo(hostStatus)) {
          queryExecutor.close();
          if (hostIter.hasNext()) {
            // still more addresses to try
            continue;
          }
          throw new PSQLException(GT
              .tr("Could not find a server with specified targetServerType: {0}", targetServerType),
              PSQLState.CONNECTION_UNABLE_TO_CONNECT);
        }

        runInitialQueries(queryExecutor, info, logger);

        // And we're done.
        return queryExecutor;
      } catch (UnsupportedProtocolException upe) {
        // Swallow this and return null so ConnectionFactory tries the next protocol.
        if (logger.logDebug()) {
          logger.debug("Protocol not supported, abandoning connection.");
        }
        closeStream(newStream);
        return null;
      } catch (ConnectException cex) {
        // Added by Peter Mount <peter@retep.org.uk>
        // ConnectException is thrown when the connection cannot be made.
        // we trap this an return a more meaningful message for the end user
        GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail);
        if (hostIter.hasNext()) {
          // still more addresses to try
          continue;
        }
        throw new PSQLException(GT.tr(
            "Connection to {0} refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
            hostSpec), PSQLState.CONNECTION_UNABLE_TO_CONNECT, cex);
      } catch (IOException ioe) {
        closeStream(newStream);
        GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail);
        if (hostIter.hasNext()) {
          // still more addresses to try
          continue;
        }
        throw new PSQLException(GT.tr("The connection attempt failed."),
            PSQLState.CONNECTION_UNABLE_TO_CONNECT, ioe);
      } catch (SQLException se) {
        closeStream(newStream);
        if (hostIter.hasNext()) {
          // still more addresses to try
          continue;
        }
        throw se;
      }
    }
    throw new PSQLException(GT.tr("The connection url is invalid."),
        PSQLState.CONNECTION_UNABLE_TO_CONNECT);
  }

  /**
   * Convert Java time zone to postgres time zone. All others stay the same except that GMT+nn
   * changes to GMT-nn and vise versa.
   *
   * @return The current JVM time zone in postgresql format.
   */
  private static String createPostgresTimeZone() {
    String tz = TimeZone.getDefault().getID();
    if (tz.length() <= 3 || !tz.startsWith("GMT")) {
      return tz;
    }
    char sign = tz.charAt(3);
    String start;
    if (sign == '+') {
      start = "GMT-";
    } else if (sign == '-') {
      start = "GMT+";
    } else {
      // unknown type
      return tz;
    }

    return start + tz.substring(4);
  }

  private PGStream enableSSL(PGStream pgStream, boolean requireSSL, Properties info, Logger logger,
      int connectTimeout) throws IOException, SQLException {
    if (logger.logDebug()) {
      logger.debug(" FE=> SSLRequest");
    }

    // Send SSL request packet
    pgStream.sendInteger4(8);
    pgStream.sendInteger2(1234);
    pgStream.sendInteger2(5679);
    pgStream.flush();

    // Now get the response from the backend, one of N, E, S.
    int beresp = pgStream.receiveChar();
    switch (beresp) {
      case 'E':
        if (logger.logDebug()) {
          logger.debug(" <=BE SSLError");
        }

        // Server doesn't even know about the SSL handshake protocol
        if (requireSSL) {
          throw new PSQLException(GT.tr("The server does not support SSL."),
              PSQLState.CONNECTION_REJECTED);
        }

        // We have to reconnect to continue.
        pgStream.close();
        return new PGStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), connectTimeout);

      case 'N':
        if (logger.logDebug()) {
          logger.debug(" <=BE SSLRefused");
        }

        // Server does not support ssl
        if (requireSSL) {
          throw new PSQLException(GT.tr("The server does not support SSL."),
              PSQLState.CONNECTION_REJECTED);
        }

        return pgStream;

      case 'S':
        if (logger.logDebug()) {
          logger.debug(" <=BE SSLOk");
        }

        // Server supports ssl
        org.postgresql.ssl.MakeSSL.convert(pgStream, info, logger);
        return pgStream;

      default:
        throw new PSQLException(GT.tr("An error occurred while setting up the SSL connection."),
            PSQLState.PROTOCOL_VIOLATION);
    }
  }

  private void sendStartupPacket(PGStream pgStream, List<String[]> params, Logger logger)
      throws IOException {
    if (logger.logDebug()) {
      StringBuilder details = new StringBuilder();
      for (int i = 0; i < params.size(); ++i) {
        if (i != 0) {
          details.append(", ");
        }
        details.append(params.get(i)[0]);
        details.append("=");
        details.append(params.get(i)[1]);
      }
      logger.debug(" FE=> StartupPacket(" + details + ")");
    }

    // Precalculate message length and encode params.
    int length = 4 + 4;
    byte[][] encodedParams = new byte[params.size() * 2][];
    for (int i = 0; i < params.size(); ++i) {
      encodedParams[i * 2] = params.get(i)[0].getBytes("UTF-8");
      encodedParams[i * 2 + 1] = params.get(i)[1].getBytes("UTF-8");
      length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
    }

    length += 1; // Terminating \0

    // Send the startup message.
    pgStream.sendInteger4(length);
    pgStream.sendInteger2(3); // protocol major
    pgStream.sendInteger2(0); // protocol minor
    for (byte[] encodedParam : encodedParams) {
      pgStream.send(encodedParam);
      pgStream.sendChar(0);
    }

    pgStream.sendChar(0);
    pgStream.flush();
  }

  private void doAuthentication(PGStream pgStream, String host, String user, Properties info,
      Logger logger) throws IOException, SQLException {
    // Now get the response from the backend, either an error message
    // or an authentication request

    String password = PGProperty.PASSWORD.get(info);

    /* SSPI negotiation state, if used */
    ISSPIClient sspiClient = null;

    try {
      authloop: while (true) {
        int beresp = pgStream.receiveChar();

        switch (beresp) {
          case 'E':
            // An error occurred, so pass the error message to the
            // user.
            //
            // The most common one to be thrown here is:
            // "User authentication failed"
            //
            int l_elen = pgStream.receiveInteger4();
            if (l_elen > 30000) {
              // if the error length is > than 30000 we assume this is really a v2 protocol
              // server, so trigger fallback.
              throw new UnsupportedProtocolException();
            }

            ServerErrorMessage errorMsg =
                new ServerErrorMessage(pgStream.receiveErrorString(l_elen - 4), logger.getLogLevel());
            if (logger.logDebug()) {
              logger.debug(" <=BE ErrorMessage(" + errorMsg + ")");
            }
            throw new PSQLException(errorMsg);

          case 'R':
            // Authentication request.
            // Get the message length
            int l_msgLen = pgStream.receiveInteger4();

            // Get the type of request
            int areq = pgStream.receiveInteger4();

            // Process the request.
            switch (areq) {
              case AUTH_REQ_CRYPT: {
                byte[] salt = pgStream.receive(2);

                if (logger.logDebug()) {
                  logger.debug(
                      " <=BE AuthenticationReqCrypt(salt='" + new String(salt, "US-ASCII") + "')");
                }

                if (password == null) {
                  throw new PSQLException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      PSQLState.CONNECTION_REJECTED);
                }

                byte[] encodedResult = UnixCrypt.crypt(salt, password.getBytes("UTF-8"));

                if (logger.logDebug()) {
                  logger.debug(
                      " FE=> Password(crypt='" + new String(encodedResult, "US-ASCII") + "')");
                }

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + encodedResult.length + 1);
                pgStream.send(encodedResult);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_MD5: {
                byte[] md5Salt = pgStream.receive(4);
                if (logger.logDebug()) {
                  logger
                      .debug(" <=BE AuthenticationReqMD5(salt=" + Utils.toHexString(md5Salt) + ")");
                }

                if (password == null) {
                  throw new PSQLException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      PSQLState.CONNECTION_REJECTED);
                }

                byte[] digest =
                    MD5Digest.encode(user.getBytes("UTF-8"), password.getBytes("UTF-8"), md5Salt);

                if (logger.logDebug()) {
                  logger.debug(" FE=> Password(md5digest=" + new String(digest, "US-ASCII") + ")");
                }

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + digest.length + 1);
                pgStream.send(digest);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_PASSWORD: {
                if (logger.logDebug()) {
                  logger.debug(" <=BE AuthenticationReqPassword");
                  logger.debug(" FE=> Password(password=<not shown>)");
                }

                if (password == null) {
                  throw new PSQLException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      PSQLState.CONNECTION_REJECTED);
                }

                byte[] encodedPassword = password.getBytes("UTF-8");

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + encodedPassword.length + 1);
                pgStream.send(encodedPassword);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_GSS:
              case AUTH_REQ_SSPI:
                /*
                 * Use GSSAPI if requested on all platforms, via JSSE.
                 *
                 * For SSPI auth requests, if we're on Windows attempt native SSPI authentication if
                 * available, and if not disabled by setting a kerberosServerName. On other
                 * platforms, attempt JSSE GSSAPI negotiation with the SSPI server.
                 *
                 * Note that this is slightly different to libpq, which uses SSPI for GSSAPI where
                 * supported. We prefer to use the existing Java JSSE Kerberos support rather than
                 * going to native (via JNA) calls where possible, so that JSSE system properties
                 * etc continue to work normally.
                 *
                 * Note that while SSPI is often Kerberos-based there's no guarantee it will be; it
                 * may be NTLM or anything else. If the client responds to an SSPI request via
                 * GSSAPI and the other end isn't using Kerberos for SSPI then authentication will
                 * fail.
                 */
                final String gsslib = PGProperty.GSS_LIB.get(info);
                final boolean usespnego = PGProperty.USE_SPNEGO.getBoolean(info);

                boolean useSSPI = false;

                /*
                 * Use SSPI if we're in auto mode on windows and have a request for SSPI auth, or if
                 * it's forced. Otherwise use gssapi. If the user has specified a Kerberos server
                 * name we'll always use JSSE GSSAPI.
                 */
                if (gsslib.equals("gssapi")) {
                  logger.debug("Using JSSE GSSAPI, param gsslib=gssapi");
                } else if (areq == AUTH_REQ_GSS && !gsslib.equals("sspi")) {
                  logger.debug(
                      "Using JSSE GSSAPI, gssapi requested by server and gsslib=sspi not forced");
                } else {
                  /* Determine if SSPI is supported by the client */
                  sspiClient = createSSPI(pgStream, PGProperty.SSPI_SERVICE_CLASS.get(info),
                      /* Use negotiation for SSPI, or if explicitly requested for GSS */
                      areq == AUTH_REQ_SSPI || (areq == AUTH_REQ_GSS && usespnego), logger);

                  useSSPI = sspiClient.isSSPISupported();
                  if (logger.logDebug()) {
                    logger.debug("SSPI support detected: " + useSSPI);
                  }

                  if (!useSSPI) {
                    /* No need to dispose() if no SSPI used */
                    sspiClient = null;

                    if (gsslib.equals("sspi")) {
                      throw new PSQLException(
                          "SSPI forced with gsslib=sspi, but SSPI not available; set loglevel=2 for details",
                          PSQLState.CONNECTION_UNABLE_TO_CONNECT);
                    }
                  }

                  if (logger.logDebug()) {
                    logger.debug("Using SSPI: " + useSSPI + ", gsslib=" + gsslib
                        + " and SSPI support detected");
                  }
                }

                if (useSSPI) {
                  /* SSPI requested and detected as available */
                  sspiClient.startSSPI();
                } else {
                  /* Use JGSS's GSSAPI for this request */
                  org.postgresql.gss.MakeGSS.authenticate(pgStream, host, user, password,
                      PGProperty.JAAS_APPLICATION_NAME.get(info),
                      PGProperty.KERBEROS_SERVER_NAME.get(info), logger, usespnego);
                }

                break;

              case AUTH_REQ_GSS_CONTINUE:
                /*
                 * Only called for SSPI, as GSS is handled by an inner loop in MakeGSS.
                 */
                sspiClient.continueSSPI(l_msgLen - 8);
                break;

              case AUTH_REQ_OK:
                /* Cleanup after successful authentication */
                if (logger.logDebug()) {
                  logger.debug(" <=BE AuthenticationOk");
                }

                break authloop; // We're done.

              default:
                if (logger.logDebug()) {
                  logger.debug(" <=BE AuthenticationReq (unsupported type " + (areq) + ")");
                }

                throw new PSQLException(GT.tr(
                    "The authentication type {0} is not supported. Check that you have configured the pg_hba.conf file to include the client''s IP address or subnet, and that it is using an authentication scheme supported by the driver.",
                    areq), PSQLState.CONNECTION_REJECTED);
            }

            break;

          default:
            throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                PSQLState.PROTOCOL_VIOLATION);
        }
      }
    } finally {
      /* Cleanup after successful or failed authentication attempts */
      if (sspiClient != null) {
        try {
          sspiClient.dispose();
        } catch (RuntimeException ex) {
          logger.log("Unexpected error during SSPI context disposal", ex);
        }

      }
    }

  }

  private void runInitialQueries(QueryExecutor queryExecutor, Properties info, Logger logger)
      throws SQLException {
    String assumeMinServerVersion = PGProperty.ASSUME_MIN_SERVER_VERSION.get(info);
    if (Utils.parseServerVersionStr(assumeMinServerVersion) >= ServerVersion.v9_0.getVersionNum()) {
      // We already sent the parameter values in the StartupMessage so skip this
      return;
    }

    final int dbVersion = queryExecutor.getServerVersionNum();

    if (dbVersion >= ServerVersion.v9_0.getVersionNum()) {
      SetupQueryRunner.run(queryExecutor, "SET extra_float_digits = 3", false);
    }

    String appName = PGProperty.APPLICATION_NAME.get(info);
    if (appName != null && dbVersion >= ServerVersion.v9_0.getVersionNum()) {
      StringBuilder sql = new StringBuilder();
      sql.append("SET application_name = '");
      Utils.escapeLiteral(sql, appName, queryExecutor.getStandardConformingStrings());
      sql.append("'");
      SetupQueryRunner.run(queryExecutor, sql.toString(), false);
    }

  }

  private boolean isMaster(QueryExecutor queryExecutor, Logger logger)
      throws SQLException, IOException {
    byte[][] results = SetupQueryRunner.run(queryExecutor, "show transaction_read_only", true);
    String value = queryExecutor.getEncoding().decode(results[0]);
    return value.equalsIgnoreCase("off");
  }
}
