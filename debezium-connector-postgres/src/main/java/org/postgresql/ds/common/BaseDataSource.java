/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ds.common;

import org.postgresql.PGProperty;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * Base class for data sources and related classes.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public abstract class BaseDataSource implements Referenceable {
  // Load the normal driver, since we'll use it to actually connect to the
  // database. That way we don't have to maintain the connecting code in
  // multiple places.
  static {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      System.err.println("PostgreSQL DataSource unable to load PostgreSQL JDBC Driver");
    }
  }

  // Needed to implement the DataSource/ConnectionPoolDataSource interfaces
  private transient PrintWriter logger;

  // Standard properties, defined in the JDBC 2.0 Optional Package spec
  private String serverName = "localhost";
  private String databaseName = "";
  private String user;
  private String password;
  private int portNumber = 0;

  // Map for all other properties
  private Properties properties = new Properties();

  /**
   * Gets a connection to the PostgreSQL database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the DataSource properties user and password.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public Connection getConnection() throws SQLException {
    return getConnection(user, password);
  }

  /**
   * Gets a connection to the PostgreSQL database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the arguments user and password, which override the DataSource properties by the same name.
   *
   * @param user user
   * @param password password
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public Connection getConnection(String user, String password) throws SQLException {
    try {
      Connection con = DriverManager.getConnection(getUrl(), user, password);
      if (logger != null) {
        logger.println("Created a non-pooled connection for " + user + " at " + getUrl());
      }
      return con;
    } catch (SQLException e) {
      if (logger != null) {
        logger.println(
            "Failed to create a non-pooled connection for " + user + " at " + getUrl() + ": " + e);
      }
      throw e;
    }
  }

  /**
   * Gets the log writer used to log connections opened.
   *
   * @return log writer used to log connections opened
   */
  public PrintWriter getLogWriter() {
    return logger;
  }

  /**
   * The DataSource will note every connection opened to the provided log writer.
   *
   * @param printWriter log writer used to log connections opened
   */
  public void setLogWriter(PrintWriter printWriter) {
    logger = printWriter;
  }

  /**
   * Gets the name of the host the PostgreSQL database is running on.
   *
   * @return name of the host the PostgreSQL database is running on
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Sets the name of the host the PostgreSQL database is running on. If this is changed, it will
   * only affect future calls to getConnection. The default value is <tt>localhost</tt>.
   *
   * @param serverName name of the host the PostgreSQL database is running on
   */
  public void setServerName(String serverName) {
    if (serverName == null || serverName.equals("")) {
      this.serverName = "localhost";
    } else {
      this.serverName = serverName;
    }
  }

  /**
   * Gets the name of the PostgreSQL database, running on the server identified by the serverName
   * property.
   *
   * @return name of the PostgreSQL database
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Sets the name of the PostgreSQL database, running on the server identified by the serverName
   * property. If this is changed, it will only affect future calls to getConnection.
   *
   * @param databaseName name of the PostgreSQL database
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Gets a description of this DataSource-ish thing. Must be customized by subclasses.
   *
   * @return description of this DataSource-ish thing
   */
  public abstract String getDescription();

  /**
   * Gets the user to connect as by default. If this is not specified, you must use the
   * getConnection method which takes a user and password as parameters.
   *
   * @return user to connect as by default
   */
  public String getUser() {
    return user;
  }

  /**
   * Sets the user to connect as by default. If this is not specified, you must use the
   * getConnection method which takes a user and password as parameters. If this is changed, it will
   * only affect future calls to getConnection.
   *
   * @param user user to connect as by default
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * Gets the password to connect with by default. If this is not specified but a password is needed
   * to log in, you must use the getConnection method which takes a user and password as parameters.
   *
   * @return password to connect with by default
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password to connect with by default. If this is not specified but a password is needed
   * to log in, you must use the getConnection method which takes a user and password as parameters.
   * If this is changed, it will only affect future calls to getConnection.
   *
   * @param password password to connect with by default
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets the port which the PostgreSQL server is listening on for TCP/IP connections.
   *
   * @return The port, or 0 if the default port will be used.
   */
  public int getPortNumber() {
    return portNumber;
  }

  /**
   * Gets the port which the PostgreSQL server is listening on for TCP/IP connections. Be sure the
   * -i flag is passed to postmaster when PostgreSQL is started. If this is not set, or set to 0,
   * the default port will be used.
   *
   * @param portNumber port which the PostgreSQL server is listening on for TCP/IP
   */
  public void setPortNumber(int portNumber) {
    this.portNumber = portNumber;
  }

  /**
   * @return value of compatible parameter
   * @see PGProperty#COMPATIBLE
   */
  public String getCompatible() {
    return PGProperty.COMPATIBLE.get(properties);
  }

  /**
   * @param compatible value of compatible parameter
   * @see PGProperty#COMPATIBLE
   */
  public void setCompatible(String compatible) {
    PGProperty.COMPATIBLE.set(properties, compatible);
  }

  /**
   * @return login timeout
   * @see PGProperty#LOGIN_TIMEOUT
   */
  public int getLoginTimeout() {
    return PGProperty.LOGIN_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param loginTimeout login timeout
   * @see PGProperty#LOGIN_TIMEOUT
   */
  public void setLoginTimeout(int loginTimeout) {
    PGProperty.LOGIN_TIMEOUT.set(properties, loginTimeout);
  }

  /**
   * @return connect timeout
   * @see PGProperty#CONNECT_TIMEOUT
   */
  public int getConnectTimeout() {
    return PGProperty.CONNECT_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param connectTimeout connect timeout
   * @see PGProperty#CONNECT_TIMEOUT
   */
  public void setConnectTimeout(int connectTimeout) {
    PGProperty.CONNECT_TIMEOUT.set(properties, connectTimeout);
  }

  /**
   * @return log level
   * @see PGProperty#LOG_LEVEL
   */
  public int getLogLevel() {
    return PGProperty.LOG_LEVEL.getIntNoCheck(properties);
  }

  /**
   * @param logLevel log level
   * @see PGProperty#LOG_LEVEL
   */
  public void setLogLevel(int logLevel) {
    PGProperty.LOG_LEVEL.set(properties, logLevel);
  }

  /**
   * @return protocol version
   * @see PGProperty#PROTOCOL_VERSION
   */
  public int getProtocolVersion() {
    if (!PGProperty.PROTOCOL_VERSION.isPresent(properties)) {
      return 0;
    } else {
      return PGProperty.PROTOCOL_VERSION.getIntNoCheck(properties);
    }
  }

  /**
   * @param protocolVersion protocol version
   * @see PGProperty#PROTOCOL_VERSION
   */
  public void setProtocolVersion(int protocolVersion) {
    if (protocolVersion == 0) {
      PGProperty.PROTOCOL_VERSION.set(properties, null);
    } else {
      PGProperty.PROTOCOL_VERSION.set(properties, protocolVersion);
    }
  }

  /**
   * @return receive buffer size
   * @see PGProperty#RECEIVE_BUFFER_SIZE
   */
  public int getReceiveBufferSize() {
    return PGProperty.RECEIVE_BUFFER_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param nbytes receive buffer size
   * @see PGProperty#RECEIVE_BUFFER_SIZE
   */
  public void setReceiveBufferSize(int nbytes) {
    PGProperty.RECEIVE_BUFFER_SIZE.set(properties, nbytes);
  }

  /**
   * @return send buffer size
   * @see PGProperty#SEND_BUFFER_SIZE
   */
  public int getSendBufferSize() {
    return PGProperty.SEND_BUFFER_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param nbytes send buffer size
   * @see PGProperty#SEND_BUFFER_SIZE
   */
  public void setSendBufferSize(int nbytes) {
    PGProperty.SEND_BUFFER_SIZE.set(properties, nbytes);
  }

  /**
   * @param count prepare threshold
   * @see PGProperty#PREPARE_THRESHOLD
   */
  public void setPrepareThreshold(int count) {
    PGProperty.PREPARE_THRESHOLD.set(properties, count);
  }

  /**
   * @return prepare threshold
   * @see PGProperty#PREPARE_THRESHOLD
   */
  public int getPrepareThreshold() {
    return PGProperty.PREPARE_THRESHOLD.getIntNoCheck(properties);
  }

  /**
   * @return prepared statement cache size (number of statements per connection)
   * @see PGProperty#PREPARED_STATEMENT_CACHE_QUERIES
   */
  public int getPreparedStatementCacheQueries() {
    return PGProperty.PREPARED_STATEMENT_CACHE_QUERIES.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize prepared statement cache size (number of statements per connection)
   * @see PGProperty#PREPARED_STATEMENT_CACHE_QUERIES
   */
  public void setPreparedStatementCacheQueries(int cacheSize) {
    PGProperty.PREPARED_STATEMENT_CACHE_QUERIES.set(properties, cacheSize);
  }

  /**
   * @return prepared statement cache size (number of megabytes per connection)
   * @see PGProperty#PREPARED_STATEMENT_CACHE_SIZE_MIB
   */
  public int getPreparedStatementCacheSizeMiB() {
    return PGProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize statement cache size (number of megabytes per connection)
   * @see PGProperty#PREPARED_STATEMENT_CACHE_SIZE_MIB
   */
  public void setPreparedStatementCacheSizeMiB(int cacheSize) {
    PGProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.set(properties, cacheSize);
  }

  /**
   * @return database metadata cache fields size (number of fields cached per connection)
   * @see PGProperty#DATABASE_METADATA_CACHE_FIELDS
   */
  public int getDatabaseMetadataCacheFields() {
    return PGProperty.DATABASE_METADATA_CACHE_FIELDS.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize database metadata cache fields size (number of fields cached per connection)
   * @see PGProperty#DATABASE_METADATA_CACHE_FIELDS
   */
  public void setDatabaseMetadataCacheFields(int cacheSize) {
    PGProperty.DATABASE_METADATA_CACHE_FIELDS.set(properties, cacheSize);
  }

  /**
   * @return database metadata cache fields size (number of megabytes per connection)
   * @see PGProperty#DATABASE_METADATA_CACHE_FIELDS_MIB
   */
  public int getDatabaseMetadataCacheFieldsMiB() {
    return PGProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize database metadata cache fields size (number of megabytes per connection)
   * @see PGProperty#DATABASE_METADATA_CACHE_FIELDS_MIB
   */
  public void setDatabaseMetadataCacheFieldsMiB(int cacheSize) {
    PGProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.set(properties, cacheSize);
  }

  /**
   * @param fetchSize default fetch size
   * @see PGProperty#DEFAULT_ROW_FETCH_SIZE
   */
  public void setDefaultRowFetchSize(int fetchSize) {
    PGProperty.DEFAULT_ROW_FETCH_SIZE.set(properties, fetchSize);
  }

  /**
   * @return default fetch size
   * @see PGProperty#DEFAULT_ROW_FETCH_SIZE
   */
  public int getDefaultRowFetchSize() {
    return PGProperty.DEFAULT_ROW_FETCH_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param unknownLength unknown length
   * @see PGProperty#UNKNOWN_LENGTH
   */
  public void setUnknownLength(int unknownLength) {
    PGProperty.UNKNOWN_LENGTH.set(properties, unknownLength);
  }

  /**
   * @return unknown length
   * @see PGProperty#UNKNOWN_LENGTH
   */
  public int getUnknownLength() {
    return PGProperty.UNKNOWN_LENGTH.getIntNoCheck(properties);
  }

  /**
   * @param seconds socket timeout
   * @see PGProperty#SOCKET_TIMEOUT
   */
  public void setSocketTimeout(int seconds) {
    PGProperty.SOCKET_TIMEOUT.set(properties, seconds);
  }

  /**
   * @return socket timeout
   * @see PGProperty#SOCKET_TIMEOUT
   */
  public int getSocketTimeout() {
    return PGProperty.SOCKET_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param seconds timeout that is used for sending cancel command
   * @see PGProperty#CANCEL_SIGNAL_TIMEOUT
   */
  public void setCancelSignalTimeout(int seconds) {
    PGProperty.CANCEL_SIGNAL_TIMEOUT.set(properties, seconds);
  }

  /**
   * @return timeout that is used for sending cancel command in seconds
   * @see PGProperty#CANCEL_SIGNAL_TIMEOUT
   */
  public int getCancelSignalTimeout() {
    return PGProperty.CANCEL_SIGNAL_TIMEOUT.getIntNoCheck(properties);
  }


  /**
   * @param enabled if SSL is enabled
   * @see PGProperty#SSL
   */
  public void setSsl(boolean enabled) {
    if (enabled) {
      PGProperty.SSL.set(properties, true);
    } else {
      PGProperty.SSL.set(properties, false);
    }
  }

  /**
   * @return true if SSL is enabled
   * @see PGProperty#SSL
   */
  public boolean getSsl() {
    // "true" if "ssl" is set but empty
    return PGProperty.SSL.getBoolean(properties) || "".equals(PGProperty.SSL.get(properties));
  }

  /**
   * @param classname SSL factory class name
   * @see PGProperty#SSL_FACTORY
   */
  public void setSslfactory(String classname) {
    PGProperty.SSL_FACTORY.set(properties, classname);
  }

  /**
   * @return SSL factory class name
   * @see PGProperty#SSL_FACTORY
   */
  public String getSslfactory() {
    return PGProperty.SSL_FACTORY.get(properties);
  }

  /**
   * @return SSL mode
   * @see PGProperty#SSL_MODE
   */
  public String getSslMode() {
    return PGProperty.SSL_MODE.get(properties);
  }

  /**
   * @param mode SSL mode
   * @see PGProperty#SSL_MODE
   */
  public void setSslMode(String mode) {
    PGProperty.SSL_MODE.set(properties, mode);
  }

  /**
   * @return SSL mode
   * @see PGProperty#SSL_FACTORY_ARG
   */
  public String getSslFactoryArg() {
    return PGProperty.SSL_FACTORY_ARG.get(properties);
  }

  /**
   * @param arg argument forwarded to SSL factory
   * @see PGProperty#SSL_FACTORY_ARG
   */
  public void setSslFactoryArg(String arg) {
    PGProperty.SSL_FACTORY_ARG.set(properties, arg);
  }

  /**
   * @return argument forwarded to SSL factory
   * @see PGProperty#SSL_HOSTNAME_VERIFIER
   */
  public String getSslHostnameVerifier() {
    return PGProperty.SSL_HOSTNAME_VERIFIER.get(properties);
  }

  /**
   * @param className SSL hostname verifier
   * @see PGProperty#SSL_HOSTNAME_VERIFIER
   */
  public void setSslHostnameVerifier(String className) {
    PGProperty.SSL_HOSTNAME_VERIFIER.set(properties, className);
  }

  /**
   * @return className SSL hostname verifier
   * @see PGProperty#SSL_CERT
   */
  public String getSslCert() {
    return PGProperty.SSL_CERT.get(properties);
  }

  /**
   * @param file SSL certificate
   * @see PGProperty#SSL_CERT
   */
  public void setSslCert(String file) {
    PGProperty.SSL_CERT.set(properties, file);
  }

  /**
   * @return SSL certificate
   * @see PGProperty#SSL_KEY
   */
  public String getSslKey() {
    return PGProperty.SSL_KEY.get(properties);
  }

  /**
   * @param file SSL key
   * @see PGProperty#SSL_KEY
   */
  public void setSslKey(String file) {
    PGProperty.SSL_KEY.set(properties, file);
  }

  /**
   * @return SSL root certificate
   * @see PGProperty#SSL_ROOT_CERT
   */
  public String getSslRootCert() {
    return PGProperty.SSL_ROOT_CERT.get(properties);
  }

  /**
   * @param file SSL root certificate
   * @see PGProperty#SSL_ROOT_CERT
   */
  public void setSslRootCert(String file) {
    PGProperty.SSL_ROOT_CERT.set(properties, file);
  }

  /**
   * @return SSL password
   * @see PGProperty#SSL_PASSWORD
   */
  public String getSslPassword() {
    return PGProperty.SSL_PASSWORD.get(properties);
  }

  /**
   * @param password SSL password
   * @see PGProperty#SSL_PASSWORD
   */
  public void setSslPassword(String password) {
    PGProperty.SSL_PASSWORD.set(properties, password);
  }

  /**
   * @return SSL password callback
   * @see PGProperty#SSL_PASSWORD_CALLBACK
   */
  public String getSslPasswordCallback() {
    return PGProperty.SSL_PASSWORD_CALLBACK.get(properties);
  }

  /**
   * @param className SSL password callback class name
   * @see PGProperty#SSL_PASSWORD_CALLBACK
   */
  public void setSslPasswordCallback(String className) {
    PGProperty.SSL_PASSWORD_CALLBACK.set(properties, className);
  }

  /**
   * @param applicationName application name
   * @see PGProperty#APPLICATION_NAME
   */
  public void setApplicationName(String applicationName) {
    PGProperty.APPLICATION_NAME.set(properties, applicationName);
  }

  /**
   * @return application name
   * @see PGProperty#APPLICATION_NAME
   */
  public String getApplicationName() {
    return PGProperty.APPLICATION_NAME.get(properties);
  }

  /**
   * @param targetServerType target server type
   * @see PGProperty#TARGET_SERVER_TYPE
   */
  public void setTargetServerType(String targetServerType) {
    PGProperty.TARGET_SERVER_TYPE.set(properties, targetServerType);
  }

  /**
   * @return target server type
   * @see PGProperty#TARGET_SERVER_TYPE
   */
  public String getTargetServerType() {
    return PGProperty.TARGET_SERVER_TYPE.get(properties);
  }

  /**
   * @param loadBalanceHosts load balance hosts
   * @see PGProperty#LOAD_BALANCE_HOSTS
   */
  public void setLoadBalanceHosts(boolean loadBalanceHosts) {
    PGProperty.LOAD_BALANCE_HOSTS.set(properties, loadBalanceHosts);
  }

  /**
   * @return load balance hosts
   * @see PGProperty#LOAD_BALANCE_HOSTS
   */
  public boolean getLoadBalanceHosts() {
    return PGProperty.LOAD_BALANCE_HOSTS.isPresent(properties);
  }

  /**
   * @param hostRecheckSeconds host recheck seconds
   * @see PGProperty#HOST_RECHECK_SECONDS
   */
  public void setHostRecheckSeconds(int hostRecheckSeconds) {
    PGProperty.HOST_RECHECK_SECONDS.set(properties, hostRecheckSeconds);
  }

  /**
   * @return host recheck seconds
   * @see PGProperty#HOST_RECHECK_SECONDS
   */
  public int getHostRecheckSeconds() {
    return PGProperty.HOST_RECHECK_SECONDS.getIntNoCheck(properties);
  }

  /**
   * @param enabled if TCP keep alive should be enabled
   * @see PGProperty#TCP_KEEP_ALIVE
   */
  public void setTcpKeepAlive(boolean enabled) {
    PGProperty.TCP_KEEP_ALIVE.set(properties, enabled);
  }

  /**
   * @return true if TCP keep alive is enabled
   * @see PGProperty#TCP_KEEP_ALIVE
   */
  public boolean getTcpKeepAlive() {
    return PGProperty.TCP_KEEP_ALIVE.getBoolean(properties);
  }

  /**
   * @param enabled if binary transfer should be enabled
   * @see PGProperty#BINARY_TRANSFER
   */
  public void setBinaryTransfer(boolean enabled) {
    PGProperty.BINARY_TRANSFER.set(properties, enabled);
  }

  /**
   * @return true if binary transfer is enabled
   * @see PGProperty#BINARY_TRANSFER
   */
  public boolean getBinaryTransfer() {
    return PGProperty.BINARY_TRANSFER.getBoolean(properties);
  }

  /**
   * @param oidList list of OIDs that are allowed to use binary transfer
   * @see PGProperty#BINARY_TRANSFER_ENABLE
   */
  public void setBinaryTransferEnable(String oidList) {
    PGProperty.BINARY_TRANSFER_ENABLE.set(properties, oidList);
  }

  /**
   * @return list of OIDs that are allowed to use binary transfer
   * @see PGProperty#BINARY_TRANSFER_ENABLE
   */
  public String getBinaryTransferEnable() {
    return PGProperty.BINARY_TRANSFER_ENABLE.get(properties);
  }

  /**
   * @param oidList list of OIDs that are not allowed to use binary transfer
   * @see PGProperty#BINARY_TRANSFER_DISABLE
   */
  public void setBinaryTransferDisable(String oidList) {
    PGProperty.BINARY_TRANSFER_DISABLE.set(properties, oidList);
  }

  /**
   * @return list of OIDs that are not allowed to use binary transfer
   * @see PGProperty#BINARY_TRANSFER_DISABLE
   */
  public String getBinaryTransferDisable() {
    return PGProperty.BINARY_TRANSFER_DISABLE.get(properties);
  }

  /**
   * @return string type
   * @see PGProperty#STRING_TYPE
   */
  public String getStringType() {
    return PGProperty.STRING_TYPE.get(properties);
  }

  /**
   * @param stringType string type
   * @see PGProperty#STRING_TYPE
   */
  public void setStringType(String stringType) {
    PGProperty.STRING_TYPE.set(properties, stringType);
  }

  /**
   * @return true if column sanitizer is disabled
   * @see PGProperty#DISABLE_COLUMN_SANITISER
   */
  public boolean isColumnSanitiserDisabled() {
    return PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(properties);
  }

  /**
   * @return true if column sanitizer is disabled
   * @see PGProperty#DISABLE_COLUMN_SANITISER
   */
  public boolean getDisableColumnSanitiser() {
    return PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(properties);
  }

  /**
   * @param disableColumnSanitiser if column sanitizer should be disabled
   * @see PGProperty#DISABLE_COLUMN_SANITISER
   */
  public void setDisableColumnSanitiser(boolean disableColumnSanitiser) {
    PGProperty.DISABLE_COLUMN_SANITISER.set(properties, disableColumnSanitiser);
  }

  /**
   * @return current schema
   * @see PGProperty#CURRENT_SCHEMA
   */
  public String getCurrentSchema() {
    return PGProperty.CURRENT_SCHEMA.get(properties);
  }

  /**
   * @param currentSchema current schema
   * @see PGProperty#CURRENT_SCHEMA
   */
  public void setCurrentSchema(String currentSchema) {
    PGProperty.CURRENT_SCHEMA.set(properties, currentSchema);
  }

  /**
   * @return true if connection is readonly
   * @see PGProperty#READ_ONLY
   */
  public boolean getReadOnly() {
    return PGProperty.READ_ONLY.getBoolean(properties);
  }

  /**
   * @param readOnly if connection should be readonly
   * @see PGProperty#READ_ONLY
   */
  public void setReadOnly(boolean readOnly) {
    PGProperty.READ_ONLY.set(properties, readOnly);
  }

  /**
   * @return true if driver should log unclosed connections
   * @see PGProperty#LOG_UNCLOSED_CONNECTIONS
   */
  public boolean getLogUnclosedConnections() {
    return PGProperty.LOG_UNCLOSED_CONNECTIONS.getBoolean(properties);
  }

  /**
   * @param enabled true if driver should log unclosed connections
   * @see PGProperty#LOG_UNCLOSED_CONNECTIONS
   */
  public void setLogUnclosedConnections(boolean enabled) {
    PGProperty.LOG_UNCLOSED_CONNECTIONS.set(properties, enabled);
  }

  /**
   * @return assumed minimal server version
   * @see PGProperty#ASSUME_MIN_SERVER_VERSION
   */
  public String getAssumeMinServerVersion() {
    return PGProperty.ASSUME_MIN_SERVER_VERSION.get(properties);
  }

  /**
   * @param minVersion assumed minimal server version
   * @see PGProperty#ASSUME_MIN_SERVER_VERSION
   */
  public void setAssumeMinServerVersion(String minVersion) {
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, minVersion);
  }

  /**
   * @return JAAS application name
   * @see PGProperty#JAAS_APPLICATION_NAME
   */
  public String getJaasApplicationName() {
    return PGProperty.JAAS_APPLICATION_NAME.get(properties);
  }

  /**
   * @param name JAAS application name
   * @see PGProperty#JAAS_APPLICATION_NAME
   */
  public void setJaasApplicationName(String name) {
    PGProperty.JAAS_APPLICATION_NAME.set(properties, name);
  }

  /**
   * @return Kerberos server name
   * @see PGProperty#KERBEROS_SERVER_NAME
   */
  public String getKerberosServerName() {
    return PGProperty.KERBEROS_SERVER_NAME.get(properties);
  }

  /**
   * @param serverName Kerberos server name
   * @see PGProperty#KERBEROS_SERVER_NAME
   */
  public void setKerberosServerName(String serverName) {
    PGProperty.KERBEROS_SERVER_NAME.set(properties, serverName);
  }

  /**
   * @return true if use SPNEGO
   * @see PGProperty#USE_SPNEGO
   */
  public boolean getUseSpNego() {
    return PGProperty.USE_SPNEGO.getBoolean(properties);
  }

  /**
   * @param use true if use SPNEGO
   * @see PGProperty#USE_SPNEGO
   */
  public void setUseSpNego(boolean use) {
    PGProperty.USE_SPNEGO.set(properties, use);
  }

  /**
   * @return GSS mode: auto, sspi, or gssapi
   * @see PGProperty#GSS_LIB
   */
  public String getGssLib() {
    return PGProperty.GSS_LIB.get(properties);
  }

  /**
   * @param lib GSS mode: auto, sspi, or gssapi
   * @see PGProperty#GSS_LIB
   */
  public void setGssLib(String lib) {
    PGProperty.GSS_LIB.set(properties, lib);
  }

  /**
   * @return SSPI service class
   * @see PGProperty#SSPI_SERVICE_CLASS
   */
  public String getSspiServiceClass() {
    return PGProperty.SSPI_SERVICE_CLASS.get(properties);
  }

  /**
   * @param serviceClass SSPI service class
   * @see PGProperty#SSPI_SERVICE_CLASS
   */
  public void setSspiServiceClass(String serviceClass) {
    PGProperty.SSPI_SERVICE_CLASS.set(properties, serviceClass);
  }

  /**
   * @return character set to use for data sent to the database or received
   * @see PGProperty#CHARSET
   */
  public String getCharset() {
    return PGProperty.CHARSET.get(properties);
  }

  /**
   * @param charset character set to use for data sent to the database or received
   * @see PGProperty#CHARSET
   */
  public void setCharset(String charset) {
    PGProperty.CHARSET.set(properties, charset);
  }

  /**
   * @return if connection allows encoding changes
   * @see PGProperty#ALLOW_ENCODING_CHANGES
   */
  public boolean getAllowEncodingChanges() {
    return PGProperty.ALLOW_ENCODING_CHANGES.getBoolean(properties);
  }

  /**
   * @param allow if connection allows encoding changes
   * @see PGProperty#ALLOW_ENCODING_CHANGES
   */
  public void setAllowEncodingChanges(boolean allow) {
    PGProperty.ALLOW_ENCODING_CHANGES.set(properties, allow);
  }

  /**
   * @return socket factory class name
   * @see PGProperty#SOCKET_FACTORY
   */
  public String getSocketFactory() {
    return PGProperty.SOCKET_FACTORY.get(properties);
  }

  /**
   * @param socketFactoryClassName socket factory class name
   * @see PGProperty#SOCKET_FACTORY
   */
  public void setSocketFactory(String socketFactoryClassName) {
    PGProperty.SOCKET_FACTORY.set(properties, socketFactoryClassName);
  }

  /**
   * @return socket factory argument
   * @see PGProperty#SOCKET_FACTORY_ARG
   */
  public String getSocketFactoryArg() {
    return PGProperty.SOCKET_FACTORY_ARG.get(properties);
  }

  /**
   * @param socketFactoryArg socket factory argument
   * @see PGProperty#SOCKET_FACTORY_ARG
   */
  public void setSocketFactoryArg(String socketFactoryArg) {
    PGProperty.SOCKET_FACTORY_ARG.set(properties, socketFactoryArg);
  }

  /**
   * @param replication replication argument
   * @see PGProperty#REPLICATION
   */
  public void setReplication(String replication) {
    PGProperty.REPLICATION.set(properties, replication);
  }

  /**
   * @see PGProperty#REPLICATION
   */
  public String getReplication() {
    return PGProperty.REPLICATION.get(properties);
  }

  /**
   * Generates a {@link DriverManager} URL from the other properties supplied.
   *
   * @return {@link DriverManager} URL from the other properties supplied
   */
  public String getUrl() {
    StringBuilder url = new StringBuilder(100);
    url.append("jdbc:postgresql://");
    url.append(serverName);
    if (portNumber != 0) {
      url.append(":").append(portNumber);
    }
    url.append("/").append(databaseName);

    StringBuilder query = new StringBuilder(100);
    for (PGProperty property : PGProperty.values()) {
      if (property.isPresent(properties)) {
        if (query.length() != 0) {
          query.append("&");
        }
        query.append(property.getName());
        query.append("=");
        query.append(property.get(properties));
      }
    }

    if (query.length() > 0) {
      url.append("?");
      url.append(query);
    }

    return url.toString();
  }

  /**
   * Sets properties from a {@link DriverManager} URL.
   *
   * @param url properties to set
   */
  public void setUrl(String url) {

    Properties p = org.postgresql.Driver.parseURL(url, null);

    for (PGProperty property : PGProperty.values()) {
      setProperty(property, property.get(p));
    }
  }

  public String getProperty(String name) throws SQLException {
    PGProperty pgProperty = PGProperty.forName(name);
    if (pgProperty != null) {
      return getProperty(pgProperty);
    } else {
      throw new PSQLException(GT.tr("Unsupported property name: {0}", name),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public void setProperty(String name, String value) throws SQLException {
    PGProperty pgProperty = PGProperty.forName(name);
    if (pgProperty != null) {
      setProperty(pgProperty, value);
    } else {
      throw new PSQLException(GT.tr("Unsupported property name: {0}", name),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public String getProperty(PGProperty property) {
    return property.get(properties);
  }

  public void setProperty(PGProperty property, String value) {
    if (value == null) {
      return;
    }
    switch (property) {
      case PG_HOST:
        serverName = value;
        break;
      case PG_PORT:
        try {
          portNumber = Integer.parseInt(value);
        } catch (NumberFormatException e) {
          portNumber = 0;
        }
        break;
      case PG_DBNAME:
        databaseName = value;
        break;
      case USER:
        user = value;
        break;
      case PASSWORD:
        password = value;
        break;
      default:
        properties.setProperty(property.getName(), value);
    }
  }

  /**
   * Generates a reference using the appropriate object factory.
   *
   * @return reference using the appropriate object factory
   */
  protected Reference createReference() {
    return new Reference(getClass().getName(), PGObjectFactory.class.getName(), null);
  }

  public Reference getReference() throws NamingException {
    Reference ref = createReference();
    ref.add(new StringRefAddr("serverName", serverName));
    if (portNumber != 0) {
      ref.add(new StringRefAddr("portNumber", Integer.toString(portNumber)));
    }
    ref.add(new StringRefAddr("databaseName", databaseName));
    if (user != null) {
      ref.add(new StringRefAddr("user", user));
    }
    if (password != null) {
      ref.add(new StringRefAddr("password", password));
    }

    for (PGProperty property : PGProperty.values()) {
      if (property.isPresent(properties)) {
        ref.add(new StringRefAddr(property.getName(), property.get(properties)));
      }
    }

    return ref;
  }

  public void setFromReference(Reference ref) {
    databaseName = getReferenceProperty(ref, "databaseName");
    String port = getReferenceProperty(ref, "portNumber");
    if (port != null) {
      portNumber = Integer.parseInt(port);
    }
    serverName = getReferenceProperty(ref, "serverName");
    user = getReferenceProperty(ref, "user");
    password = getReferenceProperty(ref, "password");

    for (PGProperty property : PGProperty.values()) {
      property.set(properties, getReferenceProperty(ref, property.getName()));
    }
  }

  private static String getReferenceProperty(Reference ref, String propertyName) {
    RefAddr addr = ref.get(propertyName);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

  protected void writeBaseObject(ObjectOutputStream out) throws IOException {
    out.writeObject(serverName);
    out.writeObject(databaseName);
    out.writeObject(user);
    out.writeObject(password);
    out.writeInt(portNumber);

    out.writeObject(properties);
  }

  protected void readBaseObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    serverName = (String) in.readObject();
    databaseName = (String) in.readObject();
    user = (String) in.readObject();
    password = (String) in.readObject();
    portNumber = in.readInt();

    properties = (Properties) in.readObject();
  }

  public void initializeFrom(BaseDataSource source) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    source.writeBaseObject(oos);
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    readBaseObject(ois);
  }

  public void setLoglevel(int logLevel) {
    PGProperty.LOG_LEVEL.set(properties, logLevel);
  }

  public int getLoglevel() {
    return PGProperty.LOG_LEVEL.getIntNoCheck(properties);
  }

  /**
   * @see PGProperty#PREFER_QUERY_MODE
   * @return preferred query execution mode
   */
  public PreferQueryMode getPreferQueryMode() {
    return PreferQueryMode.of(PGProperty.PREFER_QUERY_MODE.get(properties));
  }

  /**
   * @see PGProperty#PREFER_QUERY_MODE
   * @param preferQueryMode extended, simple, extendedForPrepared, or extendedCacheEveryting
   */
  public void setPreferQueryMode(PreferQueryMode preferQueryMode) {
    PGProperty.PREFER_QUERY_MODE.set(properties, preferQueryMode.value());
  }

  /**
   * @see PGProperty#AUTOSAVE
   * @return connection configuration regarding automatic per-query savepoints
   */
  public AutoSave getAutosave() {
    return AutoSave.of(PGProperty.AUTOSAVE.get(properties));
  }

  /**
   * @see PGProperty#AUTOSAVE
   * @param autoSave connection configuration regarding automatic per-query savepoints
   */
  public void setAutosave(AutoSave autoSave) {
    PGProperty.AUTOSAVE.set(properties, autoSave.value());
  }

  /**
   * @see PGProperty#REWRITE_BATCHED_INSERTS
   * @return boolean indicating property is enabled or not.
   */
  public boolean getReWriteBatchedInserts() {
    return PGProperty.REWRITE_BATCHED_INSERTS.getBoolean(properties);
  }

  /**
   * @see PGProperty#REWRITE_BATCHED_INSERTS
   * @param reWrite boolean value to set the property in the properties collection
   */
  public void setReWriteBatchedInserts(boolean reWrite) {
    PGProperty.REWRITE_BATCHED_INSERTS.set(properties, reWrite);
  }
}
