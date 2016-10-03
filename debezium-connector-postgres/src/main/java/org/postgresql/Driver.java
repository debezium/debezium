/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql;

import org.postgresql.core.Logger;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.SharedTimer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver should supply a class
 * that implements the Driver interface
 *
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then for any given
 * connection request, it will ask each driver in turn to try to connect to the target URL.
 *
 * <p>
 * It is strongly recommended that each Driver class should be small and standalone so that the
 * Driver class can be loaded and queried without bringing in vast quantities of supporting code.
 *
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and register it with the
 * DriverManager. This means that a user can load and register a driver by doing
 * Class.forName("foo.bah.Driver")
 *
 * @see org.postgresql.PGConnection
 * @see java.sql.Driver
 */
public class Driver implements java.sql.Driver {

  // make these public so they can be used in setLogLevel below

  public static final int DEBUG = 2;
  public static final int INFO = 1;
  public static final int OFF = 0;

  private static Driver registeredDriver;
  private static final Logger logger = new Logger();
  private static boolean logLevelSet = false;
  private static SharedTimer sharedTimer = new SharedTimer(logger);

  static {
    try {
      // moved the registerDriver from the constructor to here
      // because some clients call the driver themselves (I know, as
      // my early jdbc work did - and that was based on other examples).
      // Placing it here, means that the driver is registered once only.
      register();
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // Helper to retrieve default properties from classloader resource
  // properties files.
  private Properties defaultProperties;

  private synchronized Properties getDefaultProperties() throws IOException {
    if (defaultProperties != null) {
      return defaultProperties;
    }

    // Make sure we load properties with the maximum possible
    // privileges.
    try {
      defaultProperties =
          AccessController.doPrivileged(new PrivilegedExceptionAction<Properties>() {
            public Properties run() throws IOException {
              return loadDefaultProperties();
            }
          });
    } catch (PrivilegedActionException e) {
      throw (IOException) e.getException();
    }

    // Use the loglevel from the default properties (if any)
    // as the driver-wide default unless someone explicitly called
    // setLogLevel() already.
    synchronized (Driver.class) {
      if (!logLevelSet) {
        String driverLogLevel = PGProperty.LOG_LEVEL.get(defaultProperties);
        if (driverLogLevel != null) {
          try {
            setLogLevel(Integer.parseInt(driverLogLevel));
          } catch (Exception l_e) {
            // XXX revisit
            // invalid value for loglevel; ignore it
          }
        }
      }
    }

    return defaultProperties;
  }

  private Properties loadDefaultProperties() throws IOException {
    Properties merged = new Properties();

    try {
      PGProperty.USER.set(merged, System.getProperty("user.name"));
    } catch (java.lang.SecurityException se) {
      // We're just trying to set a default, so if we can't
      // it's not a big deal.
    }

    // If we are loaded by the bootstrap classloader, getClassLoader()
    // may return null. In that case, try to fall back to the system
    // classloader.
    //
    // We should not need to catch SecurityException here as we are
    // accessing either our own classloader, or the system classloader
    // when our classloader is null. The ClassLoader javadoc claims
    // neither case can throw SecurityException.
    ClassLoader cl = getClass().getClassLoader();
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }

    if (cl == null) {
      logger.debug("Can't find a classloader for the Driver; not loading driver configuration");
      return merged; // Give up on finding defaults.
    }

    if (logger.logDebug()) {
      logger.debug("Loading driver configuration via classloader " + cl);
    }

    // When loading the driver config files we don't want settings found
    // in later files in the classpath to override settings specified in
    // earlier files. To do this we've got to read the returned
    // Enumeration into temporary storage.
    ArrayList<URL> urls = new ArrayList<URL>();
    Enumeration<URL> urlEnum = cl.getResources("org/postgresql/driverconfig.properties");
    while (urlEnum.hasMoreElements()) {
      urls.add(urlEnum.nextElement());
    }

    for (int i = urls.size() - 1; i >= 0; i--) {
      URL url = urls.get(i);
      if (logger.logDebug()) {
        logger.debug("Loading driver configuration from: " + url);
      }
      InputStream is = url.openStream();
      merged.load(is);
      is.close();
    }

    return merged;
  }

  /**
   * Try to make a database connection to the given URL. The driver should return "null" if it
   * realizes it is the wrong kind of driver to connect to the given URL. This will be common, as
   * when the JDBC driverManager is asked to connect to a given URL, it passes the URL to each
   * loaded driver in turn.
   *
   * <p>
   * The driver should raise an SQLException if it is the right driver to connect to the given URL,
   * but has trouble connecting to the database.
   *
   * <p>
   * The java.util.Properties argument can be used to pass arbitrary string tag/value pairs as
   * connection arguments.
   *
   * user - (required) The user to connect as password - (optional) The password for the user ssl -
   * (optional) Use SSL when connecting to the server readOnly - (optional) Set connection to
   * read-only by default charSet - (optional) The character set to be used for converting to/from
   * the database to unicode. If multibyte is enabled on the server then the character set of the
   * database is used as the default, otherwise the jvm character encoding is used as the default.
   * This value is only used when connecting to a 7.2 or older server. loglevel - (optional) Enable
   * logging of messages from the driver. The value is an integer from 0 to 2 where: OFF = 0, INFO =
   * 1, DEBUG = 2 The output is sent to DriverManager.getPrintWriter() if set, otherwise it is sent
   * to System.out. compatible - (optional) This is used to toggle between different functionality
   * as it changes across different releases of the jdbc driver code. The values here are versions
   * of the jdbc client and not server versions. For example in 7.1 get/setBytes worked on
   * LargeObject values, in 7.2 these methods were changed to work on bytea values. This change in
   * functionality could be disabled by setting the compatible level to be "7.1", in which case the
   * driver will revert to the 7.1 functionality.
   *
   * <p>
   * Normally, at least "user" and "password" properties should be included in the properties. For a
   * list of supported character encoding , see
   * http://java.sun.com/products/jdk/1.2/docs/guide/internat/encoding.doc.html Note that you will
   * probably want to have set up the Postgres database itself to use the same encoding, with the
   * {@code -E <encoding>} argument to createdb.
   *
   * Our protocol takes the forms:
   *
   * <PRE>
   *  jdbc:postgresql://host:port/database?param1=val1&amp;...
   * </PRE>
   *
   * @param url the URL of the database to connect to
   * @param info a list of arbitrary tag/value pairs as connection arguments
   * @return a connection to the URL or null if it isnt us
   * @throws SQLException if a database access error occurs
   * @see java.sql.Driver#connect
   */
  public java.sql.Connection connect(String url, Properties info) throws SQLException {
    // get defaults
    Properties defaults;

    if (!url.startsWith("jdbc:postgresql:")) {
      return null;
    }
    try {
      defaults = getDefaultProperties();
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Error loading default settings from driverconfig.properties"),
          PSQLState.UNEXPECTED_ERROR, ioe);
    }

    // override defaults with provided properties
    Properties props = new Properties(defaults);
    if (info != null) {
      Enumeration<?> e = info.propertyNames();
      while (e.hasMoreElements()) {
        String propName = (String) e.nextElement();
        String propValue = info.getProperty(propName);
        if (propValue == null) {
          throw new PSQLException(
              GT.tr("Properties for the driver contains a non-string value for the key ")
                  + propName,
              PSQLState.UNEXPECTED_ERROR);
        }
        props.setProperty(propName, propValue);
      }
    }
    // parse URL and add more properties
    if ((props = parseURL(url, props)) == null) {
      logger.debug("Error in url: " + url);
      return null;
    }
    try {
      if (logger.logDebug()) {
        logger.debug("Connecting with URL: " + url);
      }

      // Enforce login timeout, if specified, by running the connection
      // attempt in a separate thread. If we hit the timeout without the
      // connection completing, we abandon the connection attempt in
      // the calling thread, but the separate thread will keep trying.
      // Eventually, the separate thread will either fail or complete
      // the connection; at that point we clean up the connection if
      // we managed to establish one after all. See ConnectThread for
      // more details.
      long timeout = timeout(props);
      if (timeout <= 0) {
        return makeConnection(url, props);
      }

      ConnectThread ct = new ConnectThread(url, props);
      Thread thread = new Thread(ct, "PostgreSQL JDBC driver connection thread");
      thread.setDaemon(true); // Don't prevent the VM from shutting down
      thread.start();
      return ct.getResult(timeout);
    } catch (PSQLException ex1) {
      logger.debug("Connection error:", ex1);
      // re-throw the exception, otherwise it will be caught next, and a
      // org.postgresql.unusual error will be returned instead.
      throw ex1;
    } catch (java.security.AccessControlException ace) {
      throw new PSQLException(
          GT.tr(
              "Your security policy has prevented the connection from being attempted.  You probably need to grant the connect java.net.SocketPermission to the database server host and port that you wish to connect to."),
          PSQLState.UNEXPECTED_ERROR, ace);
    } catch (Exception ex2) {
      logger.debug("Unexpected connection error:", ex2);
      throw new PSQLException(
          GT.tr(
              "Something unusual has occurred to cause the driver to fail. Please report this exception."),
          PSQLState.UNEXPECTED_ERROR, ex2);
    }
  }

  /**
   * Perform a connect in a separate thread; supports getting the results from the original thread
   * while enforcing a login timeout.
   */
  private static class ConnectThread implements Runnable {
    ConnectThread(String url, Properties props) {
      this.url = url;
      this.props = props;
    }

    public void run() {
      Connection conn;
      Throwable error;

      try {
        conn = makeConnection(url, props);
        error = null;
      } catch (Throwable t) {
        conn = null;
        error = t;
      }

      synchronized (this) {
        if (abandoned) {
          if (conn != null) {
            try {
              conn.close();
            } catch (SQLException e) {
            }
          }
        } else {
          result = conn;
          resultException = error;
          notify();
        }
      }
    }

    /**
     * Get the connection result from this (assumed running) thread. If the timeout is reached
     * without a result being available, a SQLException is thrown.
     *
     * @param timeout timeout in milliseconds
     * @return the new connection, if successful
     * @throws SQLException if a connection error occurs or the timeout is reached
     */
    public Connection getResult(long timeout) throws SQLException {
      long expiry = System.currentTimeMillis() + timeout;
      synchronized (this) {
        while (true) {
          if (result != null) {
            return result;
          }

          if (resultException != null) {
            if (resultException instanceof SQLException) {
              resultException.fillInStackTrace();
              throw (SQLException) resultException;
            } else {
              throw new PSQLException(
                  GT.tr(
                      "Something unusual has occurred to cause the driver to fail. Please report this exception."),
                  PSQLState.UNEXPECTED_ERROR, resultException);
            }
          }

          long delay = expiry - System.currentTimeMillis();
          if (delay <= 0) {
            abandoned = true;
            throw new PSQLException(GT.tr("Connection attempt timed out."),
                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
          }

          try {
            wait(delay);
          } catch (InterruptedException ie) {

            // reset the interrupt flag
            Thread.currentThread().interrupt();
            abandoned = true;

            // throw an unchecked exception which will hopefully not be ignored by the calling code
            throw new RuntimeException(GT.tr("Interrupted while attempting to connect."));
          }
        }
      }
    }

    private final String url;
    private final Properties props;
    private Connection result;
    private Throwable resultException;
    private boolean abandoned;
  }

  /**
   * Create a connection from URL and properties. Always does the connection work in the current
   * thread without enforcing a timeout, regardless of any timeout specified in the properties.
   *
   * @param url the original URL
   * @param props the parsed/defaulted connection properties
   * @return a new connection
   * @throws SQLException if the connection could not be made
   */
  private static Connection makeConnection(String url, Properties props) throws SQLException {
    return new PgConnection(hostSpecs(props), user(props), database(props), props, url);
  }

  /**
   * Returns true if the driver thinks it can open a connection to the given URL. Typically, drivers
   * will return true if they understand the subprotocol specified in the URL and false if they
   * don't. Our protocols start with jdbc:postgresql:
   *
   * @param url the URL of the driver
   * @return true if this driver accepts the given URL
   * @see java.sql.Driver#acceptsURL
   */
  public boolean acceptsURL(String url) {
    return parseURL(url, null) != null;
  }

  /**
   * The getPropertyInfo method is intended to allow a generic GUI tool to discover what properties
   * it should prompt a human for in order to get enough information to connect to a database.
   *
   * <p>
   * Note that depending on the values the human has supplied so far, additional values may become
   * necessary, so it may be necessary to iterate through several calls to getPropertyInfo
   *
   * @param url the Url of the database to connect to
   * @param info a proposed list of tag/value pairs that will be sent on connect open.
   * @return An array of DriverPropertyInfo objects describing possible properties. This array may
   *         be an empty array if no properties are required
   * @see java.sql.Driver#getPropertyInfo
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    Properties copy = new Properties(info);
    Properties parse = parseURL(url, copy);
    if (parse != null) {
      copy = parse;
    }

    PGProperty[] knownProperties = PGProperty.values();
    DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.length];
    for (int i = 0; i < props.length; ++i) {
      props[i] = knownProperties[i].toDriverPropertyInfo(copy);
    }

    return props;
  }

  public static final int MAJORVERSION =
      /*$mvn.project.property.parsedversion.majorversion+";"$*//*-*/9;

  /**
   * Gets the drivers major version number
   *
   * @return the drivers major version number
   */
  public int getMajorVersion() {
    return MAJORVERSION;
  }

  public static final int MINORVERSION =
      /*$mvn.project.property.parsedversion.minorversion+";"$*//*-*/4;

  /**
   * Get the drivers minor version number
   *
   * @return the drivers minor version number
   */
  public int getMinorVersion() {
    return MINORVERSION;
  }

  /**
   * Returns the server version series of this driver and the specific build number.
   *
   * @return JDBC driver version
   */
  public static String getVersion() {
    return "PostgreSQL /*$mvn.project.property.parsedversion.osgiversion$*/";
  }

  /**
   * Report whether the driver is a genuine JDBC compliant driver. A driver may only report "true"
   * here if it passes the JDBC compliance tests, otherwise it is required to return false. JDBC
   * compliance requires full support for the JDBC API and full support for SQL 92 Entry Level.
   *
   * <p>
   * For PostgreSQL, this is not yet possible, as we are not SQL92 compliant (yet).
   */
  public boolean jdbcCompliant() {
    return false;
  }

  /**
   * Constructs a new DriverURL, splitting the specified URL into its component parts
   *
   * @param url JDBC URL to parse
   * @param defaults Default properties
   * @return Properties with elements added from the url
   */
  public static Properties parseURL(String url, Properties defaults) {
    Properties urlProps = new Properties(defaults);

    String l_urlServer = url;
    String l_urlArgs = "";

    int l_qPos = url.indexOf('?');
    if (l_qPos != -1) {
      l_urlServer = url.substring(0, l_qPos);
      l_urlArgs = url.substring(l_qPos + 1);
    }

    if (!l_urlServer.startsWith("jdbc:postgresql:")) {
      return null;
    }
    l_urlServer = l_urlServer.substring("jdbc:postgresql:".length());

    if (l_urlServer.startsWith("//")) {
      l_urlServer = l_urlServer.substring(2);
      int slash = l_urlServer.indexOf('/');
      if (slash == -1) {
        return null;
      }
      urlProps.setProperty("PGDBNAME", URLDecoder.decode(l_urlServer.substring(slash + 1)));

      String[] addresses = l_urlServer.substring(0, slash).split(",");
      StringBuilder hosts = new StringBuilder();
      StringBuilder ports = new StringBuilder();
      for (int addr = 0; addr < addresses.length; ++addr) {
        String address = addresses[addr];

        int portIdx = address.lastIndexOf(':');
        if (portIdx != -1 && address.lastIndexOf(']') < portIdx) {
          String portStr = address.substring(portIdx + 1);
          try {
            // squid:S2201 The return value of "parseInt" must be used.
            // The side effect is NumberFormatException, thus ignore sonar error here
            Integer.parseInt(portStr); //NOSONAR
          } catch (NumberFormatException ex) {
            return null;
          }
          ports.append(portStr);
          hosts.append(address.subSequence(0, portIdx));
        } else {
          ports.append("/*$mvn.project.property.template.default.pg.port$*/");
          hosts.append(address);
        }
        ports.append(',');
        hosts.append(',');
      }
      ports.setLength(ports.length() - 1);
      hosts.setLength(hosts.length() - 1);
      urlProps.setProperty("PGPORT", ports.toString());
      urlProps.setProperty("PGHOST", hosts.toString());
    } else {
      urlProps.setProperty("PGPORT", "/*$mvn.project.property.template.default.pg.port$*/");
      urlProps.setProperty("PGHOST", "localhost");
      urlProps.setProperty("PGDBNAME", URLDecoder.decode(l_urlServer));
    }

    // parse the args part of the url
    String[] args = l_urlArgs.split("&");
    for (String token : args) {
      if (token.isEmpty()) {
        continue;
      }
      int l_pos = token.indexOf('=');
      if (l_pos == -1) {
        urlProps.setProperty(token, "");
      } else {
        urlProps.setProperty(token.substring(0, l_pos), URLDecoder.decode(token.substring(l_pos + 1)));
      }
    }

    return urlProps;
  }

  /**
   * @return the address portion of the URL
   */
  private static HostSpec[] hostSpecs(Properties props) {
    String[] hosts = props.getProperty("PGHOST").split(",");
    String[] ports = props.getProperty("PGPORT").split(",");
    HostSpec[] hostSpecs = new HostSpec[hosts.length];
    for (int i = 0; i < hostSpecs.length; ++i) {
      hostSpecs[i] = new HostSpec(hosts[i], Integer.parseInt(ports[i]));
    }
    return hostSpecs;
  }

  /**
   * @return the username of the URL
   */
  private static String user(Properties props) {
    return props.getProperty("user", "");
  }

  /**
   * @return the database name of the URL
   */
  private static String database(Properties props) {
    return props.getProperty("PGDBNAME", "");
  }

  /**
   * @return the timeout from the URL, in milliseconds
   */
  private static long timeout(Properties props) {
    String timeout = PGProperty.LOGIN_TIMEOUT.get(props);
    if (timeout != null) {
      try {
        return (long) (Float.parseFloat(timeout) * 1000);
      } catch (NumberFormatException e) {
        // Log level isn't set yet, so this doesn't actually
        // get printed.
        if (logger.logDebug()) {
          logger.debug("Couldn't parse loginTimeout value: " + timeout);
        }
      }
    }
    return (long) DriverManager.getLoginTimeout() * 1000;
  }

  /**
   * This method was added in v6.5, and simply throws an SQLException for an unimplemented method. I
   * decided to do it this way while implementing the JDBC2 extensions to JDBC, as it should help
   * keep the overall driver size down. It now requires the call Class and the function name to help
   * when the driver is used with closed software that don't report the stack strace
   *
   * @param callClass the call Class
   * @param functionName the name of the unimplemented function with the type of its arguments
   * @return PSQLException with a localized message giving the complete description of the
   *         unimplemeted function
   */
  public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass,
      String functionName) {
    return new SQLFeatureNotSupportedException(
        GT.tr("Method {0} is not yet implemented.", callClass.getName() + "." + functionName),
        PSQLState.NOT_IMPLEMENTED.getState());
  }

  /**
   * used to turn logging on to a certain level, can be called by specifying fully qualified class
   * ie org.postgresql.Driver.setLogLevel()
   *
   * @param logLevel sets the level which logging will respond to OFF turn off logging INFO being
   *        almost no messages DEBUG most verbose
   */
  public static void setLogLevel(int logLevel) {
    synchronized (Driver.class) {
      logger.setLogLevel(logLevel);
      logLevelSet = true;
    }
  }

  public static int getLogLevel() {
    synchronized (Driver.class) {
      return logger.getLogLevel();
    }
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw notImplemented(this.getClass(), "getParentLogger()");
  }

  public static SharedTimer getSharedTimer() {
    return sharedTimer;
  }

  /**
   * Register the driver against {@link DriverManager}. This is done automatically when the class is
   * loaded. Dropping the driver from DriverManager's list is possible using {@link #deregister()}
   * method.
   *
   * @throws IllegalStateException if the driver is already registered
   * @throws SQLException if registering the driver fails
   */
  public static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          "Driver is already registered. It can only be registered once.");
    }
    Driver registeredDriver = new Driver();
    DriverManager.registerDriver(registeredDriver);
    Driver.registeredDriver = registeredDriver;
  }

  /**
   * According to JDBC specification, this driver is registered against {@link DriverManager} when
   * the class is loaded. To avoid leaks, this method allow unregistering the driver so that the
   * class can be gc'ed if necessary.
   *
   * @throws IllegalStateException if the driver is not registered
   * @throws SQLException if deregistering the driver fails
   */
  public static void deregister() throws SQLException {
    if (!isRegistered()) {
      throw new IllegalStateException(
          "Driver is not registered (or it has not been registered using Driver.register() method)");
    }
    DriverManager.deregisterDriver(registeredDriver);
    registeredDriver = null;
  }

  /**
   * @return {@code true} if the driver is registered against {@link DriverManager}
   */
  public static boolean isRegistered() {
    return registeredDriver != null;
  }
}
