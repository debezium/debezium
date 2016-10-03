/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ds;

import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

/**
 * DataSource which uses connection pooling. <span style="color: red;">Don't use this if your
 * server/middleware vendor provides a connection pooling implementation which interfaces with the
 * PostgreSQL ConnectionPoolDataSource implementation!</span> This class is provided as a
 * convenience, but the JDBC Driver is really not supposed to handle the connection pooling
 * algorithm. Instead, the server or middleware product is supposed to handle the mechanics of
 * connection pooling, and use the PostgreSQL implementation of ConnectionPoolDataSource to provide
 * the connections to pool.
 *
 * <p>
 * If you're sure you want to use this, then you must set the properties dataSourceName,
 * databaseName, user, and password (if required for the user). The settings for serverName,
 * portNumber, initialConnections, and maxConnections are optional. Note that <i>only connections
 * for the default user will be pooled!</i> Connections for other users will be normal non-pooled
 * connections, and will not count against the maximum pool size limit.
 * </p>
 *
 * <p>
 * If you put this DataSource in JNDI, and access it from different JVMs (or otherwise load this
 * class from different ClassLoaders), you'll end up with one pool per ClassLoader or VM. This is
 * another area where a server-specific implementation may provide advanced features, such as using
 * a single pool across all VMs in a cluster.
 * </p>
 *
 * <p>
 * This implementation supports JDK 1.5 and higher.
 * </p>
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGPoolingDataSource extends BaseDataSource implements DataSource {
  protected static ConcurrentMap<String, PGPoolingDataSource> dataSources =
      new ConcurrentHashMap<String, PGPoolingDataSource>();

  public static PGPoolingDataSource getDataSource(String name) {
    return dataSources.get(name);
  }

  // Additional Data Source properties
  protected String dataSourceName; // Must be protected for subclasses to sync updates to it
  private int initialConnections = 0;
  private int maxConnections = 0;
  // State variables
  private boolean initialized = false;
  private Stack<PooledConnection> available = new Stack<PooledConnection>();
  private Stack<PooledConnection> used = new Stack<PooledConnection>();
  private Object lock = new Object();
  private PGConnectionPoolDataSource source;

  /**
   * Gets a description of this DataSource.
   */
  public String getDescription() {
    return "Pooling DataSource '" + dataSourceName + " from " + org.postgresql.Driver.getVersion();
  }

  /**
   * Ensures the DataSource properties are not changed after the DataSource has been used.
   *
   * @throws IllegalStateException The Server Name cannot be changed after the DataSource has been
   *         used.
   */
  public void setServerName(String serverName) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    super.setServerName(serverName);
  }

  /**
   * Ensures the DataSource properties are not changed after the DataSource has been used.
   *
   * @throws IllegalStateException The Database Name cannot be changed after the DataSource has been
   *         used.
   */
  public void setDatabaseName(String databaseName) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    super.setDatabaseName(databaseName);
  }

  /**
   * Ensures the DataSource properties are not changed after the DataSource has been used.
   *
   * @throws IllegalStateException The User cannot be changed after the DataSource has been used.
   */
  public void setUser(String user) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    super.setUser(user);
  }

  /**
   * Ensures the DataSource properties are not changed after the DataSource has been used.
   *
   * @throws IllegalStateException The Password cannot be changed after the DataSource has been
   *         used.
   */
  public void setPassword(String password) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    super.setPassword(password);
  }

  /**
   * Ensures the DataSource properties are not changed after the DataSource has been used.
   *
   * @throws IllegalStateException The Port Number cannot be changed after the DataSource has been
   *         used.
   */
  public void setPortNumber(int portNumber) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    super.setPortNumber(portNumber);
  }

  /**
   * Gets the number of connections that will be created when this DataSource is initialized. If you
   * do not call initialize explicitly, it will be initialized the first time a connection is drawn
   * from it.
   *
   * @return number of connections that will be created when this DataSource is initialized
   */
  public int getInitialConnections() {
    return initialConnections;
  }

  /**
   * Sets the number of connections that will be created when this DataSource is initialized. If you
   * do not call initialize explicitly, it will be initialized the first time a connection is drawn
   * from it.
   *
   * @param initialConnections number of initial connections
   * @throws IllegalStateException The Initial Connections cannot be changed after the DataSource
   *         has been used.
   */
  public void setInitialConnections(int initialConnections) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    this.initialConnections = initialConnections;
  }

  /**
   * Gets the maximum number of connections that the pool will allow. If a request comes in and this
   * many connections are in use, the request will block until a connection is available. Note that
   * connections for a user other than the default user will not be pooled and don't count against
   * this limit.
   *
   * @return The maximum number of pooled connection allowed, or 0 for no maximum.
   */
  public int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Sets the maximum number of connections that the pool will allow. If a request comes in and this
   * many connections are in use, the request will block until a connection is available. Note that
   * connections for a user other than the default user will not be pooled and don't count against
   * this limit.
   *
   * @param maxConnections The maximum number of pooled connection to allow, or 0 for no maximum.
   * @throws IllegalStateException The Maximum Connections cannot be changed after the DataSource
   *         has been used.
   */
  public void setMaxConnections(int maxConnections) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    this.maxConnections = maxConnections;
  }

  /**
   * Gets the name of this DataSource. This uniquely identifies the DataSource. You cannot use more
   * than one DataSource in the same VM with the same name.
   *
   * @return name of this DataSource
   */
  public String getDataSourceName() {
    return dataSourceName;
  }

  /**
   * Sets the name of this DataSource. This is required, and uniquely identifies the DataSource. You
   * cannot create or use more than one DataSource in the same VM with the same name.
   *
   * @param dataSourceName datasource name
   * @throws IllegalStateException The Data Source Name cannot be changed after the DataSource has
   *         been used.
   * @throws IllegalArgumentException Another PoolingDataSource with the same dataSourceName already
   *         exists.
   */
  public void setDataSourceName(String dataSourceName) {
    if (initialized) {
      throw new IllegalStateException(
          "Cannot set Data Source properties after DataSource has been used");
    }
    if (this.dataSourceName != null && dataSourceName != null
        && dataSourceName.equals(this.dataSourceName)) {
      return;
    }
    PGPoolingDataSource previous = dataSources.putIfAbsent(dataSourceName, this);
    if (previous != null) {
      throw new IllegalArgumentException(
          "DataSource with name '" + dataSourceName + "' already exists!");
    }
    if (this.dataSourceName != null) {
      dataSources.remove(this.dataSourceName);
    }
    this.dataSourceName = dataSourceName;
  }

  /**
   * Initializes this DataSource. If the initialConnections is greater than zero, that number of
   * connections will be created. After this method is called, the DataSource properties cannot be
   * changed. If you do not call this explicitly, it will be called the first time you get a
   * connection from the DataSource.
   *
   * @throws SQLException Occurs when the initialConnections is greater than zero, but the
   *         DataSource is not able to create enough physical connections.
   */
  public void initialize() throws SQLException {
    synchronized (lock) {
      source = createConnectionPool();
      try {
        source.initializeFrom(this);
      } catch (Exception e) {
        throw new PSQLException(GT.tr("Failed to setup DataSource."), PSQLState.UNEXPECTED_ERROR,
            e);
      }

      while (available.size() < initialConnections) {
        available.push(source.getPooledConnection());
      }

      initialized = true;
    }
  }

  protected boolean isInitialized() {
    return initialized;
  }

  /**
   * Creates the appropriate ConnectionPool to use for this DataSource.
   *
   * @return appropriate ConnectionPool to use for this DataSource
   */
  protected PGConnectionPoolDataSource createConnectionPool() {
    return new PGConnectionPoolDataSource();
  }

  /**
   * Gets a <b>non-pooled</b> connection, unless the user and password are the same as the default
   * values for this connection pool.
   *
   * @return A pooled connection.
   * @throws SQLException Occurs when no pooled connection is available, and a new physical
   *         connection cannot be created.
   */
  public Connection getConnection(String user, String password) throws SQLException {
    // If this is for the default user/password, use a pooled connection
    if (user == null || (user.equals(getUser()) && ((password == null && getPassword() == null)
        || (password != null && password.equals(getPassword()))))) {
      return getConnection();
    }
    // Otherwise, use a non-pooled connection
    if (!initialized) {
      initialize();
    }
    return super.getConnection(user, password);
  }

  /**
   * Gets a connection from the connection pool.
   *
   * @return A pooled connection.
   * @throws SQLException Occurs when no pooled connection is available, and a new physical
   *         connection cannot be created.
   */
  public Connection getConnection() throws SQLException {
    if (!initialized) {
      initialize();
    }
    return getPooledConnection();
  }

  /**
   * Closes this DataSource, and all the pooled connections, whether in use or not.
   */
  public void close() {
    synchronized (lock) {
      while (!available.isEmpty()) {
        PooledConnection pci = available.pop();
        try {
          pci.close();
        } catch (SQLException e) {
        }
      }
      available = null;
      while (!used.isEmpty()) {
        PooledConnection pci = used.pop();
        pci.removeConnectionEventListener(connectionEventListener);
        try {
          pci.close();
        } catch (SQLException e) {
        }
      }
      used = null;
    }
    removeStoredDataSource();
  }

  protected void removeStoredDataSource() {
    dataSources.remove(dataSourceName);
  }

  protected void addDataSource(String dataSourceName) {
    dataSources.put(dataSourceName, this);
  }

  /**
   * Gets a connection from the pool. Will get an available one if present, or create a new one if
   * under the max limit. Will block if all used and a new one would exceed the max.
   */
  private Connection getPooledConnection() throws SQLException {
    PooledConnection pc = null;
    synchronized (lock) {
      if (available == null) {
        throw new PSQLException(GT.tr("DataSource has been closed."),
            PSQLState.CONNECTION_DOES_NOT_EXIST);
      }
      while (true) {
        if (!available.isEmpty()) {
          pc = available.pop();
          used.push(pc);
          break;
        }
        if (maxConnections == 0 || used.size() < maxConnections) {
          pc = source.getPooledConnection();
          used.push(pc);
          break;
        } else {
          try {
            // Wake up every second at a minimum
            lock.wait(1000L);
          } catch (InterruptedException e) {
          }
        }
      }
    }
    pc.addConnectionEventListener(connectionEventListener);
    return pc.getConnection();
  }

  /**
   * Notified when a pooled connection is closed, or a fatal error occurs on a pooled connection.
   * This is the only way connections are marked as unused.
   */
  private ConnectionEventListener connectionEventListener = new ConnectionEventListener() {
    public void connectionClosed(ConnectionEvent event) {
      ((PooledConnection) event.getSource()).removeConnectionEventListener(this);
      synchronized (lock) {
        if (available == null) {
          return; // DataSource has been closed
        }
        boolean removed = used.remove(event.getSource());
        if (removed) {
          available.push((PooledConnection) event.getSource());
          // There's now a new connection available
          lock.notify();
        } else {
          // a connection error occurred
        }
      }
    }

    /**
     * This is only called for fatal errors, where the physical connection is useless afterward and
     * should be removed from the pool.
     */
    public void connectionErrorOccurred(ConnectionEvent event) {
      ((PooledConnection) event.getSource()).removeConnectionEventListener(this);
      synchronized (lock) {
        if (available == null) {
          return; // DataSource has been closed
        }
        used.remove(event.getSource());
        // We're now at least 1 connection under the max
        lock.notify();
      }
    }
  };

  /**
   * Adds custom properties for this DataSource to the properties defined in the superclass.
   */
  public Reference getReference() throws NamingException {
    Reference ref = super.getReference();
    ref.add(new StringRefAddr("dataSourceName", dataSourceName));
    if (initialConnections > 0) {
      ref.add(new StringRefAddr("initialConnections", Integer.toString(initialConnections)));
    }
    if (maxConnections > 0) {
      ref.add(new StringRefAddr("maxConnections", Integer.toString(maxConnections)));
    }
    return ref;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getParentLogger()");
  }
}
