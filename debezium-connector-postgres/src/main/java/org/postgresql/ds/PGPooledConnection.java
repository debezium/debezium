/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ds;

import org.postgresql.PGConnection;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * PostgreSQL implementation of the PooledConnection interface. This shouldn't be used directly, as
 * the pooling client should just interact with the ConnectionPool instead.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 * @author Csaba Nagy (ncsaba@yahoo.com)
 * @see org.postgresql.ds.PGConnectionPoolDataSource
 */
public class PGPooledConnection implements PooledConnection {
  private final List<ConnectionEventListener> listeners = new LinkedList<ConnectionEventListener>();
  private Connection con;
  private ConnectionHandler last;
  private final boolean autoCommit;
  private final boolean isXA;

  /**
   * Creates a new PooledConnection representing the specified physical connection.
   *
   * @param con connection
   * @param autoCommit whether to autocommit
   * @param isXA whether connection is a XA connection
   */
  public PGPooledConnection(Connection con, boolean autoCommit, boolean isXA) {
    this.con = con;
    this.autoCommit = autoCommit;
    this.isXA = isXA;
  }

  public PGPooledConnection(Connection con, boolean autoCommit) {
    this(con, autoCommit, false);
  }

  /**
   * Adds a listener for close or fatal error events on the connection handed out to a client.
   */
  public void addConnectionEventListener(ConnectionEventListener connectionEventListener) {
    listeners.add(connectionEventListener);
  }

  /**
   * Removes a listener for close or fatal error events on the connection handed out to a client.
   */
  public void removeConnectionEventListener(ConnectionEventListener connectionEventListener) {
    listeners.remove(connectionEventListener);
  }

  /**
   * Closes the physical database connection represented by this PooledConnection. If any client has
   * a connection based on this PooledConnection, it is forcibly closed as well.
   */
  public void close() throws SQLException {
    if (last != null) {
      last.close();
      if (!con.isClosed()) {
        if (!con.getAutoCommit()) {
          try {
            con.rollback();
          } catch (SQLException ignored) {
          }
        }
      }
    }
    try {
      con.close();
    } finally {
      con = null;
    }
  }

  /**
   * Gets a handle for a client to use. This is a wrapper around the physical connection, so the
   * client can call close and it will just return the connection to the pool without really closing
   * the pgysical connection.
   *
   * <p>
   * According to the JDBC 2.0 Optional Package spec (6.2.3), only one client may have an active
   * handle to the connection at a time, so if there is a previous handle active when this is
   * called, the previous one is forcibly closed and its work rolled back.
   * </p>
   */
  public Connection getConnection() throws SQLException {
    if (con == null) {
      // Before throwing the exception, let's notify the registered listeners about the error
      PSQLException sqlException =
          new PSQLException(GT.tr("This PooledConnection has already been closed."),
              PSQLState.CONNECTION_DOES_NOT_EXIST);
      fireConnectionFatalError(sqlException);
      throw sqlException;
    }
    // If any error occurs while opening a new connection, the listeners
    // have to be notified. This gives a chance to connection pools to
    // eliminate bad pooled connections.
    try {
      // Only one connection can be open at a time from this PooledConnection. See JDBC 2.0 Optional
      // Package spec section 6.2.3
      if (last != null) {
        last.close();
        if (!con.getAutoCommit()) {
          try {
            con.rollback();
          } catch (SQLException ignored) {
          }
        }
        con.clearWarnings();
      }
      /*
       * In XA-mode, autocommit is handled in PGXAConnection, because it depends on whether an
       * XA-transaction is open or not
       */
      if (!isXA) {
        con.setAutoCommit(autoCommit);
      }
    } catch (SQLException sqlException) {
      fireConnectionFatalError(sqlException);
      throw (SQLException) sqlException.fillInStackTrace();
    }
    ConnectionHandler handler = new ConnectionHandler(con);
    last = handler;

    Connection proxyCon = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class[]{Connection.class, PGConnection.class}, handler);
    last.setProxy(proxyCon);
    return proxyCon;
  }

  /**
   * Used to fire a connection closed event to all listeners.
   */
  void fireConnectionClosed() {
    ConnectionEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    ConnectionEventListener[] local =
        listeners.toArray(new ConnectionEventListener[listeners.size()]);
    for (ConnectionEventListener listener : local) {
      if (evt == null) {
        evt = createConnectionEvent(null);
      }
      listener.connectionClosed(evt);
    }
  }

  /**
   * Used to fire a connection error event to all listeners.
   */
  void fireConnectionFatalError(SQLException e) {
    ConnectionEvent evt = null;
    // Copy the listener list so the listener can remove itself during this method call
    ConnectionEventListener[] local =
        listeners.toArray(new ConnectionEventListener[listeners.size()]);
    for (ConnectionEventListener listener : local) {
      if (evt == null) {
        evt = createConnectionEvent(e);
      }
      listener.connectionErrorOccurred(evt);
    }
  }

  protected ConnectionEvent createConnectionEvent(SQLException e) {
    return new ConnectionEvent(this, e);
  }

  // Classes we consider fatal.
  private static String[] fatalClasses = {
      "08", // connection error
      "53", // insufficient resources

      // nb: not just "57" as that includes query cancel which is nonfatal
      "57P01", // admin shutdown
      "57P02", // crash shutdown
      "57P03", // cannot connect now

      "58", // system error (backend)
      "60", // system error (driver)
      "99", // unexpected error
      "F0", // configuration file error (backend)
      "XX", // internal error (backend)
  };

  private static boolean isFatalState(String state) {
    if (state == null) {
      // no info, assume fatal
      return true;
    }
    if (state.length() < 2) {
      // no class info, assume fatal
      return true;
    }

    for (String fatalClass : fatalClasses) {
      if (state.startsWith(fatalClass)) {
        return true; // fatal
      }
    }

    return false;
  }

  /**
   * Fires a connection error event, but only if we think the exception is fatal.
   *
   * @param e the SQLException to consider
   */
  private void fireConnectionError(SQLException e) {
    if (!isFatalState(e.getSQLState())) {
      return;
    }

    fireConnectionFatalError(e);
  }

  /**
   * Instead of declaring a class implementing Connection, which would have to be updated for every
   * JDK rev, use a dynamic proxy to handle all calls through the Connection interface. This is the
   * part that requires JDK 1.3 or higher, though JDK 1.2 could be supported with a 3rd-party proxy
   * package.
   */
  private class ConnectionHandler implements InvocationHandler {
    private Connection con;
    private Connection proxy; // the Connection the client is currently using, which is a proxy
    private boolean automatic = false;

    public ConnectionHandler(Connection con) {
      this.con = con;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final String methodName = method.getName();
      // From Object
      if (method.getDeclaringClass() == Object.class) {
        if (methodName.equals("toString")) {
          return "Pooled connection wrapping physical connection " + con;
        }
        if (methodName.equals("equals")) {
          return proxy == args[0];
        }
        if (methodName.equals("hashCode")) {
          return System.identityHashCode(proxy);
        }
        try {
          return method.invoke(con, args);
        } catch (InvocationTargetException e) {
          throw e.getTargetException();
        }
      }

      // All the rest is from the Connection or PGConnection interface
      if (methodName.equals("isClosed")) {
        return con == null || con.isClosed();
      }
      if (methodName.equals("close")) {
        // we are already closed and a double close
        // is not an error.
        if (con == null) {
          return null;
        }

        SQLException ex = null;
        if (!con.isClosed()) {
          if (!isXA && !con.getAutoCommit()) {
            try {
              con.rollback();
            } catch (SQLException e) {
              ex = e;
            }
          }
          con.clearWarnings();
        }
        con = null;
        this.proxy = null;
        last = null;
        fireConnectionClosed();
        if (ex != null) {
          throw ex;
        }
        return null;
      }
      if (con == null || con.isClosed()) {
        throw new PSQLException(automatic
            ? GT.tr(
                "Connection has been closed automatically because a new connection was opened for the same PooledConnection or the PooledConnection has been closed.")
            : GT.tr("Connection has been closed."), PSQLState.CONNECTION_DOES_NOT_EXIST);
      }

      // From here on in, we invoke via reflection, catch exceptions,
      // and check if they're fatal before rethrowing.
      try {
        if (methodName.equals("createStatement")) {
          Statement st = (Statement) method.invoke(con, args);
          return Proxy.newProxyInstance(getClass().getClassLoader(),
              new Class[]{Statement.class, org.postgresql.PGStatement.class},
              new StatementHandler(this, st));
        } else if (methodName.equals("prepareCall")) {
          Statement st = (Statement) method.invoke(con, args);
          return Proxy.newProxyInstance(getClass().getClassLoader(),
              new Class[]{CallableStatement.class, org.postgresql.PGStatement.class},
              new StatementHandler(this, st));
        } else if (methodName.equals("prepareStatement")) {
          Statement st = (Statement) method.invoke(con, args);
          return Proxy.newProxyInstance(getClass().getClassLoader(),
              new Class[]{PreparedStatement.class, org.postgresql.PGStatement.class},
              new StatementHandler(this, st));
        } else {
          return method.invoke(con, args);
        }
      } catch (final InvocationTargetException ite) {
        final Throwable te = ite.getTargetException();
        if (te instanceof SQLException) {
          fireConnectionError((SQLException) te); // Tell listeners about exception if it's fatal
        }
        throw te;
      }
    }

    Connection getProxy() {
      return proxy;
    }

    void setProxy(Connection proxy) {
      this.proxy = proxy;
    }

    public void close() {
      if (con != null) {
        automatic = true;
      }
      con = null;
      proxy = null;
      // No close event fired here: see JDBC 2.0 Optional Package spec section 6.3
    }

    public boolean isClosed() {
      return con == null;
    }
  }

  /**
   * Instead of declaring classes implementing Statement, PreparedStatement, and CallableStatement,
   * which would have to be updated for every JDK rev, use a dynamic proxy to handle all calls
   * through the Statement interfaces. This is the part that requires JDK 1.3 or higher, though JDK
   * 1.2 could be supported with a 3rd-party proxy package.
   *
   * The StatementHandler is required in order to return the proper Connection proxy for the
   * getConnection method.
   */
  private class StatementHandler implements InvocationHandler {
    private ConnectionHandler con;
    private Statement st;

    public StatementHandler(ConnectionHandler con, Statement st) {
      this.con = con;
      this.st = st;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final String methodName = method.getName();
      // From Object
      if (method.getDeclaringClass() == Object.class) {
        if (methodName.equals("toString")) {
          return "Pooled statement wrapping physical statement " + st;
        }
        if (methodName.equals("hashCode")) {
          return System.identityHashCode(proxy);
        }
        if (methodName.equals("equals")) {
          return proxy == args[0];
        }
        return method.invoke(st, args);
      }

      // All the rest is from the Statement interface
      if (methodName.equals("isClosed")) {
        return st == null || st.isClosed();
      }
      if (methodName.equals("close")) {
        if (st == null || st.isClosed()) {
          return null;
        }
        con = null;
        final Statement oldSt = st;
        st = null;
        oldSt.close();
        return null;
      }
      if (st == null || st.isClosed()) {
        throw new PSQLException(GT.tr("Statement has been closed."), PSQLState.OBJECT_NOT_IN_STATE);
      }
      if (methodName.equals("getConnection")) {
        return con.getProxy(); // the proxied connection, not a physical connection
      }

      // Delegate the call to the proxied Statement.
      try {
        return method.invoke(st, args);
      } catch (final InvocationTargetException ite) {
        final Throwable te = ite.getTargetException();
        if (te instanceof SQLException) {
          fireConnectionError((SQLException) te); // Tell listeners about exception if it's fatal
        }
        throw te;
      }
    }
  }

  public void removeStatementEventListener(StatementEventListener listener) {
  }

  public void addStatementEventListener(StatementEventListener listener) {
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getParentLogger()");
  }
}
