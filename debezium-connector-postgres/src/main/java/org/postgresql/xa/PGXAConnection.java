/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*-------------------------------------------------------------------------
*/

package org.postgresql.xa;

import org.postgresql.PGConnection;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.Logger;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.TransactionState;
import org.postgresql.ds.PGPooledConnection;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * The PostgreSQL implementation of {@link XAResource}.
 *
 * This implementation doesn't support transaction interleaving (see JTA specification, section
 * 3.4.4) and suspend/resume.
 *
 * Two-phase commit requires PostgreSQL server version 8.1 or higher.
 *
 * @author Heikki Linnakangas (heikki.linnakangas@iki.fi)
 */
public class PGXAConnection extends PGPooledConnection implements XAConnection, XAResource {
  /**
   * Underlying physical database connection. It's used for issuing PREPARE TRANSACTION/ COMMIT
   * PREPARED/ROLLBACK PREPARED commands.
   */
  private final BaseConnection conn;
  private final Logger logger;

  /*
   * PGXAConnection-object can be in one of three states:
   *
   * IDLE Not associated with a XA-transaction. You can still call getConnection and use the
   * connection outside XA. currentXid is null. autoCommit is true on a connection by getConnection,
   * per normal JDBC rules, though the caller can change it to false and manage transactions itself
   * using Connection.commit and rollback.
   *
   * ACTIVE start has been called, and we're associated with an XA transaction. currentXid is valid.
   * autoCommit is false on a connection returned by getConnection, and should not be messed with by
   * the caller or the XA transaction will be broken.
   *
   * ENDED end has been called, but the transaction has not yet been prepared. currentXid is still
   * valid. You shouldn't use the connection for anything else than issuing a XAResource.commit or
   * rollback.
   */
  private Xid currentXid;

  private int state;

  private static final int STATE_IDLE = 0;
  private static final int STATE_ACTIVE = 1;
  private static final int STATE_ENDED = 2;

  /*
   * When an XA transaction is started, we put the underlying connection into non-autocommit mode.
   * The old setting is saved in localAutoCommitMode, so that we can restore it when the XA
   * transaction ends and the connection returns into local transaction mode.
   */
  private boolean localAutoCommitMode = true;

  private void debug(String s) {
    logger.debug("XAResource " + Integer.toHexString(this.hashCode()) + ": " + s);
  }

  public PGXAConnection(BaseConnection conn) throws SQLException {
    super(conn, true, true);
    this.conn = conn;
    this.state = STATE_IDLE;
    this.logger = conn.getLogger();
  }


  /****
   * XAConnection interface
   ****/

  public Connection getConnection() throws SQLException {
    if (logger.logDebug()) {
      debug("PGXAConnection.getConnection called");
    }

    Connection conn = super.getConnection();

    // When we're outside an XA transaction, autocommit
    // is supposed to be true, per usual JDBC convention.
    // When an XA transaction is in progress, it should be
    // false.
    if (state == STATE_IDLE) {
      conn.setAutoCommit(true);
    }

    /*
     * Wrap the connection in a proxy to forbid application from fiddling with transaction state
     * directly during an XA transaction
     */
    ConnectionHandler handler = new ConnectionHandler(conn);
    return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class[]{Connection.class, PGConnection.class}, handler);
  }

  public XAResource getXAResource() {
    return this;
  }

  /*
   * A java.sql.Connection proxy class to forbid calls to transaction control methods while the
   * connection is used for an XA transaction.
   */
  private class ConnectionHandler implements InvocationHandler {
    private Connection con;

    public ConnectionHandler(Connection con) {
      this.con = con;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (state != STATE_IDLE) {
        String methodName = method.getName();
        if (methodName.equals("commit")
            || methodName.equals("rollback")
            || methodName.equals("setSavePoint")
            || (methodName.equals("setAutoCommit") && (Boolean) args[0])) {
          throw new PSQLException(
              GT.tr(
                  "Transaction control methods setAutoCommit(true), commit, rollback and setSavePoint not allowed while an XA transaction is active."),
              PSQLState.OBJECT_NOT_IN_STATE);
        }
      }
      try {
        /*
         * If the argument to equals-method is also a wrapper, present the original unwrapped
         * connection to the underlying equals method.
         */
        if (method.getName().equals("equals")) {
          Object arg = args[0];
          if (Proxy.isProxyClass(arg.getClass())) {
            InvocationHandler h = Proxy.getInvocationHandler(arg);
            if (h instanceof ConnectionHandler) {
              // unwrap argument
              args = new Object[]{((ConnectionHandler) h).con};
            }
          }
        }

        return method.invoke(con, args);
      } catch (InvocationTargetException ex) {
        throw ex.getTargetException();
      }
    }
  }


  /**** XAResource interface ****/

  /**
   * Preconditions: 1. flags must be one of TMNOFLAGS, TMRESUME or TMJOIN 2. xid != null 3.
   * connection must not be associated with a transaction 4. the TM hasn't seen the xid before
   *
   * Implementation deficiency preconditions: 1. TMRESUME not supported. 2. if flags is TMJOIN, we
   * must be in ended state, and xid must be the current transaction 3. unless flags is TMJOIN,
   * previous transaction using the connection must be committed or prepared or rolled back
   *
   * Postconditions: 1. Connection is associated with the transaction
   */
  public void start(Xid xid, int flags) throws XAException {
    if (logger.logDebug()) {
      debug("starting transaction xid = " + xid);
    }

    // Check preconditions
    if (flags != XAResource.TMNOFLAGS && flags != XAResource.TMRESUME
        && flags != XAResource.TMJOIN) {
      throw new PGXAException(GT.tr("Invalid flags"), XAException.XAER_INVAL);
    }

    if (xid == null) {
      throw new PGXAException(GT.tr("xid must not be null"), XAException.XAER_INVAL);
    }

    if (state == STATE_ACTIVE) {
      throw new PGXAException(GT.tr("Connection is busy with another transaction"),
          XAException.XAER_PROTO);
    }

    // We can't check precondition 4 easily, so we don't. Duplicate xid will be catched in prepare
    // phase.

    // Check implementation deficiency preconditions
    if (flags == TMRESUME) {
      throw new PGXAException(GT.tr("suspend/resume not implemented"), XAException.XAER_RMERR);
    }

    // It's ok to join an ended transaction. WebLogic does that.
    if (flags == TMJOIN) {
      if (state != STATE_ENDED) {
        throw new PGXAException(GT.tr("Transaction interleaving not implemented"),
            XAException.XAER_RMERR);
      }

      if (!xid.equals(currentXid)) {
        throw new PGXAException(GT.tr("Transaction interleaving not implemented"),
            XAException.XAER_RMERR);
      }
    } else if (state == STATE_ENDED) {
      throw new PGXAException(GT.tr("Transaction interleaving not implemented"),
          XAException.XAER_RMERR);
    }

    // Only need save localAutoCommitMode for NOFLAGS, TMRESUME and TMJOIN already saved old
    // localAutoCommitMode.
    if (flags == TMNOFLAGS) {
      try {
        localAutoCommitMode = conn.getAutoCommit();
        conn.setAutoCommit(false);
      } catch (SQLException ex) {
        throw new PGXAException(GT.tr("Error disabling autocommit"), ex, XAException.XAER_RMERR);
      }
    }

    // Preconditions are met, Associate connection with the transaction
    state = STATE_ACTIVE;
    currentXid = xid;
  }

  /**
   * Preconditions: 1. Flags is one of TMSUCCESS, TMFAIL, TMSUSPEND 2. xid != null 3. Connection is
   * associated with transaction xid
   *
   * Implementation deficiency preconditions: 1. Flags is not TMSUSPEND
   *
   * Postconditions: 1. connection is disassociated from the transaction.
   */
  public void end(Xid xid, int flags) throws XAException {
    if (logger.logDebug()) {
      debug("ending transaction xid = " + xid);
    }

    // Check preconditions

    if (flags != XAResource.TMSUSPEND && flags != XAResource.TMFAIL
        && flags != XAResource.TMSUCCESS) {
      throw new PGXAException(GT.tr("Invalid flags"), XAException.XAER_INVAL);
    }

    if (xid == null) {
      throw new PGXAException(GT.tr("xid must not be null"), XAException.XAER_INVAL);
    }

    if (state != STATE_ACTIVE || !currentXid.equals(xid)) {
      throw new PGXAException(GT.tr("tried to call end without corresponding start call"),
          XAException.XAER_PROTO);
    }

    // Check implementation deficiency preconditions
    if (flags == XAResource.TMSUSPEND) {
      throw new PGXAException(GT.tr("suspend/resume not implemented"), XAException.XAER_RMERR);
    }

    // We ignore TMFAIL. It's just a hint to the RM. We could roll back immediately
    // if TMFAIL was given.

    // All clear. We don't have any real work to do.
    state = STATE_ENDED;
  }

  /**
   * Preconditions: 1. xid != null 2. xid is in ended state
   *
   * Implementation deficiency preconditions: 1. xid was associated with this connection
   *
   * Postconditions: 1. Transaction is prepared
   */
  public int prepare(Xid xid) throws XAException {
    if (logger.logDebug()) {
      debug("preparing transaction xid = " + xid);
    }

    // Check preconditions
    if (!currentXid.equals(xid)) {
      throw new PGXAException(
          GT.tr(
              "Not implemented: Prepare must be issued using the same connection that started the transaction"),
          XAException.XAER_RMERR);
    }
    if (state != STATE_ENDED) {
      throw new PGXAException(GT.tr("Prepare called before end"), XAException.XAER_INVAL);
    }

    state = STATE_IDLE;
    currentXid = null;

    if (!conn.haveMinimumServerVersion(ServerVersion.v8_1)) {
      throw new PGXAException(
          GT.tr("Server versions prior to 8.1 do not support two-phase commit."),
          XAException.XAER_RMERR);
    }

    try {
      String s = RecoveredXid.xidToString(xid);

      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate("PREPARE TRANSACTION '" + s + "'");
      } finally {
        stmt.close();
      }
      conn.setAutoCommit(localAutoCommitMode);

      return XA_OK;
    } catch (SQLException ex) {
      throw new PGXAException(GT.tr("Error preparing transaction"), ex, XAException.XAER_RMERR);
    }
  }

  /**
   * Preconditions: 1. flag must be one of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS or TMSTARTTRSCAN |
   * TMENDRSCAN 2. if flag isn't TMSTARTRSCAN or TMSTARTRSCAN | TMENDRSCAN, a recovery scan must be
   * in progress
   *
   * Postconditions: 1. list of prepared xids is returned
   */
  public Xid[] recover(int flag) throws XAException {
    // Check preconditions
    if (flag != TMSTARTRSCAN && flag != TMENDRSCAN && flag != TMNOFLAGS
        && flag != (TMSTARTRSCAN | TMENDRSCAN)) {
      throw new PGXAException(GT.tr("Invalid flag"), XAException.XAER_INVAL);
    }

    // We don't check for precondition 2, because we would have to add some additional state in
    // this object to keep track of recovery scans.

    // All clear. We return all the xids in the first TMSTARTRSCAN call, and always return
    // an empty array otherwise.
    if ((flag & TMSTARTRSCAN) == 0) {
      return new Xid[0];
    } else {
      try {
        Statement stmt = conn.createStatement();
        try {
          // If this connection is simultaneously used for a transaction,
          // this query gets executed inside that transaction. It's OK,
          // except if the transaction is in abort-only state and the
          // backed refuses to process new queries. Hopefully not a problem
          // in practise.
          ResultSet rs = stmt.executeQuery(
              "SELECT gid FROM pg_prepared_xacts where database = current_database()");
          LinkedList<Xid> l = new LinkedList<Xid>();
          while (rs.next()) {
            Xid recoveredXid = RecoveredXid.stringToXid(rs.getString(1));
            if (recoveredXid != null) {
              l.add(recoveredXid);
            }
          }
          rs.close();

          return l.toArray(new Xid[l.size()]);
        } finally {
          stmt.close();
        }
      } catch (SQLException ex) {
        throw new PGXAException(GT.tr("Error during recover"), ex, XAException.XAER_RMERR);
      }
    }
  }

  /**
   * Preconditions: 1. xid is known to the RM or it's in prepared state
   *
   * Implementation deficiency preconditions: 1. xid must be associated with this connection if it's
   * not in prepared state.
   *
   * Postconditions: 1. Transaction is rolled back and disassociated from connection
   */
  public void rollback(Xid xid) throws XAException {
    if (logger.logDebug()) {
      debug("rolling back xid = " + xid);
    }

    // We don't explicitly check precondition 1.

    try {
      if (currentXid != null && xid.equals(currentXid)) {
        state = STATE_IDLE;
        currentXid = null;
        conn.rollback();
        conn.setAutoCommit(localAutoCommitMode);
      } else {
        String s = RecoveredXid.xidToString(xid);

        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        try {
          stmt.executeUpdate("ROLLBACK PREPARED '" + s + "'");
        } finally {
          stmt.close();
        }
      }
    } catch (SQLException ex) {
      if (PSQLState.UNDEFINED_OBJECT.getState().equals(ex.getSQLState())) {
        throw new PGXAException(GT.tr("Error rolling back prepared transaction"), ex,
            XAException.XAER_NOTA);
      }
      throw new PGXAException(GT.tr("Error rolling back prepared transaction"), ex,
          XAException.XAER_RMERR);
    }
  }

  public void commit(Xid xid, boolean onePhase) throws XAException {
    if (logger.logDebug()) {
      debug("committing xid = " + xid + (onePhase ? " (one phase) " : " (two phase)"));
    }

    if (xid == null) {
      throw new PGXAException(GT.tr("xid must not be null"), XAException.XAER_INVAL);
    }

    if (onePhase) {
      commitOnePhase(xid);
    } else {
      commitPrepared(xid);
    }
  }

  /**
   * Preconditions: 1. xid must in ended state.
   *
   * Implementation deficiency preconditions: 1. this connection must have been used to run the
   * transaction
   *
   * Postconditions: 1. Transaction is committed
   */
  private void commitOnePhase(Xid xid) throws XAException {
    try {
      // Check preconditions
      if (currentXid == null || !currentXid.equals(xid)) {
        // In fact, we don't know if xid is bogus, or if it just wasn't associated with this
        // connection.
        // Assume it's our fault.
        throw new PGXAException(
            GT.tr(
                "Not implemented: one-phase commit must be issued using the same connection that was used to start it"),
            XAException.XAER_RMERR);
      }
      if (state != STATE_ENDED) {
        throw new PGXAException(GT.tr("commit called before end"), XAException.XAER_PROTO);
      }

      // Preconditions are met. Commit
      state = STATE_IDLE;
      currentXid = null;

      conn.commit();
      conn.setAutoCommit(localAutoCommitMode);
    } catch (SQLException ex) {
      throw new PGXAException(GT.tr("Error during one-phase commit"), ex, XAException.XAER_RMERR);
    }
  }

  /**
   * Preconditions: 1. xid must be in prepared state in the server
   *
   * Implementation deficiency preconditions: 1. Connection must be in idle state
   *
   * Postconditions: 1. Transaction is committed
   */
  private void commitPrepared(Xid xid) throws XAException {
    try {
      // Check preconditions. The connection mustn't be used for another
      // other XA or local transaction, or the COMMIT PREPARED command
      // would mess it up.
      if (state != STATE_IDLE
          || conn.getTransactionState() != TransactionState.IDLE) {
        throw new PGXAException(
            GT.tr("Not implemented: 2nd phase commit must be issued using an idle connection"),
            XAException.XAER_RMERR);
      }

      String s = RecoveredXid.xidToString(xid);

      localAutoCommitMode = conn.getAutoCommit();
      conn.setAutoCommit(true);
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate("COMMIT PREPARED '" + s + "'");
      } finally {
        stmt.close();
        conn.setAutoCommit(localAutoCommitMode);
      }
    } catch (SQLException ex) {
      throw new PGXAException(GT.tr("Error committing prepared transaction"), ex,
          XAException.XAER_RMERR);
    }
  }

  public boolean isSameRM(XAResource xares) throws XAException {
    // This trivial implementation makes sure that the
    // application server doesn't try to use another connection
    // for prepare, commit and rollback commands.
    return xares == this;
  }

  /**
   * Does nothing, since we don't do heuristics,
   */
  public void forget(Xid xid) throws XAException {
    throw new PGXAException(GT.tr("Heuristic commit/rollback not supported"),
        XAException.XAER_NOTA);
  }

  /**
   * We don't do transaction timeouts. Just returns 0.
   */
  public int getTransactionTimeout() {
    return 0;
  }

  /**
   * We don't do transaction timeouts. Returns false.
   */
  public boolean setTransactionTimeout(int seconds) {
    return false;
  }
}
