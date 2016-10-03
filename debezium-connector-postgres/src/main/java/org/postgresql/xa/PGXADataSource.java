/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*-------------------------------------------------------------------------
*/

package org.postgresql.xa;

import org.postgresql.core.BaseConnection;
import org.postgresql.ds.common.BaseDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 * XA-enabled DataSource implementation.
 *
 * @author Heikki Linnakangas (heikki.linnakangas@iki.fi)
 */
public class PGXADataSource extends BaseDataSource implements XADataSource, Referenceable {
  /**
   * Gets a connection to the PostgreSQL database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the DataSource properties user and password.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public XAConnection getXAConnection() throws SQLException {
    return getXAConnection(getUser(), getPassword());
  }

  /**
   * Gets a XA-enabled connection to the PostgreSQL database. The database is identified by the
   * DataSource properties serverName, databaseName, and portNumber. The user to connect as is
   * identified by the arguments user and password, which override the DataSource properties by the
   * same name.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public XAConnection getXAConnection(String user, String password) throws SQLException {
    Connection con = super.getConnection(user, password);
    return new PGXAConnection((BaseConnection) con);
  }

  public String getDescription() {
    return "JDBC3 XA-enabled DataSource from " + org.postgresql.Driver.getVersion();
  }

  /**
   * Generates a reference using the appropriate object factory.
   */
  protected Reference createReference() {
    return new Reference(getClass().getName(), PGXADataSourceFactory.class.getName(), null);
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getParentLogger()");
  }
}
