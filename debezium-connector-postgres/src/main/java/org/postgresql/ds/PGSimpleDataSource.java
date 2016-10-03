/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.ds;

import org.postgresql.ds.common.BaseDataSource;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.DataSource;

/**
 * Simple DataSource which does not perform connection pooling. In order to use the DataSource, you
 * must set the property databaseName. The settings for serverName, portNumber, user, and password
 * are optional. Note: these properties are declared in the superclass.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGSimpleDataSource extends BaseDataSource implements DataSource, Serializable {
  /**
   * Gets a description of this DataSource.
   */
  public String getDescription() {
    return "Non-Pooling DataSource from " + org.postgresql.Driver.getVersion();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    writeBaseObject(out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    readBaseObject(in);
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
