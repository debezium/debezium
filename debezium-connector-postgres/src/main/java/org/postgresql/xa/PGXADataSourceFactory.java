/*-------------------------------------------------------------------------
*
* Copyright (c) 2007-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.xa;

import org.postgresql.ds.common.PGObjectFactory;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;

/**
 * An ObjectFactory implementation for PGXADataSource-objects.
 */

public class PGXADataSourceFactory extends PGObjectFactory {
  /*
   * All the other PostgreSQL DataSource use PGObjectFactory directly, but we can't do that with
   * PGXADataSource because referencing PGXADataSource from PGObjectFactory would break
   * "JDBC2 Enterprise" edition build which doesn't include PGXADataSource.
   */

  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws Exception {
    Reference ref = (Reference) obj;
    String className = ref.getClassName();
    if (className.equals("org.postgresql.xa.PGXADataSource")) {
      return loadXADataSource(ref);
    } else {
      return null;
    }
  }

  private Object loadXADataSource(Reference ref) {
    PGXADataSource ds = new PGXADataSource();
    return loadBaseDataSource(ds, ref);
  }
}
