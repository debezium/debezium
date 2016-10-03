/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.fastpath;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.ParameterList;
import org.postgresql.core.QueryExecutor;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements the Fastpath api.
 *
 * <p>
 * This is a means of executing functions embedded in the backend from within a java application.
 *
 * <p>
 * It is based around the file src/interfaces/libpq/fe-exec.c
 */
public class Fastpath {
  // Java passes oids around as longs, but in the backend
  // it's an unsigned int, so we use this to make the conversion
  // of long -> signed int which the backend interprets as unsigned.
  private final static long NUM_OIDS = 4294967296L; // 2^32

  // This maps the functions names to their id's (possible unique just
  // to a connection).
  private final Map<String, Integer> func = new HashMap<String, Integer>();
  private final QueryExecutor executor;
  private final BaseConnection connection;

  /**
   * Initialises the fastpath system
   *
   * @param conn BaseConnection to attach to
   */
  public Fastpath(BaseConnection conn) {
    this.connection = conn;
    this.executor = conn.getQueryExecutor();
  }

  /**
   * Send a function call to the PostgreSQL backend
   *
   * @param fnId Function id
   * @param resultType True if the result is a numeric (Integer or Long)
   * @param args FastpathArguments to pass to fastpath
   * @return null if no data, Integer if an integer result, Long if a long result, or byte[]
   *         otherwise
   * @throws SQLException if a database-access error occurs.
   * @deprecated please use {@link #fastpath(int, FastpathArg[])}
   */
  @Deprecated
  public Object fastpath(int fnId, boolean resultType, FastpathArg[] args) throws SQLException {
    // Run it.
    byte[] returnValue = fastpath(fnId, args);

    // Interpret results.
    if (!resultType || returnValue == null) {
      return returnValue;
    }

    if (returnValue.length == 4) {
      return ByteConverter.int4(returnValue, 0);
    } else if (returnValue.length == 8) {
      return ByteConverter.int8(returnValue, 0);
    } else {
      throw new PSQLException(
          GT.tr("Fastpath call {0} - No result was returned and we expected a numeric.", fnId),
          PSQLState.NO_DATA);
    }
  }

  /**
   * Send a function call to the PostgreSQL backend
   *
   * @param fnId Function id
   * @param args FastpathArguments to pass to fastpath
   * @return null if no data, byte[] otherwise
   * @throws SQLException if a database-access error occurs.
   */
  public byte[] fastpath(int fnId, FastpathArg[] args) throws SQLException {
    // Turn fastpath array into a parameter list.
    ParameterList params = executor.createFastpathParameters(args.length);
    for (int i = 0; i < args.length; ++i) {
      args[i].populateParameter(params, i + 1);
    }

    // Run it.
    return executor.fastpathCall(fnId, params, connection.getAutoCommit());
  }

  /**
   * @param name Function name
   * @param resulttype True if the result is a numeric (Integer or Long)
   * @param args FastpathArguments to pass to fastpath
   * @return null if no data, Integer if an integer result, Long if a long result, or byte[]
   *         otherwise
   * @throws SQLException if something goes wrong
   * @see #fastpath(int, FastpathArg[])
   * @see #fastpath(String, FastpathArg[])
   * @deprecated Use {@link #getData(String, FastpathArg[])} if you expect a binary result, or one
   *             of {@link #getInteger(String, FastpathArg[])} or
   *             {@link #getLong(String, FastpathArg[])} if you expect a numeric one
   */
  @Deprecated
  public Object fastpath(String name, boolean resulttype, FastpathArg[] args) throws SQLException {
    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("Fastpath: calling " + name);
    }
    return fastpath(getID(name), resulttype, args);
  }

  /**
   * Send a function call to the PostgreSQL backend by name.
   *
   * Note: the mapping for the procedure name to function id needs to exist, usually to an earlier
   * call to addfunction().
   *
   * This is the preferred method to call, as function id's can/may change between versions of the
   * backend.
   *
   * For an example of how this works, refer to org.postgresql.largeobject.LargeObject
   *
   * @param name Function name
   * @param args FastpathArguments to pass to fastpath
   * @return null if no data, byte[] otherwise
   * @throws SQLException if name is unknown or if a database-access error occurs.
   * @see org.postgresql.largeobject.LargeObject
   */
  public byte[] fastpath(String name, FastpathArg[] args) throws SQLException {
    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("Fastpath: calling " + name);
    }
    return fastpath(getID(name), args);
  }

  /**
   * This convenience method assumes that the return value is an integer
   *
   * @param name Function name
   * @param args Function arguments
   * @return integer result
   * @throws SQLException if a database-access error occurs or no result
   */
  public int getInteger(String name, FastpathArg[] args) throws SQLException {
    byte[] returnValue = fastpath(name, args);
    if (returnValue == null) {
      throw new PSQLException(
          GT.tr("Fastpath call {0} - No result was returned and we expected an integer.", name),
          PSQLState.NO_DATA);
    }

    if (returnValue.length == 4) {
      return ByteConverter.int4(returnValue, 0);
    } else {
      throw new PSQLException(GT.tr(
          "Fastpath call {0} - No result was returned or wrong size while expecting an integer.",
          name), PSQLState.NO_DATA);
    }
  }

  /**
   * This convenience method assumes that the return value is a long (bigint)
   *
   * @param name Function name
   * @param args Function arguments
   * @return long result
   * @throws SQLException if a database-access error occurs or no result
   */
  public long getLong(String name, FastpathArg[] args) throws SQLException {
    byte[] returnValue = fastpath(name, args);
    if (returnValue == null) {
      throw new PSQLException(
          GT.tr("Fastpath call {0} - No result was returned and we expected a long.", name),
          PSQLState.NO_DATA);
    }
    if (returnValue.length == 8) {
      return ByteConverter.int8(returnValue, 0);

    } else {
      throw new PSQLException(
          GT.tr("Fastpath call {0} - No result was returned or wrong size while expecting a long.",
              name),
          PSQLState.NO_DATA);
    }
  }

  /**
   * This convenience method assumes that the return value is an oid.
   *
   * @param name Function name
   * @param args Function arguments
   * @return oid of the given call
   * @throws SQLException if a database-access error occurs or no result
   */
  public long getOID(String name, FastpathArg[] args) throws SQLException {
    long oid = getInteger(name, args);
    if (oid < 0) {
      oid += NUM_OIDS;
    }
    return oid;
  }

  /**
   * This convenience method assumes that the return value is not an Integer
   *
   * @param name Function name
   * @param args Function arguments
   * @return byte[] array containing result
   * @throws SQLException if a database-access error occurs or no result
   */
  public byte[] getData(String name, FastpathArg[] args) throws SQLException {
    return fastpath(name, args);
  }

  /**
   * This adds a function to our lookup table.
   *
   * <p>
   * User code should use the addFunctions method, which is based upon a query, rather than hard
   * coding the oid. The oid for a function is not guaranteed to remain static, even on different
   * servers of the same version.
   *
   * @param name Function name
   * @param fnid Function id
   */
  public void addFunction(String name, int fnid) {
    func.put(name, fnid);
  }

  /**
   * This takes a ResultSet containing two columns. Column 1 contains the function name, Column 2
   * the oid.
   *
   * <p>
   * It reads the entire ResultSet, loading the values into the function table.
   *
   * <p>
   * <b>REMEMBER</b> to close() the resultset after calling this!!
   *
   * <p>
   * <b><em>Implementation note about function name lookups:</em></b>
   *
   * <p>
   * PostgreSQL stores the function id's and their corresponding names in the pg_proc table. To
   * speed things up locally, instead of querying each function from that table when required, a
   * HashMap is used. Also, only the function's required are entered into this table, keeping
   * connection times as fast as possible.
   *
   * <p>
   * The org.postgresql.largeobject.LargeObject class performs a query upon it's startup, and passes
   * the returned ResultSet to the addFunctions() method here.
   *
   * <p>
   * Once this has been done, the LargeObject api refers to the functions by name.
   *
   * <p>
   * Dont think that manually converting them to the oid's will work. Ok, they will for now, but
   * they can change during development (there was some discussion about this for V7.0), so this is
   * implemented to prevent any unwarranted headaches in the future.
   *
   * @param rs ResultSet
   * @throws SQLException if a database-access error occurs.
   * @see org.postgresql.largeobject.LargeObjectManager
   */
  public void addFunctions(ResultSet rs) throws SQLException {
    while (rs.next()) {
      func.put(rs.getString(1), rs.getInt(2));
    }
  }

  /**
   * This returns the function id associated by its name.
   *
   * <p>
   * If addFunction() or addFunctions() have not been called for this name, then an SQLException is
   * thrown.
   *
   * @param name Function name to lookup
   * @return Function ID for fastpath call
   * @throws SQLException is function is unknown.
   */
  public int getID(String name) throws SQLException {
    Integer id = func.get(name);

    // may be we could add a lookup to the database here, and store the result
    // in our lookup table, throwing the exception if that fails.
    // We must, however, ensure that if we do, any existing ResultSet is
    // unaffected, otherwise we could break user code.
    //
    // so, until we know we can do this (needs testing, on the TODO list)
    // for now, we throw the exception and do no lookups.
    if (id == null) {
      throw new PSQLException(GT.tr("The fastpath function {0} is unknown.", name),
          PSQLState.UNEXPECTED_ERROR);
    }

    return id;
  }

  /**
   * Creates a FastpathArg with an oid parameter. This is here instead of a constructor of
   * FastpathArg because the constructor can't tell the difference between an long that's really
   * int8 and a long thats an oid.
   *
   * @param oid input oid
   * @return FastpathArg with an oid parameter
   */
  public static FastpathArg createOIDArg(long oid) {
    if (oid > Integer.MAX_VALUE) {
      oid -= NUM_OIDS;
    }
    return new FastpathArg((int) oid);
  }

}

