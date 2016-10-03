/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Abstraction of a list of parameters to be substituted into a Query. The protocol-specific details
 * of how to efficiently store and stream the parameters is hidden behind implementations of this
 * interface.
 * <p>
 * In general, instances of ParameterList are associated with a particular Query object (the one
 * that created them) and shouldn't be used against another Query.
 * <p>
 * Parameter indexes are 1-based to match JDBC's PreparedStatement, i.e. the first parameter has
 * index 1.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface ParameterList {


  void registerOutParameter(int index, int sqlType) throws SQLException;

  /**
   * Get the number of parameters in this list. This value never changes for a particular instance,
   * and might be zero.
   *
   * @return the number of parameters in this list.
   */
  int getParameterCount();

  /**
   * Get the number of IN parameters in this list.
   *
   * @return the number of IN parameters in this list
   */
  int getInParameterCount();

  /**
   * Get the number of OUT parameters in this list.
   *
   * @return the number of OUT parameters in this list
   */
  int getOutParameterCount();

  /**
   * Return the oids of the parameters in this list. May be null for a ParameterList that does not
   * support typing of parameters.
   *
   * @return oids of the parameters
   */
  int[] getTypeOIDs();

  /**
   * Binds an integer value to a parameter. The type of the parameter is implicitly 'int4'.
   *
   * @param index the 1-based parameter index to bind.
   * @param value the integer value to use.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setIntParameter(int index, int value) throws SQLException;

  /**
   * Binds a String value that is an unquoted literal to the server's query parser (for example, a
   * bare integer) to a parameter. Associated with the parameter is a typename for the parameter
   * that should correspond to an entry in pg_types.
   *
   * @param index the 1-based parameter index to bind.
   * @param value the unquoted literal string to use.
   * @param oid the type OID of the parameter, or <code>0</code> to infer the type.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setLiteralParameter(int index, String value, int oid) throws SQLException;

  /**
   * Binds a String value that needs to be quoted for the server's parser to understand (for
   * example, a timestamp) to a parameter. Associated with the parameter is a typename for the
   * parameter that should correspond to an entry in pg_types.
   *
   * @param index the 1-based parameter index to bind.
   * @param value the quoted string to use.
   * @param oid the type OID of the parameter, or <code>0</code> to infer the type.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setStringParameter(int index, String value, int oid) throws SQLException;

  /**
   * Binds a binary bytea value stored as a bytearray to a parameter. The parameter's type is
   * implicitly set to 'bytea'. The bytearray's contains should remain unchanged until query
   * execution has completed.
   *
   * @param index the 1-based parameter index to bind.
   * @param data an array containing the raw data value
   * @param offset the offset within <code>data</code> of the start of the parameter data.
   * @param length the number of bytes of parameter data within <code>data</code> to use.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setBytea(int index, byte[] data, int offset, int length) throws SQLException;

  /**
   * Binds a binary bytea value stored as an InputStream. The parameter's type is implicitly set to
   * 'bytea'. The stream should remain valid until query execution has completed.
   *
   * @param index the 1-based parameter index to bind.
   * @param stream a stream containing the parameter data.
   * @param length the number of bytes of parameter data to read from <code>stream</code>.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setBytea(int index, InputStream stream, int length) throws SQLException;

  /**
   * Binds a binary bytea value stored as an InputStream. The parameter's type is implicitly set to
   * 'bytea'. The stream should remain valid until query execution has completed.
   *
   * @param index the 1-based parameter index to bind.
   * @param stream a stream containing the parameter data.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setBytea(int index, InputStream stream) throws SQLException;

  /**
   * Binds given byte[] value to a parameter. The bytes must already be in correct format matching
   * the OID.
   *
   * @param index the 1-based parameter index to bind.
   * @param value the bytes to send.
   * @param oid the type OID of the parameter.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setBinaryParameter(int index, byte[] value, int oid) throws SQLException;

  /**
   * Binds a SQL NULL value to a parameter. Associated with the parameter is a typename for the
   * parameter that should correspond to an entry in pg_types.
   *
   * @param index the 1-based parameter index to bind.
   * @param oid the type OID of the parameter, or <code>0</code> to infer the type.
   * @throws SQLException on error or if <code>index</code> is out of range
   */
  void setNull(int index, int oid) throws SQLException;

  /**
   * Perform a shallow copy of this ParameterList, returning a new instance (still suitable for
   * passing to the owning Query). If this ParameterList is immutable, copy() may return the same
   * immutable object.
   *
   * @return a new ParameterList instance
   */
  ParameterList copy();

  /**
   * Unbind all parameter values bound in this list.
   */
  void clear();

  /**
   * Return a human-readable representation of a particular parameter in this ParameterList. If the
   * parameter is not bound, returns "?".
   *
   * @param index the 1-based parameter index to bind.
   * @param standardConformingStrings true if \ is not an escape character in strings literals
   * @return a string representation of the parameter.
   */
  String toString(int index, boolean standardConformingStrings);

  /**
   * Use this operation to append more parameters to the current list.
   * @param list of parameters to append with.
   * @throws SQLException fault raised if driver or back end throw an exception
   */
  void appendAll(ParameterList list) throws SQLException ;

  /**
   * Returns the bound parameter values.
   * @return Object array containing the parameter values.
   */
  Object[] getValues();
}
