/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.fastpath;

import org.postgresql.core.ParameterList;

import java.sql.SQLException;

// Not a very clean mapping to the new QueryExecutor/ParameterList
// stuff, but it seems hard to support both v2 and v3 cleanly with
// the same model while retaining API compatibility. So I've just
// done it the ugly way..

/**
 * Each fastpath call requires an array of arguments, the number and type dependent on the function
 * being called.
 */
public class FastpathArg {
  /**
   * Encoded byte value of argument.
   */
  private final byte[] bytes;
  private final int bytesStart;
  private final int bytesLength;

  /**
   * Constructs an argument that consists of an integer value
   *
   * @param value int value to set
   */
  public FastpathArg(int value) {
    bytes = new byte[4];
    bytes[3] = (byte) (value);
    bytes[2] = (byte) (value >> 8);
    bytes[1] = (byte) (value >> 16);
    bytes[0] = (byte) (value >> 24);
    bytesStart = 0;
    bytesLength = 4;
  }

  /**
   * Constructs an argument that consists of an integer value
   *
   * @param value int value to set
   */
  public FastpathArg(long value) {
    bytes = new byte[8];
    bytes[7] = (byte) (value);
    bytes[6] = (byte) (value >> 8);
    bytes[5] = (byte) (value >> 16);
    bytes[4] = (byte) (value >> 24);
    bytes[3] = (byte) (value >> 32);
    bytes[2] = (byte) (value >> 40);
    bytes[1] = (byte) (value >> 48);
    bytes[0] = (byte) (value >> 56);
    bytesStart = 0;
    bytesLength = 8;
  }

  /**
   * Constructs an argument that consists of an array of bytes
   *
   * @param bytes array to store
   */
  public FastpathArg(byte bytes[]) {
    this(bytes, 0, bytes.length);
  }

  /**
   * Constructs an argument that consists of part of a byte array
   *
   * @param buf source array
   * @param off offset within array
   * @param len length of data to include
   */
  public FastpathArg(byte buf[], int off, int len) {
    this.bytes = buf;
    this.bytesStart = off;
    this.bytesLength = len;
  }

  /**
   * Constructs an argument that consists of a String.
   *
   * @param s String to store
   */
  public FastpathArg(String s) {
    this(s.getBytes());
  }

  void populateParameter(ParameterList params, int index) throws SQLException {
    if (bytes == null) {
      params.setNull(index, 0);
    } else {
      params.setBytea(index, bytes, bytesStart, bytesLength);
    }
  }
}

