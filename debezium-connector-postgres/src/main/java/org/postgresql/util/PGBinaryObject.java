/*-------------------------------------------------------------------------
*
* Copyright (c) 2011, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import java.sql.SQLException;

/**
 * PGBinaryObject is a inteface that classes extending {@link PGobject} can use to take advantage of
 * more optimal binary encoding of the data type.
 */
public interface PGBinaryObject {
  /**
   * This method is called to set the value of this object.
   *
   * @param value data containing the binary representation of the value of the object
   * @param offset the offset in the byte array where object data starts
   * @throws SQLException thrown if value is invalid for this type
   */
  void setByteValue(byte[] value, int offset) throws SQLException;

  /**
   * This method is called to return the number of bytes needed to store this object in the binary
   * form required by org.postgresql.
   *
   * @return the number of bytes needed to store this object
   */
  int lengthInBytes();

  /**
   * This method is called the to store the value of the object, in the binary form required by
   * org.postgresql.
   *
   * @param bytes the array to store the value, it is guaranteed to be at lest
   *        {@link #lengthInBytes} in size.
   * @param offset the offset in the byte array where object must be stored
   */
  void toBytes(byte[] bytes, int offset);
}
