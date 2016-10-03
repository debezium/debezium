/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.copy;

import java.sql.SQLException;

/**
 * Exchange bulk data between client and PostgreSQL database tables. See CopyIn and CopyOut for full
 * interfaces for corresponding copy directions.
 */
public interface CopyOperation {

  /**
   * @return number of fields in each row for this operation
   */
  int getFieldCount();

  /**
   * @return overall format of each row: 0 = textual, 1 = binary
   */
  int getFormat();

  /**
   * @param field number of field (0..fieldCount()-1)
   * @return format of requested field: 0 = textual, 1 = binary
   */
  int getFieldFormat(int field);

  /**
   * @return is connection reserved for this Copy operation?
   */
  boolean isActive();

  /**
   * Cancels this copy operation, discarding any exchanged data.
   *
   * @throws SQLException if cancelling fails
   */
  void cancelCopy() throws SQLException;

  /**
   * After succesful end of copy, returns the number of database records handled in that operation.
   * Only implemented in PostgreSQL server version 8.2 and up. Otherwise, returns -1.
   *
   * @return number of handled rows or -1
   */
  long getHandledRowCount();
}
