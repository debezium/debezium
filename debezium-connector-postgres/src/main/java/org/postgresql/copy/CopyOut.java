/*-------------------------------------------------------------------------
*
* Copyright (c) 2009-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.copy;

import java.sql.SQLException;

public interface CopyOut extends CopyOperation {
  /**
   * Blocks wait for a row of data to be received from server on an active copy operation.
   *
   * @return byte array received from server, null if server complete copy operation
   * @throws SQLException if something goes wrong for example socket timeout
   */
  byte[] readFromCopy() throws SQLException;

  /**
   * Wait for a row of data to be received from server on an active copy operation.
   *
   * @param block {@code true} if need wait data from server otherwise {@code false} and will read
   *              pending message from server
   * @return byte array received from server, if pending message from server absent and use no
   * blocking mode return null
   * @throws SQLException if something goes wrong for example socket timeout
   */
  byte[] readFromCopy(boolean block) throws SQLException;
}
