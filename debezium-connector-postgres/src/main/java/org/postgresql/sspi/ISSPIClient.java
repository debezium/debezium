/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2016, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.sspi;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Use Waffle-JNI to support SSPI authentication when PgJDBC is running on a Windows
 * client and talking to a Windows server.
 *
 * SSPI is not supported on a non-Windows client.
 */
public interface ISSPIClient {
  boolean isSSPISupported();

  void startSSPI() throws SQLException, IOException;

  void continueSSPI(int msgLength) throws SQLException, IOException;

  void dispose();
}
