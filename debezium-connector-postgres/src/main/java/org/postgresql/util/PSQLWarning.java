/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import java.sql.SQLWarning;

public class PSQLWarning extends SQLWarning {

  private ServerErrorMessage serverError;

  public PSQLWarning(ServerErrorMessage err) {
    this.serverError = err;
  }

  public String toString() {
    return serverError.toString();
  }

  public String getSQLState() {
    return serverError.getSQLState();
  }

  public String getMessage() {
    return serverError.getMessage();
  }

  public ServerErrorMessage getServerErrorMessage() {
    return serverError;
  }
}
