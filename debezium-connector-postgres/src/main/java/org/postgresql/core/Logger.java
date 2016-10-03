/*-------------------------------------------------------------------------
*
* Copyright (c) 2005-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.Driver;

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Poor man's logging infrastructure. This just deals with maintaining a per- connection ID and log
 * level, and timestamping output.
 */
public final class Logger {
  // For brevity we only log the time, not date or timezone (the main reason
  // for the timestamp is to see delays etc. between log lines, not to pin
  // down an instant in time)
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS ");
  private final FieldPosition dummyPosition = new FieldPosition(0);
  private final StringBuffer buffer = new StringBuffer();
  private final String connectionIDString;

  private int level = 0;

  public Logger() {
    connectionIDString = "(driver) ";
  }

  public Logger(int connectionID) {
    connectionIDString = "(" + connectionID + ") ";
  }

  public void setLogLevel(int level) {
    this.level = level;
  }

  public int getLogLevel() {
    return level;
  }

  public boolean logDebug() {
    return level >= Driver.DEBUG;
  }

  public boolean logInfo() {
    return level >= Driver.INFO;
  }

  public void debug(String str) {
    debug(str, null);
  }

  public void debug(String str, Throwable t) {
    if (logDebug()) {
      log(str, t);
    }
  }

  public void info(String str) {
    info(str, null);
  }

  public void info(String str, Throwable t) {
    if (logInfo()) {
      log(str, t);
    }
  }

  public void log(String str, Throwable t) {
    PrintWriter writer = DriverManager.getLogWriter();
    if (writer == null) {
      return;
    }

    synchronized (this) {
      buffer.setLength(0);
      dateFormat.format(new Date(), buffer, dummyPosition);
      buffer.append(connectionIDString);
      buffer.append(str);

      // synchronize to ensure that the exception (if any) does
      // not get split up from the corresponding log message
      synchronized (writer) {
        writer.println(buffer.toString());
        if (t != null) {
          t.printStackTrace(writer);
        }
      }
      writer.flush();
    }
  }
}
