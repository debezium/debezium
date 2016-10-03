/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import org.postgresql.core.EncodingPredictor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ServerErrorMessage implements Serializable {

  private static final Character SEVERITY = 'S';
  private static final Character MESSAGE = 'M';
  private static final Character DETAIL = 'D';
  private static final Character HINT = 'H';
  private static final Character POSITION = 'P';
  private static final Character WHERE = 'W';
  private static final Character FILE = 'F';
  private static final Character LINE = 'L';
  private static final Character ROUTINE = 'R';
  private static final Character SQLSTATE = 'C';
  private static final Character INTERNAL_POSITION = 'p';
  private static final Character INTERNAL_QUERY = 'q';
  private static final Character SCHEMA = 's';
  private static final Character TABLE = 't';
  private static final Character COLUMN = 'c';
  private static final Character DATATYPE = 'd';
  private static final Character CONSTRAINT = 'n';

  private final Map<Character, String> m_mesgParts = new HashMap<Character, String>();
  private final int verbosity;

  public ServerErrorMessage(EncodingPredictor.DecodeResult serverError, int verbosity) {
    this(serverError.result, verbosity);
    if (serverError.encoding != null) {
      m_mesgParts.put(MESSAGE, m_mesgParts.get(MESSAGE)
          + GT.tr(" (pgjdbc: autodetected server-encoding to be {0}, if the message is not readable, please check database logs and/or host, port, dbname, user, password, pg_hba.conf)",
          serverError.encoding)
      );
    }
  }

  public ServerErrorMessage(String p_serverError, int verbosity) {
    this.verbosity = verbosity;

    char[] l_chars = p_serverError.toCharArray();
    int l_pos = 0;
    int l_length = l_chars.length;
    while (l_pos < l_length) {
      char l_mesgType = l_chars[l_pos];
      if (l_mesgType != '\0') {
        l_pos++;
        int l_startString = l_pos;
        // order here is important position must be checked before accessing the array
        while (l_pos < l_length && l_chars[l_pos] != '\0') {
          l_pos++;
        }
        String l_mesgPart = new String(l_chars, l_startString, l_pos - l_startString);
        m_mesgParts.put(l_mesgType, l_mesgPart);
      }
      l_pos++;
    }
  }

  public String getSQLState() {
    return m_mesgParts.get(SQLSTATE);
  }

  public String getMessage() {
    return m_mesgParts.get(MESSAGE);
  }

  public String getSeverity() {
    return m_mesgParts.get(SEVERITY);
  }

  public String getDetail() {
    return m_mesgParts.get(DETAIL);
  }

  public String getHint() {
    return m_mesgParts.get(HINT);
  }

  public int getPosition() {
    return getIntegerPart(POSITION);
  }

  public String getWhere() {
    return m_mesgParts.get(WHERE);
  }

  public String getSchema() {
    return m_mesgParts.get(SCHEMA);
  }

  public String getTable() {
    return m_mesgParts.get(TABLE);
  }

  public String getColumn() {
    return m_mesgParts.get(COLUMN);
  }

  public String getDatatype() {
    return m_mesgParts.get(DATATYPE);
  }

  public String getConstraint() {
    return m_mesgParts.get(CONSTRAINT);
  }

  public String getFile() {
    return m_mesgParts.get(FILE);
  }

  public int getLine() {
    return getIntegerPart(LINE);
  }

  public String getRoutine() {
    return m_mesgParts.get(ROUTINE);
  }

  public String getInternalQuery() {
    return m_mesgParts.get(INTERNAL_QUERY);
  }

  public int getInternalPosition() {
    return getIntegerPart(INTERNAL_POSITION);
  }

  private int getIntegerPart(Character c) {
    String s = m_mesgParts.get(c);
    if (s == null) {
      return 0;
    }
    return Integer.parseInt(s);
  }

  public String toString() {
    // Now construct the message from what the server sent
    // The general format is:
    // SEVERITY: Message \n
    // Detail: \n
    // Hint: \n
    // Position: \n
    // Where: \n
    // Internal Query: \n
    // Internal Position: \n
    // Location: File:Line:Routine \n
    // SQLState: \n
    //
    // Normally only the message and detail is included.
    // If INFO level logging is enabled then detail, hint, position and where are
    // included. If DEBUG level logging is enabled then all information
    // is included.

    StringBuilder l_totalMessage = new StringBuilder();
    String l_message = m_mesgParts.get(SEVERITY);
    if (l_message != null) {
      l_totalMessage.append(l_message).append(": ");
    }
    l_message = m_mesgParts.get(MESSAGE);
    if (l_message != null) {
      l_totalMessage.append(l_message);
    }
    l_message = m_mesgParts.get(DETAIL);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Detail: {0}", l_message));
    }

    l_message = m_mesgParts.get(HINT);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Hint: {0}", l_message));
    }
    l_message = m_mesgParts.get(POSITION);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Position: {0}", l_message));
    }
    l_message = m_mesgParts.get(WHERE);
    if (l_message != null) {
      l_totalMessage.append("\n  ").append(GT.tr("Where: {0}", l_message));
    }

    if (verbosity > 2) {
      String l_internalQuery = m_mesgParts.get(INTERNAL_QUERY);
      if (l_internalQuery != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Internal Query: {0}", l_internalQuery));
      }
      String l_internalPosition = m_mesgParts.get(INTERNAL_POSITION);
      if (l_internalPosition != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Internal Position: {0}", l_internalPosition));
      }

      String l_file = m_mesgParts.get(FILE);
      String l_line = m_mesgParts.get(LINE);
      String l_routine = m_mesgParts.get(ROUTINE);
      if (l_file != null || l_line != null || l_routine != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Location: File: {0}, Routine: {1}, Line: {2}",
            new Object[]{l_file, l_routine, l_line}));
      }
      l_message = m_mesgParts.get(SQLSTATE);
      if (l_message != null) {
        l_totalMessage.append("\n  ").append(GT.tr("Server SQLState: {0}", l_message));
      }
    }

    return l_totalMessage.toString();
  }
}
