/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.PGNotification;

public class Notification implements PGNotification {
  public Notification(String p_name, int p_pid) {
    this(p_name, p_pid, "");
  }

  public Notification(String p_name, int p_pid, String p_parameter) {
    m_name = p_name;
    m_pid = p_pid;
    m_parameter = p_parameter;
  }

  /*
   * Returns name of this notification
   */
  public String getName() {
    return m_name;
  }

  /*
   * Returns the process id of the backend process making this notification
   */
  public int getPID() {
    return m_pid;
  }

  public String getParameter() {
    return m_parameter;
  }

  private String m_name;
  private String m_parameter;
  private int m_pid;

}

