/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql;

/**
 * This interface defines the public PostgreSQL extension for Notifications
 */
public interface PGNotification {
  /**
   * Returns name of this notification
   *
   * @return name of this notification
   * @since 7.3
   */
  String getName();

  /**
   * Returns the process id of the backend process making this notification
   *
   * @return process id of the backend process making this notification
   * @since 7.3
   */
  int getPID();

  /**
   * Returns additional information from the notifying process. This feature has only been
   * implemented in server versions 9.0 and later, so previous versions will always return an empty
   * String.
   *
   * @return additional information from the notifying process
   * @since 8.0
   */
  String getParameter();
}
