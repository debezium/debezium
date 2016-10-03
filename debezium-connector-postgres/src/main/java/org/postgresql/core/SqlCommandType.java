/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.core;

/**
 * Type information inspection support.
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */

public enum SqlCommandType {

  /**
   * Use BLANK for empty sql queries or when parsing the sql string is not
   * necessary.
   */
  BLANK,
  INSERT,
  UPDATE,
  DELETE,
  MOVE,
  SELECT,
  WITH;
}
