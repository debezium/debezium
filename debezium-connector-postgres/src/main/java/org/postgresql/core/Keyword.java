/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.core;

/**
 * Reserved SQL keywords.
 * @author Jeremy Whiting jwhiting@redhat.com
 *
 */
public enum Keyword {
  RETURNING("RETURNING"), INTO("INTO"), VALUES("VALUES"), GROUP_BY("GROUP BY");
  /* add when needed: FROM, WHERE, HAVING, ONLY, ORDER, JOIN, INNER
  , LEFT, RIGHT, OUTER, LIMIT, OFFSET;*/

  Keyword(String upperCaseWord) {
    keyword = upperCaseWord;
  }

  public String asLowerCase() {
    return keyword.toLowerCase();
  }

  public String toString() {
    return keyword;
  }

  private final String keyword;
}
