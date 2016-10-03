/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2003-2016, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.core;

import static org.postgresql.core.SqlCommandType.INSERT;

/**
 * Data Modification Language inspection support.
 *
 * @author Jeremy Whiting jwhiting@redhat.com
 * @author Christopher Deckers (chrriis@gmail.com)
 *
 */
public class SqlCommand {
  public static final SqlCommand BLANK = SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK);

  public boolean isBatchedReWriteCompatible() {
    return valuesBraceOpenPosition >= 0;
  }

  public int getBatchRewriteValuesBraceOpenPosition() {
    return valuesBraceOpenPosition;
  }

  public int getBatchRewriteValuesBraceClosePosition() {
    return valuesBraceClosePosition;
  }

  public SqlCommandType getType() {
    return commandType;
  }

  public boolean isReturningKeywordPresent() {
    return parsedSQLhasRETURNINGKeyword;
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type,
      boolean isBatchedReWritePropertyConfigured,
      int valuesBraceOpenPosition, int valuesBraceClosePosition, boolean isRETURNINGkeywordPresent,
      int priorQueryCount) {
    return new SqlCommand(type, isBatchedReWritePropertyConfigured,
        valuesBraceOpenPosition, valuesBraceClosePosition, isRETURNINGkeywordPresent,
        priorQueryCount);
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type) {
    return new SqlCommand(type, false, -1, -1, false, 0);
  }

  public static SqlCommand createStatementTypeInfo(SqlCommandType type,
      boolean isRETURNINGkeywordPresent) {
    return new SqlCommand(type, false, -1, -1, isRETURNINGkeywordPresent, 0);
  }

  private SqlCommand(SqlCommandType type, boolean isBatchedReWriteConfigured,
      int valuesBraceOpenPosition, int valuesBraceClosePosition, boolean isPresent,
      int priorQueryCount) {
    commandType = type;
    parsedSQLhasRETURNINGKeyword = isPresent;
    boolean batchedReWriteCompatible = (type == INSERT) && isBatchedReWriteConfigured
        && valuesBraceOpenPosition >= 0 && valuesBraceClosePosition > valuesBraceOpenPosition
        && !isPresent && priorQueryCount == 0;
    this.valuesBraceOpenPosition = batchedReWriteCompatible ? valuesBraceOpenPosition : -1;
    this.valuesBraceClosePosition = batchedReWriteCompatible ? valuesBraceClosePosition : -1;
  }

  private final SqlCommandType commandType;
  private final boolean parsedSQLhasRETURNINGKeyword;
  private final int valuesBraceOpenPosition;
  private final int valuesBraceClosePosition;

}
