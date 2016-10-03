package org.postgresql.core;

/**
 * This class is used as a cache key for simple statements that have no "returning columns".
 * Prepared statements that have no returning columns use just {@code String sql} as a key.
 * Simple and Prepared statements that have returning columns use {@link QueryWithReturningColumnsKey}
 * as a cache key.
 */
class BaseQueryKey {
  public final String sql;
  public final boolean isParameterized;
  public final boolean escapeProcessing;

  BaseQueryKey(String sql, boolean isParameterized, boolean escapeProcessing) {
    this.sql = sql;
    this.isParameterized = isParameterized;
    this.escapeProcessing = escapeProcessing;
  }

  @Override
  public String toString() {
    return "BaseQueryKey{"
        + "sql='" + sql + '\''
        + ", isParameterized=" + isParameterized
        + ", escapeProcessing=" + escapeProcessing
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseQueryKey that = (BaseQueryKey) o;

    if (isParameterized != that.isParameterized) {
      return false;
    }
    if (escapeProcessing != that.escapeProcessing) {
      return false;
    }
    return sql != null ? sql.equals(that.sql) : that.sql == null;

  }

  @Override
  public int hashCode() {
    int result = sql != null ? sql.hashCode() : 0;
    result = 31 * result + (isParameterized ? 1 : 0);
    result = 31 * result + (escapeProcessing ? 1 : 0);
    return result;
  }
}
