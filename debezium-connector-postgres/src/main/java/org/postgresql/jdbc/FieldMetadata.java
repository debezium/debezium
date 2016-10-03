package org.postgresql.jdbc;

import org.postgresql.util.CanEstimateSize;

/**
 * This is an internal class to hold field metadata info like table name, column name, etc.
 * This class is not meant to be used outside of pgjdbc.
 */
public class FieldMetadata implements CanEstimateSize {
  public static class Key {
    final int tableOid;
    final int positionInTable;

    Key(int tableOid, int positionInTable) {
      this.positionInTable = positionInTable;
      this.tableOid = tableOid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key key = (Key) o;

      if (tableOid != key.tableOid) {
        return false;
      }
      return positionInTable == key.positionInTable;
    }

    @Override
    public int hashCode() {
      int result = tableOid;
      result = 31 * result + positionInTable;
      return result;
    }

    @Override
    public String toString() {
      return "Key{"
          + "tableOid=" + tableOid
          + ", positionInTable=" + positionInTable
          + '}';
    }
  }

  final String columnName;
  final String tableName;
  final String schemaName;
  final int nullable;
  final boolean autoIncrement;

  public FieldMetadata(String columnName) {
    this(columnName, "", "", PgResultSetMetaData.columnNullableUnknown, false);
  }

  FieldMetadata(String columnName, String tableName, String schemaName, int nullable,
      boolean autoIncrement) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
  }

  public long getSize() {
    return columnName.length() * 2
        + tableName.length() * 2
        + schemaName.length() * 2
        + 4L
        + 1L;
  }

  @Override
  public String toString() {
    return "FieldMetadata{"
        + "columnName='" + columnName + '\''
        + ", tableName='" + tableName + '\''
        + ", schemaName='" + schemaName + '\''
        + ", nullable=" + nullable
        + ", autoIncrement=" + autoIncrement
        + '}';
  }
}
