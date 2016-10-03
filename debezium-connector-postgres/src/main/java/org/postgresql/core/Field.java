/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.jdbc.FieldMetadata;

/*
 */
public class Field {
  // The V3 protocol defines two constants for the format of data
  public static final int TEXT_FORMAT = 0;
  public static final int BINARY_FORMAT = 1;

  private final int length; // Internal Length of this field
  private final int oid; // OID of the type
  private final int mod; // type modifier of this field
  private final String columnLabel; // Column label

  private int format = TEXT_FORMAT; // In the V3 protocol each field has a format
  // 0 = text, 1 = binary
  // In the V2 protocol all fields in a
  // binary cursor are binary and all
  // others are text

  private final int tableOid; // OID of table ( zero if no table )
  private final int positionInTable;

  // Cache fields filled in by AbstractJdbc2ResultSetMetaData.fetchFieldMetaData.
  // Don't use unless that has been called.
  private FieldMetadata metadata;

  private int sqlType;
  private String pgType = NOT_YET_LOADED;

  // New string to avoid clashes with other strings
  private static final String NOT_YET_LOADED = new String("pgType is not yet loaded");

  /**
   * Construct a field based on the information fed to it.
   *
   * @param name the name (column name and label) of the field
   * @param oid the OID of the field
   * @param length the length of the field
   * @param mod modifier
   */
  public Field(String name, int oid, int length, int mod) {
    this(name, oid, length, mod, 0, 0);
  }

  /**
   * Constructor without mod parameter.
   *
   * @param name the name (column name and label) of the field
   * @param oid the OID of the field
   */
  public Field(String name, int oid) {
    this(name, oid, 0, -1);
  }

  /**
   * Construct a field based on the information fed to it.
   * @param columnLabel the column label of the field
   * @param oid the OID of the field
   * @param length the length of the field
   * @param mod modifier
   * @param tableOid the OID of the columns' table
   * @param positionInTable the position of column in the table (first column is 1, second column is 2, etc...)
   */
  public Field(String columnLabel, int oid, int length, int mod, int tableOid,
      int positionInTable) {
    this.columnLabel = columnLabel;
    this.oid = oid;
    this.length = length;
    this.mod = mod;
    this.tableOid = tableOid;
    this.positionInTable = positionInTable;
    this.metadata = tableOid == 0 ? new FieldMetadata(columnLabel) : null;
  }

  /**
   * @return the oid of this Field's data type
   */
  public int getOID() {
    return oid;
  }

  /**
   * @return the mod of this Field's data type
   */
  public int getMod() {
    return mod;
  }

  /**
   * @return the column label of this Field's data type
   */
  public String getColumnLabel() {
    return columnLabel;
  }

  /**
   * @return the length of this Field's data type
   */
  public int getLength() {
    return length;
  }

  /**
   * @return the format of this Field's data (text=0, binary=1)
   */
  public int getFormat() {
    return format;
  }

  /**
   * @param format the format of this Field's data (text=0, binary=1)
   */
  public void setFormat(int format) {
    this.format = format;
  }

  /**
   * @return the columns' table oid, zero if no oid available
   */
  public int getTableOid() {
    return tableOid;
  }

  public int getPositionInTable() {
    return positionInTable;
  }

  public FieldMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(FieldMetadata metadata) {
    this.metadata = metadata;
  }

  public String toString() {
    return "Field(" + (columnLabel != null ? columnLabel : "")
        + "," + Oid.toString(oid)
        + "," + length
        + "," + (format == TEXT_FORMAT ? 'T' : 'B')
        + ")";
  }

  public void setSQLType(int sqlType) {
    this.sqlType = sqlType;
  }

  public int getSQLType() {
    return sqlType;
  }

  public void setPGType(String pgType) {
    this.pgType = pgType;
  }

  public String getPGType() {
    return pgType;
  }

  public boolean isTypeInitialized() {
    return pgType != NOT_YET_LOADED;
  }
}
