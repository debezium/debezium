/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.PGResultSetMetaData;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Encoding;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.core.Query;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.TypeInfo;
import org.postgresql.core.Utils;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.GT;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGbytea;
import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
//#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
//#endif
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;


public class PgResultSet implements ResultSet, org.postgresql.PGRefCursorResultSet {

  // needed for updateable result set support
  private boolean updateable = false;
  private boolean doingUpdates = false;
  private HashMap<String, Object> updateValues = null;
  private boolean usingOID = false; // are we using the OID for the primary key?
  private List<PrimaryKey> primaryKeys; // list of primary keys
  private boolean singleTable = false;
  private String onlyTable = "";
  private String tableName = null;
  private PreparedStatement updateStatement = null;
  private PreparedStatement insertStatement = null;
  private PreparedStatement deleteStatement = null;
  private PreparedStatement selectStatement = null;
  private final int resultsettype;
  private final int resultsetconcurrency;
  private int fetchdirection = ResultSet.FETCH_UNKNOWN;
  private TimeZone defaultTimeZone;
  protected final BaseConnection connection; // the connection we belong to
  protected final BaseStatement statement; // the statement we belong to
  protected final Field fields[]; // Field metadata for this resultset.
  protected final Query originalQuery; // Query we originated from

  protected final int maxRows; // Maximum rows in this resultset (might be 0).
  protected final int maxFieldSize; // Maximum field size in this resultset (might be 0).

  protected List<byte[][]> rows; // Current page of results.
  protected int current_row = -1; // Index into 'rows' of our currrent row (0-based)
  protected int row_offset; // Offset of row 0 in the actual resultset
  protected byte[][] this_row; // copy of the current result row
  protected SQLWarning warnings = null; // The warning chain
  /**
   * True if the last obtained column value was SQL NULL as specified by {@link #wasNull}. The value
   * is always updated by the {@link #checkResultSet} method.
   */
  protected boolean wasNullFlag = false;
  protected boolean onInsertRow = false;
  // are we on the insert row (for JDBC2 updatable resultsets)?

  private byte[][] rowBuffer = null; // updateable rowbuffer

  protected int fetchSize; // Current fetch size (might be 0).
  protected ResultCursor cursor; // Cursor for fetching additional data.

  private Map<String, Integer> columnNameIndexMap; // Speed up findColumn by caching lookups

  private ResultSetMetaData rsMetaData;

  protected ResultSetMetaData createMetaData() throws SQLException {
    return new PgResultSetMetaData(connection, fields);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();
    if (rsMetaData == null) {
      rsMetaData = createMetaData();
    }
    return rsMetaData;
  }

  PgResultSet(Query originalQuery, BaseStatement statement, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor, int maxRows, int maxFieldSize, int rsType, int rsConcurrency,
      int rsHoldability) throws SQLException {
    // Fail-fast on invalid null inputs
    if (tuples == null) {
      throw new NullPointerException("tuples must be non-null");
    }
    if (fields == null) {
      throw new NullPointerException("fields must be non-null");
    }

    this.originalQuery = originalQuery;
    this.connection = (BaseConnection) statement.getConnection();
    this.statement = statement;
    this.fields = fields;
    this.rows = tuples;
    this.cursor = cursor;
    this.maxRows = maxRows;
    this.maxFieldSize = maxFieldSize;
    this.resultsettype = rsType;
    this.resultsetconcurrency = rsConcurrency;
  }

  public java.net.URL getURL(int columnIndex) throws SQLException {
    checkClosed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getURL(int)");
  }


  public java.net.URL getURL(String columnName) throws SQLException {
    return getURL(findColumn(columnName));
  }

  protected Object internalGetObject(int columnIndex, Field field) throws SQLException {
    switch (getSQLType(columnIndex)) {
      case Types.BOOLEAN:
        return getBoolean(columnIndex);
      case Types.SQLXML:
        return getSQLXML(columnIndex);
      case Types.BIT:
        // Also Types.BOOLEAN in JDBC3
        return getBoolean(columnIndex) ? Boolean.TRUE : Boolean.FALSE;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return getInt(columnIndex);
      case Types.BIGINT:
        return getLong(columnIndex);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return getBigDecimal(columnIndex,
            (field.getMod() == -1) ? -1 : ((field.getMod() - 4) & 0xffff));
      case Types.REAL:
        return getFloat(columnIndex);
      case Types.FLOAT:
      case Types.DOUBLE:
        return getDouble(columnIndex);
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return getString(columnIndex);
      case Types.DATE:
        return getDate(columnIndex);
      case Types.TIME:
        return getTime(columnIndex);
      case Types.TIMESTAMP:
        return getTimestamp(columnIndex, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return getBytes(columnIndex);
      case Types.ARRAY:
        return getArray(columnIndex);
      case Types.CLOB:
        return getClob(columnIndex);
      case Types.BLOB:
        return getBlob(columnIndex);

      default:
        String type = getPGType(columnIndex);

        // if the backend doesn't know the type then coerce to String
        if (type.equals("unknown")) {
          return getString(columnIndex);
        }

        if (type.equals("uuid")) {
          if (isBinary(columnIndex)) {
            return getUUID(this_row[columnIndex - 1]);
          }
          return getUUID(getString(columnIndex));
        }

        // Specialized support for ref cursors is neater.
        if (type.equals("refcursor")) {
          // Fetch all results.
          String cursorName = getString(columnIndex);

          StringBuilder sb = new StringBuilder("FETCH ALL IN ");
          Utils.escapeIdentifier(sb, cursorName);

          // nb: no BEGIN triggered here. This is fine. If someone
          // committed, and the cursor was not holdable (closing the
          // cursor), we avoid starting a new xact and promptly causing
          // it to fail. If the cursor *was* holdable, we don't want a
          // new xact anyway since holdable cursor state isn't affected
          // by xact boundaries. If our caller didn't commit at all, or
          // autocommit was on, then we wouldn't issue a BEGIN anyway.
          //
          // We take the scrollability from the statement, but until
          // we have updatable cursors it must be readonly.
          ResultSet rs =
              connection.execSQLQuery(sb.toString(), resultsettype, ResultSet.CONCUR_READ_ONLY);
          //
          // In long running transactions these backend cursors take up memory space
          // we could close in rs.close(), but if the transaction is closed before the result set,
          // then
          // the cursor no longer exists

          sb.setLength(0);
          sb.append("CLOSE ");
          Utils.escapeIdentifier(sb, cursorName);
          connection.execSQLUpdate(sb.toString());
          ((PgResultSet) rs).setRefCursor(cursorName);
          return rs;
        }
        if ("hstore".equals(type)) {
          if (isBinary(columnIndex)) {
            return HStoreConverter.fromBytes(this_row[columnIndex - 1], connection.getEncoding());
          }
          return HStoreConverter.fromString(getString(columnIndex));
        }

        // Caller determines what to do (JDBC3 overrides in this case)
        return null;
    }
  }

  private void checkScrollable() throws SQLException {
    checkClosed();
    if (resultsettype == ResultSet.TYPE_FORWARD_ONLY) {
      throw new PSQLException(
          GT.tr("Operation requires a scrollable ResultSet, but this ResultSet is FORWARD_ONLY."),
          PSQLState.INVALID_CURSOR_STATE);
    }
  }

  public boolean absolute(int index) throws SQLException {
    checkScrollable();

    // index is 1-based, but internally we use 0-based indices
    int internalIndex;

    if (index == 0) {
      beforeFirst();
      return false;
    }

    final int rows_size = rows.size();

    // if index<0, count from the end of the result set, but check
    // to be sure that it is not beyond the first index
    if (index < 0) {
      if (index >= -rows_size) {
        internalIndex = rows_size + index;
      } else {
        beforeFirst();
        return false;
      }
    } else {
      // must be the case that index>0,
      // find the correct place, assuming that
      // the index is not too large
      if (index <= rows_size) {
        internalIndex = index - 1;
      } else {
        afterLast();
        return false;
      }
    }

    current_row = internalIndex;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }


  public void afterLast() throws SQLException {
    checkScrollable();

    final int rows_size = rows.size();
    if (rows_size > 0) {
      current_row = rows_size;
    }

    onInsertRow = false;
    this_row = null;
    rowBuffer = null;
  }


  public void beforeFirst() throws SQLException {
    checkScrollable();

    if (!rows.isEmpty()) {
      current_row = -1;
    }

    onInsertRow = false;
    this_row = null;
    rowBuffer = null;
  }


  public boolean first() throws SQLException {
    checkScrollable();

    if (rows.size() <= 0) {
      return false;
    }

    current_row = 0;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }


  public java.sql.Array getArray(String colName) throws SQLException {
    return getArray(findColumn(colName));
  }

  protected Array makeArray(int oid, byte[] value) throws SQLException {
    return new PgArray(connection, oid, value);
  }

  protected Array makeArray(int oid, String value) throws SQLException {
    return new PgArray(connection, oid, value);
  }

  public java.sql.Array getArray(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    int oid = fields[i - 1].getOID();
    if (isBinary(i)) {
      return makeArray(oid, this_row[i - 1]);
    }
    return makeArray(oid, getFixedString(i));
  }


  public java.math.BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getBigDecimal(columnIndex, -1);
  }


  public java.math.BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }


  public Blob getBlob(String columnName) throws SQLException {
    return getBlob(findColumn(columnName));
  }

  protected Blob makeBlob(long oid) throws SQLException {
    return new PgBlob(connection, oid);
  }

  public Blob getBlob(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    return makeBlob(getLong(i));
  }


  public java.io.Reader getCharacterStream(String columnName) throws SQLException {
    return getCharacterStream(findColumn(columnName));
  }


  public java.io.Reader getCharacterStream(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (connection.haveMinimumCompatibleVersion(ServerVersion.v7_2)) {
      // Version 7.2 supports AsciiStream for all the PG text types
      // As the spec/javadoc for this method indicate this is to be used for
      // large text values (i.e. LONGVARCHAR) PG doesn't have a separate
      // long string datatype, but with toast the text datatype is capable of
      // handling very large values. Thus the implementation ends up calling
      // getString() since there is no current way to stream the value from the server
      return new CharArrayReader(getString(i).toCharArray());
    } else {
      // In 7.1 Handle as BLOBS so return the LargeObject input stream
      Encoding encoding = connection.getEncoding();
      InputStream input = getBinaryStream(i);

      try {
        return encoding.getDecodingReader(input);
      } catch (IOException ioe) {
        throw new PSQLException(
            GT.tr("Unexpected error while decoding character data from a large object."),
            PSQLState.UNEXPECTED_ERROR, ioe);
      }
    }
  }


  public Clob getClob(String columnName) throws SQLException {
    return getClob(findColumn(columnName));
  }


  protected Clob makeClob(long oid) throws SQLException {
    return new PgClob(connection, oid);
  }

  public Clob getClob(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    return makeClob(getLong(i));
  }

  public int getConcurrency() throws SQLException {
    checkClosed();
    return resultsetconcurrency;
  }


  @Override
  public java.sql.Date getDate(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    if (isBinary(i)) {
      int col = i - 1;
      int oid = fields[col].getOID();
      TimeZone tz = cal.getTimeZone();
      if (oid == Oid.DATE) {
        return connection.getTimestampUtils().toDateBin(tz, this_row[col]);
      } else if (oid == Oid.TIMESTAMP || oid == Oid.TIMESTAMPTZ) {
        // If backend provides just TIMESTAMP, we use "cal" timezone
        // If backend provides TIMESTAMPTZ, we ignore "cal" as we know true instant value
        Timestamp timestamp = getTimestamp(i, cal);
        // Here we just truncate date to 00:00 in a given time zone
        return connection.getTimestampUtils().convertToDate(timestamp.getTime(), tz);
      } else {
        throw new PSQLException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), "date"),
            PSQLState.DATA_TYPE_MISMATCH);
      }
    }

    return connection.getTimestampUtils().toDate(cal, getString(i));
  }


  @Override
  public Time getTime(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    if (isBinary(i)) {
      int col = i - 1;
      int oid = fields[col].getOID();
      TimeZone tz = cal.getTimeZone();
      if (oid == Oid.TIME || oid == Oid.TIMETZ) {
        return connection.getTimestampUtils().toTimeBin(tz, this_row[col]);
      } else if (oid == Oid.TIMESTAMP || oid == Oid.TIMESTAMPTZ) {
        // If backend provides just TIMESTAMP, we use "cal" timezone
        // If backend provides TIMESTAMPTZ, we ignore "cal" as we know true instant value
        Timestamp timestamp = getTimestamp(i, cal);
        // Here we just truncate date part
        return connection.getTimestampUtils().convertToTime(timestamp.getTime(), tz);
      } else {
        throw new PSQLException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), "time"),
            PSQLState.DATA_TYPE_MISMATCH);
      }
    }

    String string = getString(i);
    return connection.getTimestampUtils().toTime(cal, string);
  }


  @Override
  public Timestamp getTimestamp(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    int col = i - 1;
    int oid = fields[col].getOID();
    if (isBinary(i)) {
      if (oid == Oid.TIMESTAMPTZ || oid == Oid.TIMESTAMP) {
        boolean hasTimeZone = oid == Oid.TIMESTAMPTZ;
        TimeZone tz = cal.getTimeZone();
        return connection.getTimestampUtils().toTimestampBin(tz, this_row[col], hasTimeZone);
      } else {
        // JDBC spec says getTimestamp of Time and Date must be supported
        long millis;
        if (oid == Oid.TIME || oid == Oid.TIMETZ) {
          millis = getTime(i, cal).getTime();
        } else if (oid == Oid.DATE) {
          millis = getDate(i, cal).getTime();
        } else {
          throw new PSQLException(
              GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                  Oid.toString(oid), "timestamp"),
              PSQLState.DATA_TYPE_MISMATCH);
        }
        return new Timestamp(millis);
      }
    }

    // If this is actually a timestamptz, the server-provided timezone will override
    // the one we pass in, which is the desired behaviour. Otherwise, we'll
    // interpret the timezone-less value in the provided timezone.
    String string = getString(i);
    if (oid == Oid.TIME || oid == Oid.TIMETZ) {
      // If server sends us a TIME, we ensure java counterpart has date of 1970-01-01
      return new Timestamp(connection.getTimestampUtils().toTime(cal, string).getTime());
    }
    return connection.getTimestampUtils().toTimestamp(cal, string);
  }

  //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
  private LocalDateTime getLocalDateTime(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    int col = i - 1;
    int oid = fields[col].getOID();
    if (oid != Oid.TIMESTAMP) {
      throw new PSQLException(
              GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                  Oid.toString(oid), "timestamp"),
              PSQLState.DATA_TYPE_MISMATCH);
    }
    if (isBinary(i)) {
      TimeZone timeZone = getDefaultCalendar().getTimeZone();
      return connection.getTimestampUtils().toLocalDateTimeBin(timeZone, this_row[col]);
    }

    String string = getString(i);
    return connection.getTimestampUtils().toLocalDateTime(string);
  }
  //#endif


  public java.sql.Date getDate(String c, java.util.Calendar cal) throws SQLException {
    return getDate(findColumn(c), cal);
  }


  public Time getTime(String c, java.util.Calendar cal) throws SQLException {
    return getTime(findColumn(c), cal);
  }


  public Timestamp getTimestamp(String c, java.util.Calendar cal) throws SQLException {
    return getTimestamp(findColumn(c), cal);
  }


  public int getFetchDirection() throws SQLException {
    checkClosed();
    return fetchdirection;
  }


  public Object getObjectImpl(String columnName, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(findColumn(columnName), map);
  }


  /*
   * This checks against map for the type of column i, and if found returns an object based on that
   * mapping. The class must implement the SQLData interface.
   */
  public Object getObjectImpl(int i, Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    if (map == null || map.isEmpty()) {
      return getObject(i);
    }
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getObjectImpl(int,Map)");
  }


  public Ref getRef(String columnName) throws SQLException {
    return getRef(findColumn(columnName));
  }


  public Ref getRef(int i) throws SQLException {
    checkClosed();
    // The backend doesn't yet have SQL3 REF types
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getRef(int)");
  }


  public int getRow() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      return 0;
    }

    final int rows_size = rows.size();

    if (current_row < 0 || current_row >= rows_size) {
      return 0;
    }

    return row_offset + current_row + 1;
  }


  // This one needs some thought, as not all ResultSets come from a statement
  public Statement getStatement() throws SQLException {
    checkClosed();
    return statement;
  }


  public int getType() throws SQLException {
    checkClosed();
    return resultsettype;
  }


  public boolean isAfterLast() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    final int rows_size = rows.size();
    if (row_offset + rows_size == 0) {
      return false;
    }
    return (current_row >= rows_size);
  }


  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    return ((row_offset + current_row) < 0 && !rows.isEmpty());
  }


  public boolean isFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    final int rows_size = rows.size();
    if (row_offset + rows_size == 0) {
      return false;
    }

    return ((row_offset + current_row) == 0);
  }


  public boolean isLast() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    final int rows_size = rows.size();

    if (rows_size == 0) {
      return false; // No rows.
    }

    if (current_row != (rows_size - 1)) {
      return false; // Not on the last row of this block.
    }

    // We are on the last row of the current block.

    if (cursor == null) {
      // This is the last block and therefore the last row.
      return true;
    }

    if (maxRows > 0 && row_offset + current_row == maxRows) {
      // We are implicitly limited by maxRows.
      return true;
    }

    // Now the more painful case begins.
    // We are on the last row of the current block, but we don't know if the
    // current block is the last block; we must try to fetch some more data to
    // find out.

    // We do a fetch of the next block, then prepend the current row to that
    // block (so current_row == 0). This works as the current row
    // must be the last row of the current block if we got this far.

    row_offset += rows_size - 1; // Discarding all but one row.

    // Work out how many rows maxRows will let us fetch.
    int fetchRows = fetchSize;
    if (maxRows != 0) {
      if (fetchRows == 0 || row_offset + fetchRows > maxRows) {
        // Fetch would exceed maxRows, limit it.
        fetchRows = maxRows - row_offset;
      }
    }

    // Do the actual fetch.
    connection.getQueryExecutor().fetch(cursor, new CursorResultHandler(), fetchRows);

    // Now prepend our one saved row and move to it.
    rows.add(0, this_row);
    current_row = 0;

    // Finally, now we can tell if we're the last row or not.
    return (rows.size() == 1);
  }

  public boolean last() throws SQLException {
    checkScrollable();

    final int rows_size = rows.size();
    if (rows_size <= 0) {
      return false;
    }

    current_row = rows_size - 1;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }


  public boolean previous() throws SQLException {
    checkScrollable();

    if (onInsertRow) {
      throw new PSQLException(GT.tr("Can''t use relative move methods while on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (current_row - 1 < 0) {
      current_row = -1;
      this_row = null;
      rowBuffer = null;
      return false;
    } else {
      current_row--;
    }
    initRowBuffer();
    return true;
  }


  public boolean relative(int rows) throws SQLException {
    checkScrollable();

    if (onInsertRow) {
      throw new PSQLException(GT.tr("Can''t use relative move methods while on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    // have to add 1 since absolute expects a 1-based index
    return absolute(current_row + 1 + rows);
  }


  public void setFetchDirection(int direction) throws SQLException {
    checkClosed();
    switch (direction) {
      case ResultSet.FETCH_FORWARD:
        break;
      case ResultSet.FETCH_REVERSE:
      case ResultSet.FETCH_UNKNOWN:
        checkScrollable();
        break;
      default:
        throw new PSQLException(GT.tr("Invalid fetch direction constant: {0}.", direction),
            PSQLState.INVALID_PARAMETER_VALUE);
    }

    this.fetchdirection = direction;
  }


  public synchronized void cancelRowUpdates() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      throw new PSQLException(GT.tr("Cannot call cancelRowUpdates() when on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (doingUpdates) {
      doingUpdates = false;

      clearRowBuffer(true);
    }
  }


  public synchronized void deleteRow() throws SQLException {
    checkUpdateable();

    if (onInsertRow) {
      throw new PSQLException(GT.tr("Cannot call deleteRow() when on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (isBeforeFirst()) {
      throw new PSQLException(
          GT.tr(
              "Currently positioned before the start of the ResultSet.  You cannot call deleteRow() here."),
          PSQLState.INVALID_CURSOR_STATE);
    }
    if (isAfterLast()) {
      throw new PSQLException(
          GT.tr(
              "Currently positioned after the end of the ResultSet.  You cannot call deleteRow() here."),
          PSQLState.INVALID_CURSOR_STATE);
    }
    if (rows.isEmpty()) {
      throw new PSQLException(GT.tr("There are no rows in this ResultSet."),
          PSQLState.INVALID_CURSOR_STATE);
    }


    int numKeys = primaryKeys.size();
    if (deleteStatement == null) {


      StringBuilder deleteSQL =
          new StringBuilder("DELETE FROM ").append(onlyTable).append(tableName).append(" where ");

      for (int i = 0; i < numKeys; i++) {
        Utils.escapeIdentifier(deleteSQL, primaryKeys.get(i).name);
        deleteSQL.append(" = ?");
        if (i < numKeys - 1) {
          deleteSQL.append(" and ");
        }
      }

      deleteStatement = connection.prepareStatement(deleteSQL.toString());
    }
    deleteStatement.clearParameters();

    for (int i = 0; i < numKeys; i++) {
      deleteStatement.setObject(i + 1, primaryKeys.get(i).getValue());
    }


    deleteStatement.executeUpdate();

    rows.remove(current_row);
    current_row--;
    moveToCurrentRow();
  }


  public synchronized void insertRow() throws SQLException {
    checkUpdateable();

    if (!onInsertRow) {
      throw new PSQLException(GT.tr("Not on the insert row."), PSQLState.INVALID_CURSOR_STATE);
    } else if (updateValues.isEmpty()) {
      throw new PSQLException(GT.tr("You must specify at least one column value to insert a row."),
          PSQLState.INVALID_PARAMETER_VALUE);
    } else {

      // loop through the keys in the insertTable and create the sql statement
      // we have to create the sql every time since the user could insert different
      // columns each time

      StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
      StringBuilder paramSQL = new StringBuilder(") values (");

      Iterator<String> columnNames = updateValues.keySet().iterator();
      int numColumns = updateValues.size();

      for (int i = 0; columnNames.hasNext(); i++) {
        String columnName = columnNames.next();

        Utils.escapeIdentifier(insertSQL, columnName);
        if (i < numColumns - 1) {
          insertSQL.append(", ");
          paramSQL.append("?,");
        } else {
          paramSQL.append("?)");
        }

      }

      insertSQL.append(paramSQL.toString());
      insertStatement = connection.prepareStatement(insertSQL.toString());

      Iterator<Object> values = updateValues.values().iterator();

      for (int i = 1; values.hasNext(); i++) {
        insertStatement.setObject(i, values.next());
      }

      insertStatement.executeUpdate();

      if (usingOID) {
        // we have to get the last inserted OID and put it in the resultset

        long insertedOID = ((PgStatement) insertStatement).getLastOID();

        updateValues.put("oid", insertedOID);

      }

      // update the underlying row to the new inserted data
      updateRowBuffer();

      rows.add(rowBuffer);

      // we should now reflect the current data in this_row
      // that way getXXX will get the newly inserted data
      this_row = rowBuffer;

      // need to clear this in case of another insert
      clearRowBuffer(false);


    }
  }


  public synchronized void moveToCurrentRow() throws SQLException {
    checkUpdateable();

    if (current_row < 0 || current_row >= rows.size()) {
      this_row = null;
      rowBuffer = null;
    } else {
      initRowBuffer();
    }

    onInsertRow = false;
    doingUpdates = false;
  }


  public synchronized void moveToInsertRow() throws SQLException {
    checkUpdateable();

    if (insertStatement != null) {
      insertStatement = null;
    }


    // make sure the underlying data is null
    clearRowBuffer(false);

    onInsertRow = true;
    doingUpdates = false;

  }


  private synchronized void clearRowBuffer(boolean copyCurrentRow) throws SQLException {
    // rowBuffer is the temporary storage for the row
    rowBuffer = new byte[fields.length][];

    // inserts want an empty array while updates want a copy of the current row
    if (copyCurrentRow) {
      System.arraycopy(this_row, 0, rowBuffer, 0, this_row.length);
    }

    // clear the updateValues hash map for the next set of updates
    updateValues.clear();

  }


  public boolean rowDeleted() throws SQLException {
    checkClosed();
    return false;
  }


  public boolean rowInserted() throws SQLException {
    checkClosed();
    return false;
  }


  public boolean rowUpdated() throws SQLException {
    checkClosed();
    return false;
  }


  public synchronized void updateAsciiStream(int columnIndex, java.io.InputStream x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    try {
      InputStreamReader reader = new InputStreamReader(x, "ASCII");
      char data[] = new char[length];
      int numRead = 0;
      while (true) {
        int n = reader.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
      updateString(columnIndex, new String(data, 0, numRead));
    } catch (UnsupportedEncodingException uee) {
      throw new PSQLException(GT.tr("The JVM claims not to support the encoding: {0}", "ASCII"),
          PSQLState.UNEXPECTED_ERROR, uee);
    } catch (IOException ie) {
      throw new PSQLException(GT.tr("Provided InputStream failed."), null, ie);
    }
  }


  public synchronized void updateBigDecimal(int columnIndex, java.math.BigDecimal x)
      throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateBinaryStream(int columnIndex, java.io.InputStream x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    byte data[] = new byte[length];
    int numRead = 0;
    try {
      while (true) {
        int n = x.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
    } catch (IOException ie) {
      throw new PSQLException(GT.tr("Provided InputStream failed."), null, ie);
    }

    if (numRead == length) {
      updateBytes(columnIndex, data);
    } else {
      // the stream contained less data than they said
      // perhaps this is an error?
      byte data2[] = new byte[numRead];
      System.arraycopy(data, 0, data2, 0, numRead);
      updateBytes(columnIndex, data2);
    }
  }


  public synchronized void updateBoolean(int columnIndex, boolean x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateByte(int columnIndex, byte x) throws SQLException {
    updateValue(columnIndex, String.valueOf(x));
  }


  public synchronized void updateBytes(int columnIndex, byte[] x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateCharacterStream(int columnIndex, java.io.Reader x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    try {
      char data[] = new char[length];
      int numRead = 0;
      while (true) {
        int n = x.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
      updateString(columnIndex, new String(data, 0, numRead));
    } catch (IOException ie) {
      throw new PSQLException(GT.tr("Provided Reader failed."), null, ie);
    }
  }


  public synchronized void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateDouble(int columnIndex, double x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateFloat(int columnIndex, float x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateInt(int columnIndex, int x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateLong(int columnIndex, long x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateNull(int columnIndex) throws SQLException {
    checkColumnIndex(columnIndex);
    String columnTypeName = getPGType(columnIndex);
    updateValue(columnIndex, new NullObject(columnTypeName));
  }


  public synchronized void updateObject(int columnIndex, Object x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    this.updateObject(columnIndex, x);

  }


  public void refreshRow() throws SQLException {
    checkUpdateable();
    if (onInsertRow) {
      throw new PSQLException(GT.tr("Can''t refresh the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (isBeforeFirst() || isAfterLast() || rows.isEmpty()) {
      return;
    }

    StringBuilder selectSQL = new StringBuilder("select ");

    ResultSetMetaData rsmd = getMetaData();
    PGResultSetMetaData pgmd = (PGResultSetMetaData) rsmd;
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      if (i > 1) {
        selectSQL.append(", ");
      }
      selectSQL.append(pgmd.getBaseColumnName(i));
    }
    selectSQL.append(" from ").append(onlyTable).append(tableName).append(" where ");

    int numKeys = primaryKeys.size();

    for (int i = 0; i < numKeys; i++) {

      PrimaryKey primaryKey = primaryKeys.get(i);
      selectSQL.append(primaryKey.name).append("= ?");

      if (i < numKeys - 1) {
        selectSQL.append(" and ");
      }
    }
    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("selecting " + selectSQL.toString());
    }
    // because updateable result sets do not yet support binary transfers we must request refresh
    // with updateable result set to get field data in correct format
    selectStatement = connection.prepareStatement(selectSQL.toString(),
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);


    for (int j = 0, i = 1; j < numKeys; j++, i++) {
      selectStatement.setObject(i, primaryKeys.get(j).getValue());
    }

    PgResultSet rs = (PgResultSet) selectStatement.executeQuery();

    if (rs.next()) {
      rowBuffer = rs.this_row;
    }

    rows.set(current_row, rowBuffer);
    this_row = rowBuffer;

    connection.getLogger().debug("done updates");

    rs.close();
    selectStatement.close();
    selectStatement = null;

  }


  public synchronized void updateRow() throws SQLException {
    checkUpdateable();

    if (onInsertRow) {
      throw new PSQLException(GT.tr("Cannot call updateRow() when on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (isBeforeFirst() || isAfterLast() || rows.isEmpty()) {
      throw new PSQLException(
          GT.tr(
              "Cannot update the ResultSet because it is either before the start or after the end of the results."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (!doingUpdates) {
      return; // No work pending.
    }

    StringBuilder updateSQL = new StringBuilder("UPDATE " + onlyTable + tableName + " SET  ");

    int numColumns = updateValues.size();
    Iterator<String> columns = updateValues.keySet().iterator();

    for (int i = 0; columns.hasNext(); i++) {
      String column = columns.next();
      Utils.escapeIdentifier(updateSQL, column);
      updateSQL.append(" = ?");

      if (i < numColumns - 1) {
        updateSQL.append(", ");
      }
    }

    updateSQL.append(" WHERE ");

    int numKeys = primaryKeys.size();

    for (int i = 0; i < numKeys; i++) {
      PrimaryKey primaryKey = primaryKeys.get(i);
      Utils.escapeIdentifier(updateSQL, primaryKey.name);
      updateSQL.append(" = ?");

      if (i < numKeys - 1) {
        updateSQL.append(" and ");
      }
    }

    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("updating " + updateSQL.toString());
    }
    updateStatement = connection.prepareStatement(updateSQL.toString());

    int i = 0;
    Iterator<Object> iterator = updateValues.values().iterator();
    for (; iterator.hasNext(); i++) {
      Object o = iterator.next();
      updateStatement.setObject(i + 1, o);
    }

    for (int j = 0; j < numKeys; j++, i++) {
      updateStatement.setObject(i + 1, primaryKeys.get(j).getValue());
    }

    updateStatement.executeUpdate();
    updateStatement.close();
    updateStatement = null;

    updateRowBuffer();

    connection.getLogger().debug("copying data");
    System.arraycopy(rowBuffer, 0, this_row, 0, rowBuffer.length);
    rows.set(current_row, rowBuffer);

    connection.getLogger().debug("done updates");
    updateValues.clear();
    doingUpdates = false;
  }


  public synchronized void updateShort(int columnIndex, short x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateString(int columnIndex, String x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateTime(int columnIndex, Time x) throws SQLException {
    updateValue(columnIndex, x);
  }


  public synchronized void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    updateValue(columnIndex, x);

  }


  public synchronized void updateNull(String columnName) throws SQLException {
    updateNull(findColumn(columnName));
  }


  public synchronized void updateBoolean(String columnName, boolean x) throws SQLException {
    updateBoolean(findColumn(columnName), x);
  }


  public synchronized void updateByte(String columnName, byte x) throws SQLException {
    updateByte(findColumn(columnName), x);
  }


  public synchronized void updateShort(String columnName, short x) throws SQLException {
    updateShort(findColumn(columnName), x);
  }


  public synchronized void updateInt(String columnName, int x) throws SQLException {
    updateInt(findColumn(columnName), x);
  }


  public synchronized void updateLong(String columnName, long x) throws SQLException {
    updateLong(findColumn(columnName), x);
  }


  public synchronized void updateFloat(String columnName, float x) throws SQLException {
    updateFloat(findColumn(columnName), x);
  }


  public synchronized void updateDouble(String columnName, double x) throws SQLException {
    updateDouble(findColumn(columnName), x);
  }


  public synchronized void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
    updateBigDecimal(findColumn(columnName), x);
  }


  public synchronized void updateString(String columnName, String x) throws SQLException {
    updateString(findColumn(columnName), x);
  }


  public synchronized void updateBytes(String columnName, byte x[]) throws SQLException {
    updateBytes(findColumn(columnName), x);
  }


  public synchronized void updateDate(String columnName, java.sql.Date x) throws SQLException {
    updateDate(findColumn(columnName), x);
  }


  public synchronized void updateTime(String columnName, java.sql.Time x) throws SQLException {
    updateTime(findColumn(columnName), x);
  }


  public synchronized void updateTimestamp(String columnName, java.sql.Timestamp x)
      throws SQLException {
    updateTimestamp(findColumn(columnName), x);
  }


  public synchronized void updateAsciiStream(String columnName, java.io.InputStream x, int length)
      throws SQLException {
    updateAsciiStream(findColumn(columnName), x, length);
  }


  public synchronized void updateBinaryStream(String columnName, java.io.InputStream x, int length)
      throws SQLException {
    updateBinaryStream(findColumn(columnName), x, length);
  }


  public synchronized void updateCharacterStream(String columnName, java.io.Reader reader,
      int length) throws SQLException {
    updateCharacterStream(findColumn(columnName), reader, length);
  }


  public synchronized void updateObject(String columnName, Object x, int scale)
      throws SQLException {
    updateObject(findColumn(columnName), x);
  }


  public synchronized void updateObject(String columnName, Object x) throws SQLException {
    updateObject(findColumn(columnName), x);
  }


  /**
   * Is this ResultSet updateable?
   */

  boolean isUpdateable() throws SQLException {
    checkClosed();

    if (resultsetconcurrency == ResultSet.CONCUR_READ_ONLY) {
      throw new PSQLException(
          GT.tr("ResultSets with concurrency CONCUR_READ_ONLY cannot be updated."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (updateable) {
      return true;
    }

    connection.getLogger().debug("checking if rs is updateable");

    parseQuery();

    if (!singleTable) {
      connection.getLogger().debug("not a single table");
      return false;
    }

    connection.getLogger().debug("getting primary keys");

    //
    // Contains the primary key?
    //

    primaryKeys = new ArrayList<PrimaryKey>();

    // this is not stricty jdbc spec, but it will make things much faster if used
    // the user has to select oid, * from table and then we will just use oid


    usingOID = false;
    int oidIndex = findColumnIndex("oid"); // 0 if not present

    int i = 0;
    int numPKcolumns = 0;

    // if we find the oid then just use it

    // oidIndex will be >0 if the oid was in the select list
    if (oidIndex > 0) {
      i++;
      numPKcolumns++;
      primaryKeys.add(new PrimaryKey(oidIndex, "oid"));
      usingOID = true;
    } else {
      // otherwise go and get the primary keys and create a list of keys
      String[] s = quotelessTableName(tableName);
      String quotelessTableName = s[0];
      String quotelessSchemaName = s[1];
      java.sql.ResultSet rs = connection.getMetaData().getPrimaryKeys("",
          quotelessSchemaName, quotelessTableName);
      while (rs.next()) {
        numPKcolumns++;
        String columnName = rs.getString(4); // get the columnName
        int index = findColumnIndex(columnName);

        if (index > 0) {
          i++;
          primaryKeys.add(new PrimaryKey(index, columnName)); // get the primary key information
        }
      }

      rs.close();
    }

    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("no of keys=" + i);
    }

    if (i < 1) {
      throw new PSQLException(GT.tr("No primary key found for table {0}.", tableName),
          PSQLState.DATA_ERROR);
    }

    updateable = (i == numPKcolumns);

    if (connection.getLogger().logDebug()) {
      connection.getLogger().debug("checking primary key " + updateable);
    }

    return updateable;
  }

  /**
   * Cracks out the table name and schema (if it exists) from a fully qualified table name.
   *
   * @param fullname string that we are trying to crack. Test cases:
   *
   *        <pre>
   *
   *                 Table: table
   *                                 ()
   *
   *                 "Table": Table
   *                                 ()
   *
   *                 Schema.Table:
   *                                 table (schema)
   *
   *                                 "Schema"."Table": Table
   *                                                 (Schema)
   *
   *                                 "Schema"."Dot.Table": Dot.Table
   *                                                 (Schema)
   *
   *                                 Schema."Dot.Table": Dot.Table
   *                                                 (schema)
   *
   *        </pre>
   *
   * @return String array with element zero always being the tablename and element 1 the schema name
   *         which may be a zero length string.
   */
  public static String[] quotelessTableName(String fullname) {

    String[] parts = new String[]{null, ""};
    StringBuilder acc = new StringBuilder();
    boolean betweenQuotes = false;
    for (int i = 0; i < fullname.length(); i++) {
      char c = fullname.charAt(i);
      switch (c) {
        case '"':
          if ((i < fullname.length() - 1) && (fullname.charAt(i + 1) == '"')) {
            // two consecutive quotes - keep one
            i++;
            acc.append(c); // keep the quote
          } else { // Discard it
            betweenQuotes = !betweenQuotes;
          }
          break;
        case '.':
          if (betweenQuotes) { // Keep it
            acc.append(c);
          } else { // Have schema name
            parts[1] = acc.toString();
            acc = new StringBuilder();
          }
          break;
        default:
          acc.append((betweenQuotes) ? c : Character.toLowerCase(c));
          break;
      }
    }
    // Always put table in slot 0
    parts[0] = acc.toString();
    return parts;
  }

  private void parseQuery() {
    String l_sql = originalQuery.toString(null);
    StringTokenizer st = new StringTokenizer(l_sql, " \r\t\n");
    boolean tableFound = false;
    boolean tablesChecked = false;
    String name = "";

    singleTable = true;

    while (!tableFound && !tablesChecked && st.hasMoreTokens()) {
      name = st.nextToken();
      if ("from".equalsIgnoreCase(name)) {
        tableName = st.nextToken();
        if ("only".equalsIgnoreCase(tableName)) {
          tableName = st.nextToken();
          onlyTable = "ONLY ";
        }
        tableFound = true;
      }
    }
  }


  private void updateRowBuffer() throws SQLException {

    for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
      int columnIndex = findColumn(entry.getKey()) - 1;

      Object valueObject = entry.getValue();
      if (valueObject instanceof PGobject) {
        String value = ((PGobject) valueObject).getValue();
        rowBuffer[columnIndex] = (value == null) ? null : connection.encodeString(value);
      } else {
        switch (getSQLType(columnIndex + 1)) {

          //
          // toString() isn't enough for date and time types; we must format it correctly
          // or we won't be able to re-parse it.
          //

          case Types.DATE:
            rowBuffer[columnIndex] = connection
                .encodeString(
                    connection.getTimestampUtils().toString(
                        getDefaultCalendar(), (Date) valueObject));
            break;

          case Types.TIME:
            rowBuffer[columnIndex] = connection
                .encodeString(
                    connection.getTimestampUtils().toString(
                        getDefaultCalendar(), (Time) valueObject));
            break;

          case Types.TIMESTAMP:
            rowBuffer[columnIndex] = connection.encodeString(
                connection.getTimestampUtils().toString(
                    getDefaultCalendar(), (Timestamp) valueObject));
            break;

          case Types.NULL:
            // Should never happen?
            break;

          case Types.BINARY:
          case Types.LONGVARBINARY:
          case Types.VARBINARY:
            if (isBinary(columnIndex + 1)) {
              rowBuffer[columnIndex] = (byte[]) valueObject;
            } else {
              try {
                rowBuffer[columnIndex] =
                    PGbytea.toPGString((byte[]) valueObject).getBytes("ISO-8859-1");
              } catch (UnsupportedEncodingException e) {
                throw new PSQLException(
                    GT.tr("The JVM claims not to support the encoding: {0}", "ISO-8859-1"),
                    PSQLState.UNEXPECTED_ERROR, e);
              }
            }
            break;

          default:
            rowBuffer[columnIndex] = connection.encodeString(String.valueOf(valueObject));
            break;
        }

      }
    }
  }

  public class CursorResultHandler extends ResultHandlerBase {

    public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
        ResultCursor cursor) {
      PgResultSet.this.rows = tuples;
      PgResultSet.this.cursor = cursor;
    }

    public void handleCommandStatus(String status, int updateCount, long insertOID) {
      handleError(new PSQLException(GT.tr("Unexpected command status: {0}.", status),
          PSQLState.PROTOCOL_VIOLATION));
    }

    public void handleCompletion() throws SQLException {
      SQLWarning warning = getWarning();
      if (warning != null) {
        PgResultSet.this.addWarning(warning);
      }
      super.handleCompletion();
    }
  }


  public BaseStatement getPGStatement() {
    return statement;
  }

  //
  // Backwards compatibility with PGRefCursorResultSet
  //

  private String refCursorName;

  public String getRefCursor() {
    // Can't check this because the PGRefCursorResultSet
    // interface doesn't allow throwing a SQLException
    //
    // checkClosed();
    return refCursorName;
  }

  private void setRefCursor(String refCursorName) {
    this.refCursorName = refCursorName;
  }

  public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    if (rows < 0) {
      throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    fetchSize = rows;
  }

  public int getFetchSize() throws SQLException {
    checkClosed();
    return fetchSize;
  }

  public boolean next() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      throw new PSQLException(GT.tr("Can''t use relative move methods while on the insert row."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (current_row + 1 >= rows.size()) {
      if (cursor == null || (maxRows > 0 && row_offset + rows.size() >= maxRows)) {
        current_row = rows.size();
        this_row = null;
        rowBuffer = null;
        return false; // End of the resultset.
      }

      // Ask for some more data.
      row_offset += rows.size(); // We are discarding some data.

      int fetchRows = fetchSize;
      if (maxRows != 0) {
        if (fetchRows == 0 || row_offset + fetchRows > maxRows) {
          // Fetch would exceed maxRows, limit it.
          fetchRows = maxRows - row_offset;
        }
      }

      // Execute the fetch and update this resultset.
      connection.getQueryExecutor().fetch(cursor, new CursorResultHandler(), fetchRows);

      current_row = 0;

      // Test the new rows array.
      if (rows.isEmpty()) {
        this_row = null;
        rowBuffer = null;
        return false;
      }
    } else {
      current_row++;
    }

    initRowBuffer();
    return true;
  }

  public void close() throws SQLException {
    try {
      // release resources held (memory for tuples)
      rows = null;
      if (cursor != null) {
        cursor.close();
        cursor = null;
      }
    } finally {
      ((PgStatement) statement).checkCompletion();
    }
  }

  public boolean wasNull() throws SQLException {
    checkClosed();
    return wasNullFlag;
  }

  public String getString(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    // varchar in binary is same as text, other binary fields are converted to their text format
    if (isBinary(columnIndex) && getSQLType(columnIndex) != Types.VARCHAR) {
      Field field = fields[columnIndex - 1];
      Object obj = internalGetObject(columnIndex, field);
      if (obj == null) {
        return null;
      }
      // hack to be compatible with text protocol
      if (obj instanceof java.util.Date) {
        int oid = field.getOID();
        return connection.getTimestampUtils().timeToString((java.util.Date) obj,
            oid == Oid.TIMESTAMPTZ || oid == Oid.TIMETZ);
      }
      if ("hstore".equals(getPGType(columnIndex))) {
        return HStoreConverter.toString((Map<?, ?>) obj);
      }
      return trimString(columnIndex, obj.toString());
    }

    Encoding encoding = connection.getEncoding();
    try {
      return trimString(columnIndex, encoding.decode(this_row[columnIndex - 1]));
    } catch (IOException ioe) {
      throw new PSQLException(
          GT.tr(
              "Invalid character data was found.  This is most likely caused by stored data containing characters that are invalid for the character set the database was created in.  The most common example of this is storing 8bit data in a SQL_ASCII database."),
          PSQLState.DATA_ERROR, ioe);
    }
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return false; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      return readDoubleValue(this_row[col], fields[col].getOID(), "boolean") == 1;
    }

    return toBoolean(getString(columnIndex));
  }

  private static final BigInteger BYTEMAX = new BigInteger(Byte.toString(Byte.MAX_VALUE));
  private static final BigInteger BYTEMIN = new BigInteger(Byte.toString(Byte.MIN_VALUE));

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      // there is no Oid for byte so must always do conversion from
      // some other numeric type
      return (byte) readLongValue(this_row[col], fields[col].getOID(), Byte.MIN_VALUE,
          Byte.MAX_VALUE, "byte");
    }

    String s = getString(columnIndex);

    if (s != null) {
      s = s.trim();
      if (s.isEmpty()) {
        return 0;
      }
      try {
        // try the optimal parse
        return Byte.parseByte(s);
      } catch (NumberFormatException e) {
        // didn't work, assume the column is not a byte
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();

          int gt = i.compareTo(BYTEMAX);
          int lt = i.compareTo(BYTEMIN);

          if (gt > 0 || lt < 0) {
            throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "byte", s),
                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.byteValue();
        } catch (NumberFormatException ex) {
          throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "byte", s),
              PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  private static final BigInteger SHORTMAX = new BigInteger(Short.toString(Short.MAX_VALUE));
  private static final BigInteger SHORTMIN = new BigInteger(Short.toString(Short.MIN_VALUE));

  @Override
  public short getShort(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT2) {
        return ByteConverter.int2(this_row[col], 0);
      }
      return (short) readLongValue(this_row[col], oid, Short.MIN_VALUE, Short.MAX_VALUE, "short");
    }

    String s = getFixedString(columnIndex);

    if (s != null) {
      s = s.trim();
      try {
        return Short.parseShort(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(SHORTMAX);
          int lt = i.compareTo(SHORTMIN);

          if (gt > 0 || lt < 0) {
            throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "short", s),
                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.shortValue();

        } catch (NumberFormatException ne) {
          throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "short", s),
              PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public int getInt(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT4) {
        return ByteConverter.int4(this_row[col], 0);
      }
      return (int) readLongValue(this_row[col], oid, Integer.MIN_VALUE, Integer.MAX_VALUE, "int");
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        return getFastInt(columnIndex);
      } catch (NumberFormatException ex) {
      }
    }
    return toInt(getFixedString(columnIndex));
  }

  public long getLong(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT8) {
        return ByteConverter.int8(this_row[col], 0);
      }
      return readLongValue(this_row[col], oid, Long.MIN_VALUE, Long.MAX_VALUE, "long");
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        return getFastLong(columnIndex);
      } catch (NumberFormatException ex) {
      }
    }
    return toLong(getFixedString(columnIndex));
  }

  /**
   * A dummy exception thrown when fast byte[] to number parsing fails and no value can be returned.
   * The exact stack trace does not matter because the exception is always caught and is not visible
   * to users.
   */
  private static final NumberFormatException FAST_NUMBER_FAILED = new NumberFormatException() {

    // Override fillInStackTrace to prevent memory leak via Throwable.backtrace hidden field
    // The field is not observable via reflection, however when throwable contains stacktrace, it
    // does
    // hold strong references to user objects (e.g. classes -> classloaders), thus it might lead to
    // OutOfMemory conditions.
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  };

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toLong(String)}.
   */
  private long getFastLong(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = this_row[columnIndex - 1];

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    long val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 19) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 18) {
        throw FAST_NUMBER_FAILED;
      }
    }

    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        throw FAST_NUMBER_FAILED;
      }

      val *= 10;
      val += b - '0';
    }

    if (neg) {
      val = -val;
    }

    return val;
  }

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toInt(String)}.
   */
  private int getFastInt(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = this_row[columnIndex - 1];

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    int val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 10) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 9) {
        throw FAST_NUMBER_FAILED;
      }
    }

    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        throw FAST_NUMBER_FAILED;
      }

      val *= 10;
      val += b - '0';
    }

    if (neg) {
      val = -val;
    }

    return val;
  }

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toBigDecimal(String, int)}.
   */
  private BigDecimal getFastBigDecimal(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = this_row[columnIndex - 1];

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    int scale = 0;
    long val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 19) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 18) {
        throw FAST_NUMBER_FAILED;
      }
    }

    int periodsSeen = 0;
    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        if (b == '.') {
          scale = bytes.length - start;
          periodsSeen++;
          continue;
        } else {
          throw FAST_NUMBER_FAILED;
        }
      }
      val *= 10;
      val += b - '0';
    }

    int numNonSignChars = neg ? bytes.length - 1 : bytes.length;
    if (periodsSeen > 1 || periodsSeen == numNonSignChars) {
      throw FAST_NUMBER_FAILED;
    }

    if (neg) {
      val = -val;
    }

    return BigDecimal.valueOf(val, scale);
  }

  public float getFloat(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.FLOAT4) {
        return ByteConverter.float4(this_row[col], 0);
      }
      return (float) readDoubleValue(this_row[col], oid, "float");
    }

    return toFloat(getFixedString(columnIndex));
  }

  public double getDouble(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.FLOAT8) {
        return ByteConverter.float8(this_row[col], 0);
      }
      return readDoubleValue(this_row[col], oid, "double");
    }

    return toDouble(getFixedString(columnIndex));
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      int sqlType = getSQLType(columnIndex);
      if (sqlType != Types.NUMERIC && sqlType != Types.DECIMAL) {
        Object obj = internalGetObject(columnIndex, fields[columnIndex - 1]);
        if (obj == null) {
          return null;
        }
        if (obj instanceof Long || obj instanceof Integer || obj instanceof Byte) {
          BigDecimal res = BigDecimal.valueOf(((Number) obj).longValue());
          res = scaleBigDecimal(res, scale);
          return res;
        }
        return toBigDecimal(trimMoney(String.valueOf(obj)), scale);
      }
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        BigDecimal res = getFastBigDecimal(columnIndex);
        res = scaleBigDecimal(res, scale);
        return res;
      } catch (NumberFormatException ex) {
      }
    }

    return toBigDecimal(getFixedString(columnIndex), scale);
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * In normal use, the bytes represent the raw values returned by the backend. However, if the
   * column is an OID, then it is assumed to refer to a Large Object, and that object is returned as
   * a byte array.
   *
   * <p>
   * <b>Be warned</b> If the large object is huge, then you may run out of memory.
   */
  public byte[] getBytes(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      // If the data is already binary then just return it
      return this_row[columnIndex - 1];
    } else if (connection.haveMinimumCompatibleVersion(ServerVersion.v7_2)) {
      // Version 7.2 supports the bytea datatype for byte arrays
      if (fields[columnIndex - 1].getOID() == Oid.BYTEA) {
        return trimBytes(columnIndex, PGbytea.toBytes(this_row[columnIndex - 1]));
      } else {
        return trimBytes(columnIndex, this_row[columnIndex - 1]);
      }
    } else {
      // Version 7.1 and earlier supports LargeObjects for byte arrays
      // Handle OID's as BLOBS
      if (fields[columnIndex - 1].getOID() == Oid.OID) {
        LargeObjectManager lom = connection.getLargeObjectAPI();
        LargeObject lob = lom.open(getLong(columnIndex));
        byte buf[] = lob.read(lob.size());
        lob.close();
        return trimBytes(columnIndex, buf);
      } else {
        return trimBytes(columnIndex, this_row[columnIndex - 1]);
      }
    }
  }

  public java.sql.Date getDate(int columnIndex) throws SQLException {
    return getDate(columnIndex, null);
  }

  public Time getTime(int columnIndex) throws SQLException {
    return getTime(columnIndex, null);
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(columnIndex, null);
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (connection.haveMinimumCompatibleVersion(ServerVersion.v7_2)) {
      // Version 7.2 supports AsciiStream for all the PG text types
      // As the spec/javadoc for this method indicate this is to be used for
      // large text values (i.e. LONGVARCHAR) PG doesn't have a separate
      // long string datatype, but with toast the text datatype is capable of
      // handling very large values. Thus the implementation ends up calling
      // getString() since there is no current way to stream the value from the server
      try {
        return new ByteArrayInputStream(getString(columnIndex).getBytes("ASCII"));
      } catch (UnsupportedEncodingException l_uee) {
        throw new PSQLException(GT.tr("The JVM claims not to support the encoding: {0}", "ASCII"),
            PSQLState.UNEXPECTED_ERROR, l_uee);
      }
    } else {
      // In 7.1 Handle as BLOBS so return the LargeObject input stream
      return getBinaryStream(columnIndex);
    }
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (connection.haveMinimumCompatibleVersion(ServerVersion.v7_2)) {
      // Version 7.2 supports AsciiStream for all the PG text types
      // As the spec/javadoc for this method indicate this is to be used for
      // large text values (i.e. LONGVARCHAR) PG doesn't have a separate
      // long string datatype, but with toast the text datatype is capable of
      // handling very large values. Thus the implementation ends up calling
      // getString() since there is no current way to stream the value from the server
      try {
        return new ByteArrayInputStream(getString(columnIndex).getBytes("UTF-8"));
      } catch (UnsupportedEncodingException l_uee) {
        throw new PSQLException(GT.tr("The JVM claims not to support the encoding: {0}", "UTF-8"),
            PSQLState.UNEXPECTED_ERROR, l_uee);
      }
    } else {
      // In 7.1 Handle as BLOBS so return the LargeObject input stream
      return getBinaryStream(columnIndex);
    }
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (connection.haveMinimumCompatibleVersion(ServerVersion.v7_2)) {
      // Version 7.2 supports BinaryStream for all PG bytea type
      // As the spec/javadoc for this method indicate this is to be used for
      // large binary values (i.e. LONGVARBINARY) PG doesn't have a separate
      // long binary datatype, but with toast the bytea datatype is capable of
      // handling very large values. Thus the implementation ends up calling
      // getBytes() since there is no current way to stream the value from the server
      byte b[] = getBytes(columnIndex);
      if (b != null) {
        return new ByteArrayInputStream(b);
      }
    } else {
      // In 7.1 Handle as BLOBS so return the LargeObject input stream
      if (fields[columnIndex - 1].getOID() == Oid.OID) {
        LargeObjectManager lom = connection.getLargeObjectAPI();
        LargeObject lob = lom.open(getLong(columnIndex));
        return lob.getInputStream();
      }
    }
    return null;
  }

  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  public byte getByte(String columnName) throws SQLException {

    return getByte(findColumn(columnName));
  }

  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  public byte[] getBytes(String columnName) throws SQLException {
    return getBytes(findColumn(columnName));
  }

  public java.sql.Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName), null);
  }

  public Time getTime(String columnName) throws SQLException {
    return getTime(findColumn(columnName), null);
  }

  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName), null);
  }

  public InputStream getAsciiStream(String columnName) throws SQLException {
    return getAsciiStream(findColumn(columnName));
  }

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    return getUnicodeStream(findColumn(columnName));
  }

  public InputStream getBinaryStream(String columnName) throws SQLException {
    return getBinaryStream(findColumn(columnName));
  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    return warnings;
  }

  public void clearWarnings() throws SQLException {
    checkClosed();
    warnings = null;
  }

  protected void addWarning(SQLWarning warnings) {
    if (this.warnings != null) {
      this.warnings.setNextWarning(warnings);
    } else {
      this.warnings = warnings;
    }
  }

  public String getCursorName() throws SQLException {
    checkClosed();
    return null;
  }

  public Object getObject(int columnIndex) throws SQLException {
    Field field;

    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    field = fields[columnIndex - 1];

    // some fields can be null, mainly from those returned by MetaData methods
    if (field == null) {
      wasNullFlag = true;
      return null;
    }

    Object result = internalGetObject(columnIndex, field);
    if (result != null) {
      return result;
    }

    if (isBinary(columnIndex)) {
      return connection.getObject(getPGType(columnIndex), null, this_row[columnIndex - 1]);
    }
    return connection.getObject(getPGType(columnIndex), getString(columnIndex), null);
  }

  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  public int findColumn(String columnName) throws SQLException {
    checkClosed();

    int col = findColumnIndex(columnName);
    if (col == 0) {
      throw new PSQLException(
          GT.tr("The column name {0} was not found in this ResultSet.", columnName),
          PSQLState.UNDEFINED_COLUMN);
    }
    return col;
  }

  public static Map<String, Integer> createColumnNameIndexMap(Field[] fields,
      boolean isSanitiserDisabled) {
    Map<String, Integer> columnNameIndexMap = new HashMap<String, Integer>(fields.length * 2);
    // The JDBC spec says when you have duplicate columns names,
    // the first one should be returned. So load the map in
    // reverse order so the first ones will overwrite later ones.
    for (int i = fields.length - 1; i >= 0; i--) {
      String columnLabel = fields[i].getColumnLabel();
      if (isSanitiserDisabled) {
        columnNameIndexMap.put(columnLabel, i + 1);
      } else {
        columnNameIndexMap.put(columnLabel.toLowerCase(Locale.US), i + 1);
      }
    }
    return columnNameIndexMap;
  }

  private int findColumnIndex(String columnName) {
    if (columnNameIndexMap == null) {
      if (originalQuery != null) {
        columnNameIndexMap = originalQuery.getResultSetColumnNameIndexMap();
      }
      if (columnNameIndexMap == null) {
        columnNameIndexMap = createColumnNameIndexMap(fields, connection.isColumnSanitiserDisabled());
      }
    }

    Integer index = columnNameIndexMap.get(columnName);
    if (index != null) {
      return index;
    }

    index = columnNameIndexMap.get(columnName.toLowerCase(Locale.US));
    if (index != null) {
      columnNameIndexMap.put(columnName, index);
      return index;
    }

    index = columnNameIndexMap.get(columnName.toUpperCase(Locale.US));
    if (index != null) {
      columnNameIndexMap.put(columnName, index);
      return index;
    }

    return 0;
  }

  /**
   * Returns the OID of a field. It is used internally by the driver.
   *
   * @param field field index
   * @return OID of a field
   */
  public int getColumnOID(int field) {
    return fields[field - 1].getOID();
  }

  /**
   * This is used to fix get*() methods on Money fields. It should only be used by those methods!
   *
   * It converts ($##.##) to -##.## and $##.## to ##.##
   *
   * @param col column position (1-based)
   * @return numeric-parsable representation of money string literal
   * @throws SQLException if something wrong happens
   */
  public String getFixedString(int col) throws SQLException {
    return trimMoney(getString(col));
  }

  private String trimMoney(String s) {
    if (s == null) {
      return null;
    }

    // if we don't have at least 2 characters it can't be money.
    if (s.length() < 2) {
      return s;
    }

    // Handle Money
    char ch = s.charAt(0);

    // optimise for non-money type: return immediately with one check
    // if the first char cannot be '(', '$' or '-'
    if (ch > '-') {
      return s;
    }

    if (ch == '(') {
      s = "-" + PGtokenizer.removePara(s).substring(1);
    } else if (ch == '$') {
      s = s.substring(1);
    } else if (ch == '-' && s.charAt(1) == '$') {
      s = "-" + s.substring(2);
    }

    return s;
  }

  protected String getPGType(int column) throws SQLException {
    Field field = fields[column - 1];
    initSqlType(field);
    return field.getPGType();
  }

  protected int getSQLType(int column) throws SQLException {
    Field field = fields[column - 1];
    initSqlType(field);
    return field.getSQLType();
  }

  private void initSqlType(Field field) throws SQLException {
    if (field.isTypeInitialized()) {
      return;
    }
    TypeInfo typeInfo = connection.getTypeInfo();
    int oid = field.getOID();
    String pgType = typeInfo.getPGType(oid);
    int sqlType = typeInfo.getSQLType(pgType);
    field.setSQLType(sqlType);
    field.setPGType(pgType);
  }

  private void checkUpdateable() throws SQLException {
    checkClosed();

    if (!isUpdateable()) {
      throw new PSQLException(
          GT.tr(
              "ResultSet is not updateable.  The query that generated this result set must select only one table, and must select all primary keys from that table. See the JDBC 2.1 API Specification, section 5.6 for more details."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    if (updateValues == null) {
      // allow every column to be updated without a rehash.
      updateValues = new HashMap<String, Object>((int) (fields.length / 0.75), 0.75f);
    }
  }

  protected void checkClosed() throws SQLException {
    if (rows == null) {
      throw new PSQLException(GT.tr("This ResultSet is closed."), PSQLState.OBJECT_NOT_IN_STATE);
    }
  }

  /*
   * for jdbc3 to call internally
   */
  protected boolean isResultSetClosed() {
    return rows == null;
  }

  protected void checkColumnIndex(int column) throws SQLException {
    if (column < 1 || column > fields.length) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              column, fields.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  /**
   * Checks that the result set is not closed, it's positioned on a valid row and that the given
   * column number is valid. Also updates the {@link #wasNullFlag} to correct value.
   *
   * @param column The column number to check. Range starts from 1.
   * @throws SQLException If state or column is invalid.
   */
  protected void checkResultSet(int column) throws SQLException {
    checkClosed();
    if (this_row == null) {
      throw new PSQLException(
          GT.tr("ResultSet not positioned properly, perhaps you need to call next."),
          PSQLState.INVALID_CURSOR_STATE);
    }
    checkColumnIndex(column);
    wasNullFlag = (this_row[column - 1] == null);
  }

  /**
   * Returns true if the value of the given column is in binary format.
   *
   * @param column The column to check. Range starts from 1.
   * @return True if the column is in binary format.
   */
  protected boolean isBinary(int column) {
    return fields[column - 1].getFormat() == Field.BINARY_FORMAT;
  }

  // ----------------- Formatting Methods -------------------

  public static boolean toBoolean(String s) {
    if (s != null) {
      s = s.trim();

      if (s.equalsIgnoreCase("t") || s.equalsIgnoreCase("true") || s.equals("1")) {
        return true;
      }

      if (s.equalsIgnoreCase("f") || s.equalsIgnoreCase("false") || s.equals("0")) {
        return false;
      }

      try {
        if (Double.parseDouble(s) == 1) {
          return true;
        }
      } catch (NumberFormatException e) {
      }
    }
    return false; // SQL NULL
  }

  private static final BigInteger INTMAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
  private static final BigInteger INTMIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));

  public static int toInt(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();

          int gt = i.compareTo(INTMAX);
          int lt = i.compareTo(INTMIN);

          if (gt > 0 || lt < 0) {
            throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "int", s),
                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.intValue();

        } catch (NumberFormatException ne) {
          throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "int", s),
              PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  private final static BigInteger LONGMAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  private final static BigInteger LONGMIN = new BigInteger(Long.toString(Long.MIN_VALUE));

  public static long toLong(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(LONGMAX);
          int lt = i.compareTo(LONGMIN);

          if (gt > 0 || lt < 0) {
            throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "long", s),
                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.longValue();
        } catch (NumberFormatException ne) {
          throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "long", s),
              PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public static BigDecimal toBigDecimal(String s) throws SQLException {
    if (s == null) {
      return null;
    }
    try {
      s = s.trim();
      return new BigDecimal(s);
    } catch (NumberFormatException e) {
      throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "BigDecimal", s),
          PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
  }

  public BigDecimal toBigDecimal(String s, int scale) throws SQLException {
    if (s == null) {
      return null;
    }
    BigDecimal val = toBigDecimal(s);
    return scaleBigDecimal(val, scale);
  }

  private BigDecimal scaleBigDecimal(BigDecimal val, int scale) throws PSQLException {
    if (scale == -1) {
      return val;
    }
    try {
      return val.setScale(scale);
    } catch (ArithmeticException e) {
      throw new PSQLException(
          GT.tr("Bad value for type {0} : {1}", "BigDecimal", val),
          PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
  }

  public static float toFloat(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Float.parseFloat(s);
      } catch (NumberFormatException e) {
        throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "float", s),
            PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  public static double toDouble(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        throw new PSQLException(GT.tr("Bad value for type {0} : {1}", "double", s),
            PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  private void initRowBuffer() {
    this_row = rows.get(current_row);
    // We only need a copy of the current row if we're going to
    // modify it via an updatable resultset.
    if (resultsetconcurrency == ResultSet.CONCUR_UPDATABLE) {
      rowBuffer = new byte[this_row.length][];
      System.arraycopy(this_row, 0, rowBuffer, 0, this_row.length);
    } else {
      rowBuffer = null;
    }
  }

  private boolean isColumnTrimmable(int columnIndex) throws SQLException {
    switch (getSQLType(columnIndex)) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return true;
    }
    return false;
  }

  private byte[] trimBytes(int p_columnIndex, byte[] p_bytes) throws SQLException {
    // we need to trim if maxsize is set and the length is greater than maxsize and the
    // type of this column is a candidate for trimming
    if (maxFieldSize > 0 && p_bytes.length > maxFieldSize && isColumnTrimmable(p_columnIndex)) {
      byte[] l_bytes = new byte[maxFieldSize];
      System.arraycopy(p_bytes, 0, l_bytes, 0, maxFieldSize);
      return l_bytes;
    } else {
      return p_bytes;
    }
  }

  private String trimString(int p_columnIndex, String p_string) throws SQLException {
    // we need to trim if maxsize is set and the length is greater than maxsize and the
    // type of this column is a candidate for trimming
    if (maxFieldSize > 0 && p_string.length() > maxFieldSize && isColumnTrimmable(p_columnIndex)) {
      return p_string.substring(0, maxFieldSize);
    } else {
      return p_string;
    }
  }

  /**
   * Converts any numeric binary field to double value. This method does no overflow checking.
   *
   * @param bytes The bytes of the numeric field.
   * @param oid The oid of the field.
   * @param targetType The target type. Used for error reporting.
   * @return The value as double.
   * @throws PSQLException If the field type is not supported numeric type.
   */
  private double readDoubleValue(byte[] bytes, int oid, String targetType) throws PSQLException {
    // currently implemented binary encoded fields
    switch (oid) {
      case Oid.INT2:
        return ByteConverter.int2(bytes, 0);
      case Oid.INT4:
        return ByteConverter.int4(bytes, 0);
      case Oid.INT8:
        // might not fit but there still should be no overflow checking
        return ByteConverter.int8(bytes, 0);
      case Oid.FLOAT4:
        return ByteConverter.float4(bytes, 0);
      case Oid.FLOAT8:
        return ByteConverter.float8(bytes, 0);
    }
    throw new PSQLException(GT.tr("Cannot convert the column of type {0} to requested type {1}.",
        Oid.toString(oid), targetType), PSQLState.DATA_TYPE_MISMATCH);
  }

  /**
   * Converts any numeric binary field to long value.
   * <p>
   * This method is used by getByte,getShort,getInt and getLong. It must support a subset of the
   * following java types that use Binary encoding. (fields that use text encoding use a different
   * code path).
   * <p>
   * <code>byte,short,int,long,float,double,BigDecimal,boolean,string</code>.
   *
   * @param bytes The bytes of the numeric field.
   * @param oid The oid of the field.
   * @param minVal the minimum value allowed.
   * @param minVal the maximum value allowed.
   * @param targetType The target type. Used for error reporting.
   * @return The value as long.
   * @throws PSQLException If the field type is not supported numeric type or if the value is out of
   *         range.
   */
  private long readLongValue(byte[] bytes, int oid, long minVal, long maxVal, String targetType)
      throws PSQLException {
    long val;
    // currently implemented binary encoded fields
    switch (oid) {
      case Oid.INT2:
        val = ByteConverter.int2(bytes, 0);
        break;
      case Oid.INT4:
        val = ByteConverter.int4(bytes, 0);
        break;
      case Oid.INT8:
        val = ByteConverter.int8(bytes, 0);
        break;
      case Oid.FLOAT4:
        val = (long) ByteConverter.float4(bytes, 0);
        break;
      case Oid.FLOAT8:
        val = (long) ByteConverter.float8(bytes, 0);
        break;
      default:
        throw new PSQLException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), targetType),
            PSQLState.DATA_TYPE_MISMATCH);
    }
    if (val < minVal || val > maxVal) {
      throw new PSQLException(GT.tr("Bad value for type {0} : {1}", targetType, val),
          PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return val;
  }

  protected void updateValue(int columnIndex, Object value) throws SQLException {
    checkUpdateable();

    if (!onInsertRow && (isBeforeFirst() || isAfterLast() || rows.isEmpty())) {
      throw new PSQLException(
          GT.tr(
              "Cannot update the ResultSet because it is either before the start or after the end of the results."),
          PSQLState.INVALID_CURSOR_STATE);
    }

    checkColumnIndex(columnIndex);

    doingUpdates = !onInsertRow;
    if (value == null) {
      updateNull(columnIndex);
    } else {
      PGResultSetMetaData md = (PGResultSetMetaData) getMetaData();
      updateValues.put(md.getBaseColumnName(columnIndex), value);
    }
  }

  protected Object getUUID(String data) throws SQLException {
    UUID uuid;
    try {
      uuid = UUID.fromString(data);
    } catch (IllegalArgumentException iae) {
      throw new PSQLException(GT.tr("Invalid UUID data."), PSQLState.INVALID_PARAMETER_VALUE, iae);
    }

    return uuid;
  }

  protected Object getUUID(byte[] data) throws SQLException {
    return new UUID(ByteConverter.int8(data, 0), ByteConverter.int8(data, 8));
  }

  private class PrimaryKey {
    int index; // where in the result set is this primaryKey
    String name; // what is the columnName of this primary Key

    PrimaryKey(int index, String name) {
      this.index = index;
      this.name = name;
    }

    Object getValue() throws SQLException {
      return getObject(index);
    }
  }

  ;

  //
  // We need to specify the type of NULL when updating a column to NULL, so
  // NullObject is a simple extension of PGobject that always returns null
  // values but retains column type info.
  //

  static class NullObject extends PGobject {
    NullObject(String type) {
      setType(type);
    }

    public String getValue() {
      return null;
    }
  }

  ;

  /**
   * Used to add rows to an already existing ResultSet that exactly match the existing rows.
   * Currently only used for assembling generated keys from batch statement execution.
   */
  void addRows(List<byte[][]> tuples) {
    rows.addAll(tuples);
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateRef(int,Ref)");
  }

  public void updateRef(String columnName, Ref x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateRef(String,Ref)");
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateBlob(int,Blob)");
  }

  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateBlob(String,Blob)");
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateClob(int,Clob)");
  }

  public void updateClob(String columnName, Clob x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateClob(String,Clob)");
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    updateObject(columnIndex, x);
  }

  public void updateArray(String columnName, Array x) throws SQLException {
    updateArray(findColumn(columnName), x);
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if (type == null) {
      throw new SQLException("type is null");
    }
    int sqlType = getSQLType(columnIndex);
    if (type == BigDecimal.class) {
      if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
        return type.cast(getBigDecimal(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == String.class) {
      if (sqlType == Types.CHAR || sqlType == Types.VARCHAR) {
        return type.cast(getString(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Boolean.class) {
      if (sqlType == Types.BOOLEAN || sqlType == Types.BIT) {
        boolean booleanValue = getBoolean(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(booleanValue);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Integer.class) {
      if (sqlType == Types.SMALLINT || sqlType == Types.INTEGER) {
        int intValue = getInt(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(intValue);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Long.class) {
      if (sqlType == Types.BIGINT) {
        long longValue = getLong(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(longValue);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Float.class) {
      if (sqlType == Types.REAL) {
        float floatValue = getFloat(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(floatValue);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Double.class) {
      if (sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
        double doubleValue = getDouble(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(doubleValue);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Date.class) {
      if (sqlType == Types.DATE) {
        return type.cast(getDate(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Time.class) {
      if (sqlType == Types.TIME) {
        return type.cast(getTime(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Timestamp.class) {
      if (sqlType == Types.TIMESTAMP
              //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
              || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
      //#endif
      ) {
        return type.cast(getTimestamp(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Calendar.class) {
      if (sqlType == Types.TIMESTAMP
              //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
              || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
      //#endif
      ) {
        Timestamp timestampValue = getTimestamp(columnIndex);
        Calendar calendar = Calendar.getInstance(getDefaultCalendar().getTimeZone());
        calendar.setTimeInMillis(timestampValue.getTime());
        return type.cast(calendar);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Blob.class) {
      if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.BIGINT) {
        return type.cast(getBlob(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Clob.class) {
      if (sqlType == Types.CLOB || sqlType == Types.BIGINT) {
        return type.cast(getClob(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == NClob.class) {
      if (sqlType == Types.NCLOB) {
        return type.cast(getNClob(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == Array.class) {
      if (sqlType == Types.ARRAY) {
        return type.cast(getArray(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == SQLXML.class) {
      if (sqlType == Types.SQLXML) {
        return type.cast(getSQLXML(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == UUID.class) {
      return type.cast(getObject(columnIndex));
    } else if (type == InetAddress.class) {
      Object addressString = getObject(columnIndex);
      if (addressString == null) {
        return null;
      }
      try {
        return type.cast(InetAddress.getByName(((PGobject) addressString).getValue()));
      } catch (UnknownHostException e) {
        throw new SQLException("could not create inet address from string '" + addressString + "'");
      }
      // JSR-310 support
      //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
    } else if (type == LocalDate.class) {
      if (sqlType == Types.DATE) {
        Date dateValue = getDate(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(dateValue.toLocalDate());
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == LocalTime.class) {
      if (sqlType == Types.TIME) {
        Time timeValue = getTime(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(timeValue.toLocalTime());
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == LocalDateTime.class) {
      if (sqlType == Types.TIMESTAMP) {
        return type.cast(getLocalDateTime(columnIndex));
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
    } else if (type == OffsetDateTime.class) {
      if (sqlType == Types.TIMESTAMP_WITH_TIMEZONE || sqlType == Types.TIMESTAMP) {
        Timestamp timestampValue = getTimestamp(columnIndex);
        if (wasNull()) {
          return null;
        }
        // Postgres stores everything in UTC and does not keep original time zone
        OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(timestampValue.toInstant(), ZoneOffset.UTC);
        return type.cast(offsetDateTime);
      } else {
        throw new SQLException("conversion to " + type + " from " + sqlType + " not supported");
      }
      //#endif
    } else if (PGobject.class.isAssignableFrom(type)) {
      Object object;
      if (isBinary(columnIndex)) {
        object = connection.getObject(getPGType(columnIndex), null, this_row[columnIndex - 1]);
      } else {
        object = connection.getObject(getPGType(columnIndex), getString(columnIndex), null);
      }
      return type.cast(object);
    }
    throw new SQLException("unsupported conversion to " + type);
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(s, map);
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(i, map);
  }

  //#if mvn.project.property.postgresql.jdbc.spec >= "JDBC4.2"
  public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType,
      int scaleOrLength) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType,
      int scaleOrLength) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateObject");
  }
  //#endif

  public RowId getRowId(int columnIndex) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getRowId(int)");
  }

  public RowId getRowId(String columnName) throws SQLException {
    return getRowId(findColumn(columnName));
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateRowId(int, RowId)");
  }

  public void updateRowId(String columnName, RowId x) throws SQLException {
    updateRowId(findColumn(columnName), x);
  }

  public int getHoldability() throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getHoldability()");
  }

  public boolean isClosed() throws SQLException {
    return (rows == null);
  }

  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateNString(int, String)");
  }

  public void updateNString(String columnName, String nString) throws SQLException {
    updateNString(findColumn(columnName), nString);
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateNClob(int, NClob)");
  }

  public void updateNClob(String columnName, NClob nClob) throws SQLException {
    updateNClob(findColumn(columnName), nClob);
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateNClob(int, Reader)");
  }

  public void updateNClob(String columnName, Reader reader) throws SQLException {
    updateNClob(findColumn(columnName), reader);
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateNClob(int, Reader, long)");
  }

  public void updateNClob(String columnName, Reader reader, long length) throws SQLException {
    updateNClob(findColumn(columnName), reader, length);
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getNClob(int)");
  }

  public NClob getNClob(String columnName) throws SQLException {
    return getNClob(findColumn(columnName));
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateBlob(int, InputStream, long)");
  }

  public void updateBlob(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateBlob(findColumn(columnName), inputStream, length);
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateBlob(int, InputStream)");
  }

  public void updateBlob(String columnName, InputStream inputStream) throws SQLException {
    updateBlob(findColumn(columnName), inputStream);
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateClob(int, Reader, long)");
  }

  public void updateClob(String columnName, Reader reader, long length) throws SQLException {
    updateClob(findColumn(columnName), reader, length);
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "updateClob(int, Reader)");
  }

  public void updateClob(String columnName, Reader reader) throws SQLException {
    updateClob(findColumn(columnName), reader);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    String data = getString(columnIndex);
    if (data == null) {
      return null;
    }

    return new PgSQLXML(connection, data);
  }

  public SQLXML getSQLXML(String columnName) throws SQLException {
    return getSQLXML(findColumn(columnName));
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    updateValue(columnIndex, xmlObject);
  }

  public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
    updateSQLXML(findColumn(columnName), xmlObject);
  }

  public String getNString(int columnIndex) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getNString(int)");
  }

  public String getNString(String columnName) throws SQLException {
    return getNString(findColumn(columnName));
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(), "getNCharacterStream(int)");
  }

  public Reader getNCharacterStream(String columnName) throws SQLException {
    return getNCharacterStream(findColumn(columnName));
  }

  public void updateNCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader, int)");
  }

  public void updateNCharacterStream(String columnName, Reader x, int length) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x, length);
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader)");
  }

  public void updateNCharacterStream(String columnName, Reader x) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x);
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader, long)");
  }

  public void updateNCharacterStream(String columnName, Reader x, long length) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x, length);
  }

  public void updateCharacterStream(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateCharaceterStream(int, Reader, long)");
  }

  public void updateCharacterStream(String columnName, Reader reader, long length)
      throws SQLException {
    updateCharacterStream(findColumn(columnName), reader, length);
  }

  public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateCharaceterStream(int, Reader)");
  }

  public void updateCharacterStream(String columnName, Reader reader) throws SQLException {
    updateCharacterStream(findColumn(columnName), reader);
  }

  public void updateBinaryStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateBinaryStream(int, InputStream, long)");
  }

  public void updateBinaryStream(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateBinaryStream(findColumn(columnName), inputStream, length);
  }

  public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateBinaryStream(int, InputStream)");
  }

  public void updateBinaryStream(String columnName, InputStream inputStream) throws SQLException {
    updateBinaryStream(findColumn(columnName), inputStream);
  }

  public void updateAsciiStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateAsciiStream(int, InputStream, long)");
  }

  public void updateAsciiStream(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateAsciiStream(findColumn(columnName), inputStream, length);
  }

  public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
    throw org.postgresql.Driver.notImplemented(this.getClass(),
        "updateAsciiStream(int, InputStream)");
  }

  public void updateAsciiStream(String columnName, InputStream inputStream) throws SQLException {
    updateAsciiStream(findColumn(columnName), inputStream);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  private Calendar getDefaultCalendar() {
    TimestampUtils timestampUtils = connection.getTimestampUtils();
    if (timestampUtils.hasFastDefaultTimeZone()) {
      return timestampUtils.getSharedCalendar(null);
    }
    Calendar sharedCalendar = timestampUtils.getSharedCalendar(defaultTimeZone);
    if (defaultTimeZone == null) {
      defaultTimeZone = sharedCalendar.getTimeZone();
    }
    return sharedCalendar;
  }
}
