/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.core.Oid;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Utils;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.UUIDArrayAssistant;
import org.postgresql.util.ByteConverter;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.StreamWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;


/**
 * Parameter list for a single-statement V3 query.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class SimpleParameterList implements V3ParameterList {

  private final static byte IN = 1;
  private final static byte OUT = 2;
  private final static byte INOUT = IN | OUT;

  private final static byte TEXT = 0;
  private final static byte BINARY = 4;

  SimpleParameterList(int paramCount, TypeTransferModeRegistry transferModeRegistry) {
    this.paramValues = new Object[paramCount];
    this.paramTypes = new int[paramCount];
    this.encoded = new byte[paramCount][];
    this.flags = new byte[paramCount];
    this.transferModeRegistry = transferModeRegistry;
  }

  @Override
  public void registerOutParameter(int index, int sqlType) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              index, paramValues.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    flags[index - 1] |= OUT;
  }

  private void bind(int index, Object value, int oid, byte binary) throws SQLException {
    if (index < 1 || index > paramValues.length) {
      throw new PSQLException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              index, paramValues.length),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    --index;

    encoded[index] = null;
    paramValues[index] = value;
    flags[index] = (byte) (direction(index) | IN | binary);

    // If we are setting something to an UNSPECIFIED NULL, don't overwrite
    // our existing type for it. We don't need the correct type info to
    // send this value, and we don't want to overwrite and require a
    // reparse.
    if (oid == Oid.UNSPECIFIED && paramTypes[index] != Oid.UNSPECIFIED && value == NULL_OBJECT) {
      return;
    }

    paramTypes[index] = oid;
    pos = index + 1;
  }

  public int getParameterCount() {
    return paramValues.length;
  }

  public int getOutParameterCount() {
    int count = 0;
    for (int i = 0; i < paramTypes.length; i++) {
      if ((direction(i) & OUT) == OUT) {
        count++;
      }
    }
    // Every function has at least one output.
    if (count == 0) {
      count = 1;
    }
    return count;

  }

  public int getInParameterCount() {
    int count = 0;
    for (int i = 0; i < paramTypes.length; i++) {
      if (direction(i) != OUT) {
        count++;
      }
    }
    return count;
  }

  public void setIntParameter(int index, int value) throws SQLException {
    byte[] data = new byte[4];
    ByteConverter.int4(data, 0, value);
    bind(index, data, Oid.INT4, BINARY);
  }

  public void setLiteralParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
  }

  public void setStringParameter(int index, String value, int oid) throws SQLException {
    bind(index, value, oid, TEXT);
  }

  public void setBinaryParameter(int index, byte[] value, int oid) throws SQLException {
    bind(index, value, oid, BINARY);
  }

  public void setBytea(int index, byte[] data, int offset, int length) throws SQLException {
    bind(index, new StreamWrapper(data, offset, length), Oid.BYTEA, BINARY);
  }

  public void setBytea(int index, InputStream stream, int length) throws SQLException {
    bind(index, new StreamWrapper(stream, length), Oid.BYTEA, BINARY);
  }

  public void setBytea(int index, InputStream stream) throws SQLException {
    bind(index, new StreamWrapper(stream), Oid.BYTEA, BINARY);
  }

  public void setNull(int index, int oid) throws SQLException {

    byte binaryTransfer = TEXT;

    if (transferModeRegistry.useBinaryForReceive(oid)) {
      binaryTransfer = BINARY;
    }
    bind(index, NULL_OBJECT, oid, binaryTransfer);
  }

  public String toString(int index, boolean standardConformingStrings) {
    --index;
    if (paramValues[index] == null) {
      return "?";
    } else if (paramValues[index] == NULL_OBJECT) {
      return "NULL";
    } else if ((flags[index] & BINARY) == BINARY) {
      // handle some of the numeric types

      switch (paramTypes[index]) {
        case Oid.INT2:
          short s = ByteConverter.int2((byte[]) paramValues[index], 0);
          return Short.toString(s);

        case Oid.INT4:
          int i = ByteConverter.int4((byte[]) paramValues[index], 0);
          return Integer.toString(i);

        case Oid.INT8:
          long l = ByteConverter.int8((byte[]) paramValues[index], 0);
          return Long.toString(l);

        case Oid.FLOAT4:
          float f = ByteConverter.float4((byte[]) paramValues[index], 0);
          return Float.toString(f);

        case Oid.FLOAT8:
          double d = ByteConverter.float8((byte[]) paramValues[index], 0);
          return Double.toString(d);

        case Oid.UUID:
          String uuid =
              new UUIDArrayAssistant().buildElement((byte[]) paramValues[index], 0, 16).toString();
          return "'" + uuid + "'::uuid";

        case Oid.POINT:
          PGpoint pgPoint = new PGpoint();
          pgPoint.setByteValue((byte[]) paramValues[index], 0);
          return "'" + pgPoint.toString() + "'::point";

        case Oid.BOX:
          PGbox pgBox = new PGbox();
          pgBox.setByteValue((byte[]) paramValues[index], 0);
          return "'" + pgBox.toString() + "'::box";
      }
      return "?";
    } else {
      String param = paramValues[index].toString();

      // add room for quotes + potential escaping.
      StringBuilder p = new StringBuilder(3 + param.length() * 11 / 10);

      // No E'..' here since escapeLiteral escapes all things and it does not use \123 kind of
      // escape codes
      p.append('\'');
      try {
        p = Utils.escapeLiteral(p, param, standardConformingStrings);
      } catch (SQLException sqle) {
        // This should only happen if we have an embedded null
        // and there's not much we can do if we do hit one.
        //
        // The goal of toString isn't to be sent to the server,
        // so we aren't 100% accurate (see StreamWrapper), put
        // the unescaped version of the data.
        //
        p.append(param);
      }
      p.append('\'');
      int paramType = paramTypes[index];
      if (paramType == Oid.TIMESTAMP) {
        p.append("::timestamp");
      } else if (paramType == Oid.TIMESTAMPTZ) {
        p.append("::timestamp with time zone");
      } else if (paramType == Oid.TIME) {
        p.append("::time");
      } else if (paramType == Oid.TIMETZ) {
        p.append("::time with time zone");
      } else if (paramType == Oid.DATE) {
        p.append("::date");
      } else if (paramType == Oid.INTERVAL) {
        p.append("::interval");
      }
      return p.toString();
    }
  }

  public void checkAllParametersSet() throws SQLException {
    for (int i = 0; i < paramTypes.length; ++i) {
      if (direction(i) != OUT && paramValues[i] == null) {
        throw new PSQLException(GT.tr("No value specified for parameter {0}.", i + 1),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    }
  }

  public void convertFunctionOutParameters() {
    for (int i = 0; i < paramTypes.length; ++i) {
      if (direction(i) == OUT) {
        paramTypes[i] = Oid.VOID;
        paramValues[i] = "null";
      }
    }
  }

  //
  // bytea helper
  //

  private static void streamBytea(PGStream pgStream, StreamWrapper wrapper) throws IOException {
    byte[] rawData = wrapper.getBytes();
    if (rawData != null) {
      pgStream.send(rawData, wrapper.getOffset(), wrapper.getLength());
      return;
    }

    pgStream.sendStream(wrapper.getStream(), wrapper.getLength());
  }

  public int[] getTypeOIDs() {
    return paramTypes;
  }

  //
  // Package-private V3 accessors
  //

  int getTypeOID(int index) {
    return paramTypes[index - 1];
  }

  boolean hasUnresolvedTypes() {
    for (int paramType : paramTypes) {
      if (paramType == Oid.UNSPECIFIED) {
        return true;
      }
    }
    return false;
  }

  void setResolvedType(int index, int oid) {
    // only allow overwriting an unknown value
    if (paramTypes[index - 1] == Oid.UNSPECIFIED) {
      paramTypes[index - 1] = oid;
    } else if (paramTypes[index - 1] != oid) {
      throw new IllegalArgumentException("Can't change resolved type for param: " + index + " from "
          + paramTypes[index - 1] + " to " + oid);
    }
  }

  boolean isNull(int index) {
    return (paramValues[index - 1] == NULL_OBJECT);
  }

  boolean isBinary(int index) {
    return (flags[index - 1] & BINARY) != 0;
  }

  private byte direction(int index) {
    return (byte) (flags[index] & INOUT);
  }

  int getV3Length(int index) {
    --index;

    // Null?
    if (paramValues[index] == NULL_OBJECT) {
      throw new IllegalArgumentException("can't getV3Length() on a null parameter");
    }

    // Directly encoded?
    if (paramValues[index] instanceof byte[]) {
      return ((byte[]) paramValues[index]).length;
    }

    // Binary-format bytea?
    if (paramValues[index] instanceof StreamWrapper) {
      return ((StreamWrapper) paramValues[index]).getLength();
    }

    // Already encoded?
    if (encoded[index] == null) {
      // Encode value and compute actual length using UTF-8.
      encoded[index] = Utils.encodeUTF8(paramValues[index].toString());
    }

    return encoded[index].length;
  }

  void writeV3Value(int index, PGStream pgStream) throws IOException {
    --index;

    // Null?
    if (paramValues[index] == NULL_OBJECT) {
      throw new IllegalArgumentException("can't writeV3Value() on a null parameter");
    }

    // Directly encoded?
    if (paramValues[index] instanceof byte[]) {
      pgStream.send((byte[]) paramValues[index]);
      return;
    }

    // Binary-format bytea?
    if (paramValues[index] instanceof StreamWrapper) {
      streamBytea(pgStream, (StreamWrapper) paramValues[index]);
      return;
    }

    // Encoded string.
    if (encoded[index] == null) {
      encoded[index] = Utils.encodeUTF8((String) paramValues[index]);
    }
    pgStream.send(encoded[index]);
  }


  public ParameterList copy() {
    SimpleParameterList newCopy = new SimpleParameterList(paramValues.length, transferModeRegistry);
    System.arraycopy(paramValues, 0, newCopy.paramValues, 0, paramValues.length);
    System.arraycopy(paramTypes, 0, newCopy.paramTypes, 0, paramTypes.length);
    System.arraycopy(flags, 0, newCopy.flags, 0, flags.length);
    newCopy.pos = pos;
    return newCopy;
  }

  public void clear() {
    Arrays.fill(paramValues, null);
    Arrays.fill(paramTypes, 0);
    Arrays.fill(encoded, null);
    Arrays.fill(flags, (byte) 0);
    pos = 0;
  }

  public SimpleParameterList[] getSubparams() {
    return null;
  }

  public Object[] getValues() {
    return paramValues;
  }

  public int[] getParamTypes() {
    return paramTypes;
  }

  public byte[] getFlags() {
    return flags;
  }

  public byte[][] getEncoding() {
    return encoded;
  }

  @Override
  public void appendAll(ParameterList list) throws SQLException {
    if (list instanceof org.postgresql.core.v3.SimpleParameterList ) {
      /* only v3.SimpleParameterList is compatible with this type
      we need to create copies of our parameters, otherwise the values can be changed */
      SimpleParameterList spl = (SimpleParameterList) list;
      int inParamCount = spl.getInParameterCount();
      if ((pos + inParamCount) > paramValues.length) {
        throw new PSQLException(
          GT.tr("Added parameters index out of range: {0}, number of columns: {1}.",
              (pos + inParamCount), paramValues.length),
              PSQLState.INVALID_PARAMETER_VALUE);
      }
      System.arraycopy(spl.getValues(), 0, this.paramValues, pos, inParamCount);
      System.arraycopy(spl.getParamTypes(), 0, this.paramTypes, pos, inParamCount);
      System.arraycopy(spl.getFlags(), 0, this.flags, pos, inParamCount);
      System.arraycopy(spl.getEncoding(), 0, this.encoded, pos, inParamCount);
      pos += inParamCount;
    }
  }

  /**
   * Useful implementation of toString.
   * @return String representation of the list values
   */
  @Override
  public String toString() {
    StringBuilder ts = new StringBuilder("<[");
    if (paramValues.length > 0) {
      ts.append(toString(1, true));
      for (int c = 2; c <= paramValues.length; c++) {
        ts.append(" ,").append(toString(c, true));
      }
    }
    ts.append("]>");
    return ts.toString();
  }

  private final Object[] paramValues;
  private final int[] paramTypes;
  private final byte[] flags;
  private final byte[][] encoded;
  private final TypeTransferModeRegistry transferModeRegistry;

  /**
   * Marker object representing NULL; this distinguishes "parameter never set" from "parameter set
   * to null".
   */
  private final static Object NULL_OBJECT = new Object();

  private int pos = 0;
}

