package org.postgresql.replication;


import java.nio.ByteBuffer;

/**
 * LSN (Log Sequence Number) data which is a pointer to a location in the XLOG
 */
public final class LogSequenceNumber {
  /**
   * Zero is used indicate an invalid pointer. Bootstrap skips the first possible WAL segment,
   * initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG record can begin at zero.
   */
  public static final LogSequenceNumber INVALID_LSN = LogSequenceNumber.valueOf(0);

  private final long value;

  private LogSequenceNumber(long value) {
    this.value = value;
  }

  /**
   * @param value numeric represent position in the write-ahead log stream
   * @return not null LSN instance
   */
  public static LogSequenceNumber valueOf(long value) {
    return new LogSequenceNumber(value);
  }

  /**
   * Create LSN instance by string represent LSN
   *
   * @param strValue not null string as two hexadecimal numbers of up to 8 digits each, separated by
   *                 a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
   * @return not null LSN instance where if specified string represent have not valid form {@link
   * LogSequenceNumber#INVALID_LSN}
   */
  public static LogSequenceNumber valueOf(String strValue) {
    int slashIndex = strValue.lastIndexOf('/');

    if (slashIndex <= 0) {
      return INVALID_LSN;
    }

    String logicalXLogStr = strValue.substring(0, slashIndex);
    int logicalXlog = (int) Long.parseLong(logicalXLogStr, 16);
    String segmentStr = strValue.substring(slashIndex + 1, strValue.length());
    int segment = (int) Long.parseLong(segmentStr, 16);

    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putInt(logicalXlog);
    buf.putInt(segment);
    buf.position(0);
    long value = buf.getLong();

    return LogSequenceNumber.valueOf(value);
  }

  /**
   * @return Long represent position in the write-ahead log stream
   */
  public long asLong() {
    return value;
  }

  /**
   * @return String represent position in the write-ahead log stream as two hexadecimal numbers of
   * up to 8 digits each, separated by a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
   */
  public String asString() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(value);
    buf.position(0);

    int logicalXlog = buf.getInt();
    int segment = buf.getInt();
    return String.format("%X/%X", logicalXlog, segment);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogSequenceNumber that = (LogSequenceNumber) o;

    return value == that.value;

  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return "LSN{" + asString() + '}';
  }
}
