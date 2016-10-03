package org.postgresql.replication;


import org.postgresql.replication.fluent.CommonOptions;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Not tread safe replication stream. After complete streaming should be close, for free resource on
 * backend. Periodical status update work only when use {@link PGReplicationStream#read()} method.
 * It means that process wal record should be fast as possible, because during process wal record
 * lead to disconnect by timeout from server.
 */
public interface PGReplicationStream {

  /**
   * <p>Read next wal record from backend. It method can be block until new message will not get
   * from server.
   *
   * <p>A single WAL record is never split across two XLogData messages. When a WAL record crosses a
   * WAL page boundary, and is therefore already split using continuation records, it can be split
   * at the page boundary. In other words, the first main WAL record and its continuation records
   * can be sent in different XLogData messages.
   *
   * @return not null byte array received by replication protocol, return ByteBuffer wrap around
   * received byte array with use offset, so, use {@link ByteBuffer#array()} carefully
   * @throws SQLException when some internal exception occurs during read from stream
   */
  ByteBuffer read() throws SQLException;

  /**
   * <p>Read next wal record from backend. It method can't be block and in contrast to {@link
   * PGReplicationStream#read()}. If message from backend absent return null. It allow periodically
   * check message in stream and if they absent sleep some time, but it time should be less than
   * {@link CommonOptions#getStatusInterval()} to avoid disconnect from the server.
   *
   * <p>A single WAL record is never split across two XLogData messages. When a WAL record crosses a
   * WAL page boundary, and is therefore already split using continuation records, it can be split
   * at the page boundary. In other words, the first main WAL record and its continuation records
   * can be sent in different XLogData messages.
   *
   * @return byte array received by replication protocol or null if pending message from server
   * absent. Returns ByteBuffer wrap around received byte array with use offset, so, use {@link
   * ByteBuffer#array()} carefully.
   * @throws SQLException when some internal exception occurs during read from stream
   */
  ByteBuffer readPending() throws SQLException;

  /**
   * Parameter updates by execute {@link PGReplicationStream#read()} method.
   *
   * @return not null LSN position that was receive last time via {@link PGReplicationStream#read()}
   * method
   */
  LogSequenceNumber getLastReceiveLSN();

  /**
   * Last flushed lsn send in update message to backend. Parameter updates only via {@link
   * PGReplicationStream#setFlushedLSN(LogSequenceNumber)}
   *
   * @return not null location of the last WAL flushed to disk in the standby.
   */
  LogSequenceNumber getLastFlushedLSN();

  /**
   * Last applied lsn send in update message to backed. Parameter updates only via {@link
   * PGReplicationStream#setAppliedLSN(LogSequenceNumber)}
   *
   * @return not null location of the last WAL applied in the standby.
   */
  LogSequenceNumber getLastAppliedLSN();

  /**
   * Set flushed LSN. It parameter will be send to backend on next update status iteration. Flushed
   * LSN position help backend define which wal can be recycle.
   *
   * @param flushed not null location of the last WAL flushed to disk in the standby.
   * @see PGReplicationStream#forceUpdateStatus()
   */
  void setFlushedLSN(LogSequenceNumber flushed);

  /**
   * Parameter used only physical replication and define which lsn already was apply on standby.
   * Feedback will send to backend on next update status iteration.
   *
   * @param applied not null location of the last WAL applied in the standby.
   * @see PGReplicationStream#forceUpdateStatus()
   */
  void setAppliedLSN(LogSequenceNumber applied);

  /**
   * Force send to backend status about last received, flushed and applied LSN. You can not use it
   * method explicit, because {@link PGReplicationStream} send status to backend periodical by
   * configured interval via {@link LogicalReplicationOptions#getStatusInterval}
   *
   * @see LogicalReplicationOptions#getStatusInterval()
   * @throws SQLException when some internal exception occurs during read from stream
   */
  void forceUpdateStatus() throws SQLException;

  /**
   * @return {@code true} if replication stream was already close, otherwise return {@code false}
   */
  boolean isClosed();

  /**
   * <p>Stop replication changes from server and free resources. After that connection can be reuse
   * to another queries. Also after close current stream they cannot be used anymore.
   *
   * <p><b>Note:</b> This method can spend much time for logical replication stream on postgresql
   * version 9.6 and lower, because postgresql have bug - during decode big transaction to logical
   * form and during wait new changes postgresql ignore messages from client. As workaround you can
   * close replication connection instead of close replication stream. For more information about it
   * problem see mailing list thread <a href="http://www.postgresql.org/message-id/CAFgjRd3hdYOa33m69TbeOfNNer2BZbwa8FFjt2V5VFzTBvUU3w@mail.gmail.com">
   * Stopping logical replication protocol</a>
   *
   * @throws SQLException when some internal exception occurs during end streaming
   */
  void close() throws SQLException;
}
