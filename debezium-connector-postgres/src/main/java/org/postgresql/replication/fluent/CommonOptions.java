package org.postgresql.replication.fluent;

import org.postgresql.replication.LogSequenceNumber;

/**
 * Common parameters for logical and physical replication
 */
public interface CommonOptions {
  /**
   * Replication slots provide an automated way to ensure that the master does not remove WAL
   * segments until they have been received by all standbys, and that the master does not remove
   * rows which could cause a recovery conflict even when the standby is disconnected.
   *
   * @return nullable replication slot name that already exists on server and free.
   */
  String getSlotName();

  /**
   * @return not null position from which need start replicate changes
   */
  LogSequenceNumber getStartLSNPosition();

  /**
   * Specifies the number of millisecond between status packets sent back to the server. This allows
   * for easier monitoring of the progress from server. A value of zero disables the periodic status
   * updates completely, although an update will still be sent when requested by the server, to
   * avoid timeout disconnect. The default value is 10 seconds.
   */
  int getStatusInterval();
}
