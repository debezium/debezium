package org.postgresql.core.v3;

public interface TypeTransferModeRegistry {
  /**
   * Returns if given oid should be sent in binary format
   * @param oid type oid
   * @return true if given oid should be sent in binary format
   */
  boolean useBinaryForSend(int oid);

  /**
   * Returns if given oid should be received in binary format
   * @param oid type oid
   * @return true if given oid should be received in binary format
   */
  boolean useBinaryForReceive(int oid);
}
