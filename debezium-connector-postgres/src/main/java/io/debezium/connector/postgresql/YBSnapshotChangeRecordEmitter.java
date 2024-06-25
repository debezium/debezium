package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.ReplicaIdentityInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Clock;

/**
 * Custom snapshot change record emitter for YugabyteDB which forms column values object based on
 * the replica identity type
 * @param <P> instance of {@link io.debezium.pipeline.spi.Partition}
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBSnapshotChangeRecordEmitter<P extends PostgresPartition> extends RelationalChangeRecordEmitter<P> {
  private final Object[] row;
  private final PostgresConnectorConfig connectorConfig;

  public YBSnapshotChangeRecordEmitter(P partition, OffsetContext offset, Object[] row, Clock clock,
                                       PostgresConnectorConfig connectorConfig) {
    super(partition, offset, clock, connectorConfig);

    this.row = row;
    this.connectorConfig = connectorConfig;
  }

  @Override
  public Envelope.Operation getOperation() {
    return Envelope.Operation.READ;
  }

  @Override
  protected Object[] getOldColumnValues() {
    throw new UnsupportedOperationException("Can't get old row values for READ record");
  }

  @Override
  protected Object[] getNewColumnValues() {
    Object[] values = new Object[row.length];

    for (int position = 0; position < values.length; ++position) {
      if (connectorConfig.plugin().isYBOutput()) {
        values[position] = new Object[]{row[position], Boolean.TRUE};
      } else {
        values[position] = row[position];
      }
    }

    return values;
  }
}
