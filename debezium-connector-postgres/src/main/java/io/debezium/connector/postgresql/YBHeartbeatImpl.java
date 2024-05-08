package io.debezium.connector.postgresql;

import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;
import java.util.Map;

/**
 * YugabyteDB specific heartbeat implementation to only allow the forcedHeartbeat method which
 * will be called in the transition phase when we are waiting for transitioning from snapshot to
 * streaming.
 */
public class YBHeartbeatImpl extends HeartbeatImpl {
  public YBHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, SchemaNameAdjuster schemaNameAdjuster) {
    super(heartbeatInterval, topicName, key, schemaNameAdjuster);
  }

  @Override
  public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
  }

  @Override
  public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
  }

  @Override
  public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
    super.forcedBeat(partition, offset, consumer);
  }
}
