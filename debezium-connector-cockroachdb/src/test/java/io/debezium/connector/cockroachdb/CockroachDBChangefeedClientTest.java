/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.pipeline.source.spi.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.EventDispatcher;

/**
 * Unit test for {@link CockroachDBChangefeedClient}, using a mock Kafka consumer
 * and verifying that records are parsed and dispatched correctly.
 *
 * @author Virag Tripathi
 */
@SuppressWarnings("unchecked")
public class CockroachDBChangefeedClientTest {

    private static final String TEST_TOPIC = "roachfeed.test";
    private static final String JSON_RECORD = """
            {
              "key": {"id": 1},
              "after": {"id": 1, "name": "Alice"},
              "updated": "2025-06-18T18:00:00Z",
              "source": {
                "cluster": "dev",
                "db": "defaultdb",
                "table": "users",
                "ts_hlc": "486481203757654017",
                "ts_ns": 1718704800000000000
              }
            }
            """;

    private CockroachDBConnectorConfig connectorConfig;
    private CockroachDBChangefeedClient changefeedClient;
    private EventDispatcher<CockroachDBPartition, ?> dispatcher;
    private ChangeEventSourceContext context;
    private Consumer<String, String> consumer;

    @BeforeEach
    public void setup() {
        connectorConfig = new CockroachDBConnectorConfig(
                Configuration.create()
                        .with("name", "test-connector")
                        .with("kafka.bootstrap.servers", "localhost:9092")
                        .with("changefeed.topic", TEST_TOPIC)
                        .with("changefeed.poll.interval.ms", 100)
                        .build());

        dispatcher = mock(EventDispatcher.class);
        context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true).thenReturn(false);

        consumer = mock(Consumer.class);
        changefeedClient = new CockroachDBChangefeedClient(connectorConfig) {
            @Override
            protected Consumer<String, String> createKafkaConsumer() {
                return consumer;
            }
        };
    }

    @Test
    public void shouldParseKafkaRecordAndDispatch() throws InterruptedException {
        CockroachDBPartition partition = new CockroachDBPartition("test-connector");
        CockroachDBOffsetContext offset = new CockroachDBOffsetContext(connectorConfig);

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                TEST_TOPIC, 0, 0L, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0L, 0, 0,
                "1", JSON_RECORD);

        TopicPartition tp = new TopicPartition(TEST_TOPIC, 0);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(Map.of(tp, Collections.singletonList(record)));

        when(consumer.poll(any(Duration.class))).thenReturn(records);

        changefeedClient.execute(context, partition, offset, dispatcher);

        verify(dispatcher, atLeastOnce()).dispatchDataChangeEvent(eq(partition), any(SourceRecord.class));
    }
}
