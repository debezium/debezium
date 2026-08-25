/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Unit tests for {@link CommonEventMeter}.
 */
public class CommonEventMeterTest {

    private final EventMetadataProvider metadataProvider = new EventMetadataProvider() {
        @Override
        public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return Instant.now();
        }

        @Override
        public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return Map.of("pos", "1");
        }

        @Override
        public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return "tx-1";
        }
    };

    private CommonEventMeter meter;
    private static final TableId TABLE_ID = TableId.parse("db.schema.table");
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

    @BeforeEach
    public void setUp() {
        meter = new CommonEventMeter(Clock.system(), metadataProvider);
    }

    @Test
    public void shouldReportZeroForAllMetricsWhenNewlyCreated() {
        assertThat(meter.getTotalNumberOfEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfCreateEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfUpdateEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfDeleteEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfReadEventsSeen()).isEqualTo(0);
        assertThat(meter.getNumberOfEventsFiltered()).isEqualTo(0);
        assertThat(meter.getNumberOfErroneousEvents()).isEqualTo(0);
        assertThat(meter.getLastEvent()).isNull();
        assertThat(meter.getMilliSecondsSinceLastEvent()).isEqualTo(-1);
    }

    @Test
    @FixFor("debezium/dbz#2167")
    public void shouldCountReadEventsSeparatelyFromOtherOperations() {
        final Struct value = new Struct(VALUE_SCHEMA).put("id", 1);

        meter.onEvent(TABLE_ID, null, 1L, value, Operation.CREATE);
        meter.onEvent(TABLE_ID, null, 2L, value, Operation.READ);
        meter.onEvent(TABLE_ID, null, 3L, value, Operation.READ);
        meter.onEvent(TABLE_ID, null, 4L, value, Operation.UPDATE);
        meter.onEvent(TABLE_ID, null, 5L, value, Operation.DELETE);

        assertThat(meter.getTotalNumberOfEventsSeen()).isEqualTo(5);
        assertThat(meter.getTotalNumberOfCreateEventsSeen()).isEqualTo(1);
        assertThat(meter.getTotalNumberOfReadEventsSeen()).isEqualTo(2);
        assertThat(meter.getTotalNumberOfUpdateEventsSeen()).isEqualTo(1);
        assertThat(meter.getTotalNumberOfDeleteEventsSeen()).isEqualTo(1);
        assertThat(meter.getLastEvent()).isNotNull();
        assertThat(meter.getMilliSecondsSinceLastEvent()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2167")
    public void shouldCountReadEventsWhenFilteredOrErroneous() {
        meter.onFilteredEvent(Operation.READ);
        meter.onErroneousEvent(Operation.READ);

        assertThat(meter.getTotalNumberOfEventsSeen()).isEqualTo(2);
        assertThat(meter.getTotalNumberOfReadEventsSeen()).isEqualTo(2);
        assertThat(meter.getNumberOfEventsFiltered()).isEqualTo(1);
        assertThat(meter.getNumberOfErroneousEvents()).isEqualTo(1);
    }

    @Test
    @FixFor("debezium/dbz#2167")
    public void shouldResetReadEventCountWhenResetIsCalled() {
        final Struct value = new Struct(VALUE_SCHEMA).put("id", 1);

        meter.onEvent(TABLE_ID, null, 1L, value, Operation.READ);
        meter.onEvent(TABLE_ID, null, 2L, value, Operation.CREATE);
        meter.onFilteredEvent(Operation.READ);
        meter.onErroneousEvent(Operation.READ);

        meter.reset();

        assertThat(meter.getTotalNumberOfEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfCreateEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfUpdateEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfDeleteEventsSeen()).isEqualTo(0);
        assertThat(meter.getTotalNumberOfReadEventsSeen()).isEqualTo(0);
        assertThat(meter.getNumberOfEventsFiltered()).isEqualTo(0);
        assertThat(meter.getNumberOfErroneousEvents()).isEqualTo(0);
        assertThat(meter.getLastEvent()).isNull();
        assertThat(meter.getMilliSecondsSinceLastEvent()).isEqualTo(-1);
    }
}
