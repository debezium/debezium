/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

public class ActivityMonitoringMeterTest {

    ActivityMonitoringMeter activityMonitoringMeter;

    @Test
    public void whenNoEventIsReceivedAndNewCreatedMeterThenNoMetricsMustBeReturned() {

        activityMonitoringMeter = new ActivityMonitoringMeter();

        assertThat(activityMonitoringMeter.getNumberOfCreateEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfUpdateEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfDeleteEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfTruncateEventsSeen()).isEmpty();

    }

    @Test
    public void whenInsertEventIsReceivedThenCorrectMetricsMustBeReturned() {

        activityMonitoringMeter = new ActivityMonitoringMeter();

        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.UPDATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.DELETE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.anotherTable"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.anotherTable"), null, 1L, null, Envelope.Operation.TRUNCATE);

        assertThat(activityMonitoringMeter.getNumberOfCreateEventsSeen())
                .contains(Map.entry("db.schema.table", 2L), Map.entry("db.schema.anotherTable", 1L));
        assertThat(activityMonitoringMeter.getNumberOfUpdateEventsSeen())
                .contains(Map.entry("db.schema.table", 1L));
        assertThat(activityMonitoringMeter.getNumberOfDeleteEventsSeen())
                .contains(Map.entry("db.schema.table", 1L));
        assertThat(activityMonitoringMeter.getNumberOfTruncateEventsSeen())
                .contains(Map.entry("db.schema.anotherTable", 1L));

    }

    @Test
    public void whenMeterIsResetThenNoMetricsMustBeReturned() {

        activityMonitoringMeter = new ActivityMonitoringMeter();

        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.UPDATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.table"), null, 1L, null, Envelope.Operation.DELETE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.anotherTable"), null, 1L, null, Envelope.Operation.CREATE);
        activityMonitoringMeter.onEvent(TableId.parse("db.schema.anotherTable"), null, 1L, null, Envelope.Operation.TRUNCATE);

        activityMonitoringMeter.reset();

        assertThat(activityMonitoringMeter.getNumberOfCreateEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfUpdateEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfDeleteEventsSeen()).isEmpty();
        assertThat(activityMonitoringMeter.getNumberOfTruncateEventsSeen()).isEmpty();
    }
}
