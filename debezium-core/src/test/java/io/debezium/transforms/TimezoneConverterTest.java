/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

public class TimezoneConverterTest {
    private final TimezoneConverter<SourceRecord> converter = new TimezoneConverter();
    protected final Schema sourceSchema = SchemaBuilder.struct()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    @Test
    public void testMultipleDebeziumTimestamps() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+05:30");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("order_date_micros", MicroTimestamp.builder().optional().build())
                .field("order_date_nanos", NanoTimestamp.builder().optional().build())
                .field("order_date_timestamp", Timestamp.builder().optional().build())
                .field("order_date_zoned_timestamp", ZonedTimestamp.builder().optional().build())
                .field("order_date_zoned_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "Srikanth");
        before.put("order_date_micros", 1529507596945104L);
        before.put("order_date_nanos", 1531481025340000000L);
        before.put("order_date_timestamp", 1514908810123L);
        before.put("order_date_zoned_timestamp", "2018-01-02T11:15:30.123456789+00:00");
        before.put("order_date_zoned_time", "11:15:30.123456789+00:00");

        source.put("table", "orders");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("order_date_micros")).isEqualTo(1529487796945000L);
        assertThat(transformedAfter.get("order_date_nanos")).isEqualTo(1531461225340000000L);
        assertThat(transformedAfter.get("order_date_timestamp")).isEqualTo(1514889010123L);
        assertThat(transformedAfter.get("order_date_zoned_timestamp")).isEqualTo("2018-01-02T16:45:30.123456789+05:30");
        assertThat(transformedAfter.get("order_date_zoned_time")).isEqualTo("16:45:30.123456789+05:30");
    }

    @Test
    public void testSingleDebeziumTimestamp() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Pacific/Easter");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("updated_at", ZonedTimestamp.builder().optional().build())
                .field("order_date", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "Srikanth");
        before.put("created_at", "2011-01-11T16:40:30.123456789+00:00");
        before.put("updated_at", "2011-02-02T11:04:30.123456789+00:00");
        before.put("order_date", "2011-04-09T13:00:30.123456789+00:00");

        source.put("table", "orders");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("2011-01-11T11:40:30.123456789-05:00");
        assertThat(transformedAfter.get("updated_at")).isEqualTo("2011-02-02T06:04:30.123456789-05:00");
        assertThat(transformedAfter.get("order_date")).isEqualTo("2011-04-09T08:00:30.123456789-05:00");
    }

    @Test
    public void testKafkaConnectTimestamp() {
        Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Africa/Cairo");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", org.apache.kafka.connect.data.Timestamp.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "Pierre Wright");
        before.put("created_at", Date.from(LocalDateTime.of(2018, 3, 27, 2, 0, 0).toInstant(ZoneOffset.UTC)));

        source.put("table", "orders");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isInstanceOf(Date.class);
        assertThat(transformedAfter.get("created_at")).isEqualTo(Date.from(LocalDateTime.of(2018, 3, 27, 0, 0, 0).toInstant(ZoneOffset.UTC)));
    }

    @Test
    public void testIncludeListWithTablePrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Atlantic/Azores");
        props.put("include.list", "source:customers:order_time,customers:created_at");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("updated_at", ZonedTimestamp.builder().optional().build())
                .field("order_time", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "2020-01-01T11:55:30+00:00");
        before.put("updated_at", "2020-05-01T11:55:30+00:00");
        before.put("order_time", "2020-06-20T11:55:30+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("2020-01-01T10:55:30-01:00");
        assertThat(transformedAfter.get("updated_at")).isEqualTo("2020-05-01T11:55:30+00:00");
        assertThat(transformedAfter.get("order_time")).isEqualTo("2020-06-20T11:55:30Z");
    }

    @Test
    public void testIncludeListWithTopicPrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+05:30");
        props.put("include.list", "topic:db.server1.table1:order_time");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "11:55:30+00:00");
        before.put("order_time", "12:10:10+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("11:55:30+00:00");
        assertThat(transformedAfter.get("order_time")).isEqualTo("17:40:10+05:30");
    }

    @Test
    public void testIncludeListWithNoPrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Asia/Kolkata");
        props.put("include.list", "db.server1.table1:order_time");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "11:55:30+00:00");
        before.put("order_time", "12:10:10+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("order_time")).isEqualTo("17:40:10+05:30");
        assertThat(transformedAfter.get("created_at")).isEqualTo("11:55:30+00:00");

    }

    @Test
    public void testExcludeListWithTablePrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+05:30");
        props.put("exclude.list", "source:customers:order_time");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTime.builder().optional().build())
                .field("updated_at", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "11:55:30+00:00");
        before.put("updated_at", "12:10:10+00:00");
        before.put("order_time", "15:40:10+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("17:25:30+05:30");
        assertThat(transformedAfter.get("updated_at")).isEqualTo("17:40:10+05:30");
        assertThat(transformedAfter.get("order_time")).isEqualTo("15:40:10+00:00");
    }

    @Test
    public void testExcludeListWithTopicPrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "America/Chicago");
        props.put("exclude.list", "topic:db.server1.table1:order_time");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "11:55:30+00:00");
        before.put("order_time", "15:40:10+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("06:55:30-05:00");
        assertThat(transformedAfter.get("order_time")).isEqualTo("15:40:10+00:00");
    }

    @Test
    public void testExcludeListWithNoPrefix() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+08:00");
        props.put("exclude.list", "db.server1.table1:order_time");
        converter.configure(props);

        Schema recordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "John Doe");
        before.put("created_at", "11:55:30+00:00");
        before.put("order_time", "15:40:10+00:00");

        source.put("table", "customers");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct payload = envelope.create(before, source, Instant.now());

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.get("created_at")).isEqualTo("19:55:30+08:00");
        assertThat(transformedAfter.get("order_time")).isEqualTo("15:40:10+00:00");
    }

    @Test
    public void testIncludeListMultipleTables() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+03:00");
        props.put("include.list", "source:customers1,orders1,public1");
        converter.configure(props);

        Schema customersRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("updated_at", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct customersBefore = new Struct(customersRecordSchema);
        final Struct customersSource = new Struct(sourceSchema);

        customersBefore.put("id", (byte) 1);
        customersBefore.put("name", "John Doe");
        customersBefore.put("created_at", "2020-01-01T11:55:30+00:00");
        customersBefore.put("updated_at", "2020-01-01T15:40:10+00:00");

        customersSource.put("table", "customers1");
        customersSource.put("lsn", 1);
        customersSource.put("ts_ms", 123456789);

        final Envelope customersEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(customersRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct customersPayload = customersEnvelope.create(customersBefore, customersSource, Instant.now());
        SourceRecord customersRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.customers",
                customersEnvelope.schema(),
                customersPayload);

        final SourceRecord transformedCustomersRecord = converter.apply(customersRecord);
        final Struct transformedCustomersValue = (Struct) transformedCustomersRecord.value();
        final Struct transformedCustomersAfter = transformedCustomersValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedCustomersAfter.get("created_at")).isEqualTo("2020-01-01T14:55:30+03:00");
        assertThat(transformedCustomersAfter.get("updated_at")).isEqualTo("2020-01-01T18:40:10+03:00");

        Schema ordersRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("customer_id", Schema.INT8_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("order_time", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct ordersBefore = new Struct(ordersRecordSchema);
        final Struct ordersSource = new Struct(sourceSchema);

        ordersBefore.put("id", (byte) 1);
        ordersBefore.put("customer_id", (byte) 1);
        ordersBefore.put("created_at", "2023-08-01T11:50:45+00:00");
        ordersBefore.put("order_time", "2023-09-01T11:55:30+00:00");

        ordersSource.put("table", "orders1");
        ordersSource.put("lsn", 1);
        ordersSource.put("ts_ms", 123456789);

        final Envelope ordersEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(ordersRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct ordersPayload = ordersEnvelope.create(ordersBefore, ordersSource, Instant.now());
        SourceRecord ordersRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.orders",
                ordersEnvelope.schema(),
                ordersPayload);

        final SourceRecord transformedOrdersRecord = converter.apply(ordersRecord);
        final Struct transformedOrdersValue = (Struct) transformedOrdersRecord.value();
        final Struct transformedOrdersAfter = transformedOrdersValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedOrdersAfter.get("created_at")).isEqualTo("2023-08-01T14:50:45+03:00");
        assertThat(transformedOrdersAfter.get("order_time")).isEqualTo("2023-09-01T14:55:30+03:00");
    }

    @Test
    public void testExcludeListMultipleTables() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "-06:00");
        props.put("exclude.list", "topic:db.server1.customers,db.server1.public");
        converter.configure(props);

        Schema customersRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("updated_at", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct customersBefore = new Struct(customersRecordSchema);
        final Struct customersSource = new Struct(sourceSchema);

        customersBefore.put("id", (byte) 1);
        customersBefore.put("name", "John Doe");
        customersBefore.put("created_at", "2020-01-01T11:55:30+00:00");
        customersBefore.put("updated_at", "2020-01-01T15:40:10+00:00");

        customersSource.put("table", "customers1");
        customersSource.put("lsn", 1);
        customersSource.put("ts_ms", 123456789);

        final Envelope customersEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(customersRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct customersPayload = customersEnvelope.create(customersBefore, customersSource, Instant.now());
        SourceRecord customersRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.customers",
                customersEnvelope.schema(),
                customersPayload);

        final SourceRecord transformedCustomersRecord = converter.apply(customersRecord);
        final Struct transformedCustomersValue = (Struct) transformedCustomersRecord.value();
        final Struct transformedCustomersAfter = transformedCustomersValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedCustomersAfter.get("created_at")).isEqualTo("2020-01-01T11:55:30+00:00");
        assertThat(transformedCustomersAfter.get("updated_at")).isEqualTo("2020-01-01T15:40:10+00:00");

        Schema ordersRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("customer_id", Schema.INT8_SCHEMA)
                .field("created_at", ZonedTimestamp.builder().optional().build())
                .field("order_time", ZonedTimestamp.builder().optional().build())
                .build();

        final Struct ordersBefore = new Struct(ordersRecordSchema);
        final Struct ordersSource = new Struct(sourceSchema);

        ordersBefore.put("id", (byte) 1);
        ordersBefore.put("customer_id", (byte) 1);
        ordersBefore.put("created_at", "2023-08-01T11:50:45+00:00");
        ordersBefore.put("order_time", "2023-09-01T11:55:30+00:00");

        ordersSource.put("table", "orders1");
        ordersSource.put("lsn", 1);
        ordersSource.put("ts_ms", 123456789);

        final Envelope ordersEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(ordersRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct ordersPayload = ordersEnvelope.create(ordersBefore, ordersSource, Instant.now());
        SourceRecord ordersRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.orders",
                ordersEnvelope.schema(),
                ordersPayload);

        final SourceRecord transformedOrdersRecord = converter.apply(ordersRecord);
        final Struct transformedOrdersValue = (Struct) transformedOrdersRecord.value();
        final Struct transformedOrdersAfter = transformedOrdersValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedOrdersAfter.get("created_at")).isEqualTo("2023-08-01T05:50:45-06:00");
        assertThat(transformedOrdersAfter.get("order_time")).isEqualTo("2023-09-01T05:55:30-06:00");
    }

    @Test
    public void testBothIncludeExcludeList() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "+05:30");
        props.put("include.list", "topic:db.server1.customers,db.server1.public");
        props.put("exclude.list", "topic:db.server1.customers,db.server1.public");

        assertThat(catchThrowable(() -> converter.configure(props))).isInstanceOf(DebeziumException.class);
        assertThat(catchThrowable(() -> converter.configure(props))).hasMessageContaining("Both include and exclude lists are specified. Please specify only one.");
    }

    @Test
    public void testWithNoConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put("include.list", "topic:db.server1.customers");

        assertThat(catchThrowable(() -> converter.configure(props))).isInstanceOf(ConfigException.class);
        assertThat(catchThrowable(() -> converter.configure(props)))
                .hasMessageContaining("Invalid value null for configuration converted.timezone: The 'converted.timezone' value is invalid: A value is required");
    }

    @Test
    public void testWithInvalidConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Asia");
        props.put("include.list", "topic:db.server1.customers");

        assertThat(catchThrowable(() -> converter.configure(props))).isInstanceOf(DebeziumException.class);
        assertThat(catchThrowable(() -> converter.configure(props)))
                .hasMessageContaining(
                        "Invalid timezone format. Please specify either a geographic timezone (e.g. America/Los_Angeles) or a UTC offset in the format of +/-hh:mm, (e.g. +08:00).");
    }

    @Test
    public void testExcludeListWithMultipleFields() {
        Map<String, String> props = new HashMap<>();
        props.put("converted.timezone", "Europe/Moscow");
        props.put("exclude.list", "topic:db.server1.customers:order_time,db.server1.inventory:order_time");
        converter.configure(props);

        Schema customersRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct customersBefore = new Struct(customersRecordSchema);
        customersBefore.put("id", (byte) 1);
        customersBefore.put("name", "Amy Rose");
        customersBefore.put("order_time", "10:19:25+00:00");

        final Struct customersSource = new Struct(sourceSchema);
        customersSource.put("table", "customers");
        customersSource.put("lsn", 1);
        customersSource.put("ts_ms", 123456789);

        final Envelope customersEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(customersRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct customersPayload = customersEnvelope.create(customersBefore, customersSource, Instant.now());
        SourceRecord customersRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.customers",
                customersEnvelope.schema(),
                customersPayload);

        final SourceRecord transformedCustomersRecord = converter.apply(customersRecord);
        final Struct transformedCustomersValue = (Struct) transformedCustomersRecord.value();
        final Struct transformedCustomersAfter = transformedCustomersValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedCustomersAfter.get("name")).isEqualTo("Amy Rose");
        assertThat(transformedCustomersAfter.get("order_time")).isEqualTo("10:19:25+00:00");

        Schema inventoryRecordSchema = SchemaBuilder.struct()
                .field("id", Schema.INT8_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("shipping_time", ZonedTime.builder().optional().build())
                .field("order_time", ZonedTime.builder().optional().build())
                .build();

        final Struct inventoryBefore = new Struct(inventoryRecordSchema);
        inventoryBefore.put("id", (byte) 1);
        inventoryBefore.put("name", "Amy Rose");
        inventoryBefore.put("shipping_time", "19:19:25+00:00");
        inventoryBefore.put("order_time", "10:19:25+00:00");

        final Struct inventorySource = new Struct(sourceSchema);
        inventorySource.put("table", "inventory");
        inventorySource.put("lsn", 1);
        inventorySource.put("ts_ms", 123456789);

        final Envelope inventoryEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(inventoryRecordSchema)
                .withSource(sourceSchema)
                .build();

        final Struct inventoryPayload = inventoryEnvelope.create(inventoryBefore, inventorySource, Instant.now());
        SourceRecord inventoryRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.inventory",
                inventoryEnvelope.schema(),
                inventoryPayload);

        final SourceRecord transformedInventoryRecord = converter.apply(inventoryRecord);
        final Struct transformedInventoryValue = (Struct) transformedInventoryRecord.value();
        final Struct transformedInventoryAfter = transformedInventoryValue.getStruct(Envelope.FieldName.AFTER);
        assertThat(transformedInventoryAfter.get("name")).isEqualTo("Amy Rose");
        assertThat(transformedInventoryAfter.get("shipping_time")).isEqualTo("22:19:25+03:00");
        assertThat(transformedInventoryAfter.get("order_time")).isEqualTo("10:19:25+00:00");

    }
}
