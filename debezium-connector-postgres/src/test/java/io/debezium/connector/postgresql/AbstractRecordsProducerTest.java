/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;

import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.VariableLatch;

/**
 * Base class for the integration tests for the different {@link RecordsProducer} instances
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public abstract class AbstractRecordsProducerTest {
    
    protected static final Pattern INSERT_TABLE_MATCHING_PATTERN = Pattern.compile("insert into (\\w+).+", Pattern.CASE_INSENSITIVE);
    
    protected static final String INSERT_CASH_TYPES_STMT = "INSERT INTO cash_table (csh) VALUES ('$1234.11')";
    protected static final String INSERT_DATE_TIME_TYPES_STMT = "INSERT INTO time_table(ts, tz, date, ti, ttz, it) " +
                                                                "VALUES ('2016-11-04T13:51:30'::TIMESTAMP, '2016-11-04T13:51:30+02:00'::TIMESTAMPTZ, " +
                                                                "'2016-11-04'::DATE, '13:51:30'::TIME, '13:51:30+02:00'::TIMETZ, 'P1Y2M3DT4H5M0S'::INTERVAL)";
    protected static final String INSERT_BIN_TYPES_STMT = "INSERT INTO bitbin_table (ba, bol, bs, bv) " +
                                                          "VALUES (E'\\\\001\\\\002\\\\003'::bytea, '0'::bit(1), '11'::bit(2), '00'::bit(2))";
    protected static final String INSERT_GEOM_TYPES_STMT = "INSERT INTO geom_table(p) VALUES ('(1,1)'::point)";
    protected static final String INSERT_TEXT_TYPES_STMT = "INSERT INTO text_table(j, jb, x, u) " +
                                                           "VALUES ('{\"bar\": \"baz\"}'::json, '{\"bar\": \"baz\"}'::jsonb, " +
                                                           "'<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID)";
    protected static final String INSERT_STRING_TYPES_STMT = "INSERT INTO string_table (vc, vcv, ch, c, t) " +
                                                             "VALUES ('aa', 'bb', 'cdef', 'abc', 'some text')";
    protected static final String INSERT_NUMERIC_TYPES_STMT = "INSERT INTO numeric_table (si, i, bi, d, n, r, db, ss, bs, b) " +
                                                              "VALUES (1, 123456, 1234567890123, 1.1, 22.22, 3.3, 4.44, 1, 123, true)";
    
    protected static final Set<String> ALL_STMTS = new HashSet<>(Arrays.asList(INSERT_NUMERIC_TYPES_STMT, INSERT_DATE_TIME_TYPES_STMT,
                                                                 INSERT_BIN_TYPES_STMT, INSERT_GEOM_TYPES_STMT, INSERT_TEXT_TYPES_STMT,
                                                                 INSERT_CASH_TYPES_STMT, INSERT_STRING_TYPES_STMT));
    
    protected List<SchemaAndValueField> schemasAndValuesForNumericType() {
        return Arrays.asList(new SchemaAndValueField("si", SchemaBuilder.OPTIONAL_INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("i", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 123456),
                             new SchemaAndValueField("bi", SchemaBuilder.OPTIONAL_INT64_SCHEMA, 1234567890123L),
                             new SchemaAndValueField("d", Decimal.builder(1).optional().build(), BigDecimal.valueOf(1.1d)),
                             new SchemaAndValueField("n", Decimal.builder(2).optional().build(), BigDecimal.valueOf(22.22d)),
                             new SchemaAndValueField("r", Schema.OPTIONAL_FLOAT32_SCHEMA, 3.3f),
                             new SchemaAndValueField("db", Schema.OPTIONAL_FLOAT64_SCHEMA, 4.44d),
                             new SchemaAndValueField("ss", Schema.INT16_SCHEMA, (short) 1),
                             new SchemaAndValueField("bs", Schema.INT64_SCHEMA, 123L),
                             new SchemaAndValueField("b", Schema.OPTIONAL_BOOLEAN_SCHEMA, Boolean.TRUE));
    }
    
    protected List<SchemaAndValueField> schemasAndValuesForStringTypes() {
       return Arrays.asList(new SchemaAndValueField("vc", Schema.OPTIONAL_STRING_SCHEMA, "aa"),
                            new SchemaAndValueField("vcv", Schema.OPTIONAL_STRING_SCHEMA, "bb"),
                            new SchemaAndValueField("ch", Schema.OPTIONAL_STRING_SCHEMA, "cdef"),
                            new SchemaAndValueField("c", Schema.OPTIONAL_STRING_SCHEMA, "abc"),
                            new SchemaAndValueField("t", Schema.OPTIONAL_STRING_SCHEMA, "some text"));
    }
   
    protected List<SchemaAndValueField> schemasAndValuesForTextTypes() {
        return Arrays.asList(new SchemaAndValueField("j", Json.builder().optional().build(), "{\"bar\": \"baz\"}"),
                             new SchemaAndValueField("jb", Json.builder().optional().build(), "{\"bar\": \"baz\"}"),
                             new SchemaAndValueField("x", Xml.builder().optional().build(), "<foo>bar</foo><foo>bar</foo>"),
                             new SchemaAndValueField("u", Uuid.builder().optional().build(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
    }
    
    protected List<SchemaAndValueField> schemaAndValuesForGeomTypes() {
        Schema pointSchema = Point.builder().optional().build();
        return Collections.singletonList(new SchemaAndValueField("p", pointSchema, Point.createValue(pointSchema, 1, 1)));
    }
    
    protected List<SchemaAndValueField> schemaAndValuesForBinTypes() {
       return Arrays.asList(new SchemaAndValueField("ba", Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 1, 2, 3})),
                            new SchemaAndValueField("bol", Schema.OPTIONAL_BOOLEAN_SCHEMA, false),
                            new SchemaAndValueField("bs", Bits.builder(2).optional().build(), new byte[] { 3, 0 }),  // bitsets get converted from two's complement
                            new SchemaAndValueField("bv", Bits.builder(2).optional().build(), new byte[] { 0, 0 }));
    }
    
    protected List<SchemaAndValueField> schemaAndValuesForDateTimeTypes() {
        long expectedTs = NanoTimestamp.toEpochNanos(LocalDateTime.parse("2016-11-04T13:51:30"), null);
        String expectedTz = "2016-11-04T11:51:30Z"; //timestamp is stored with TZ, should be read back with UTC
        int expectedDate = Date.toEpochDay(LocalDate.parse("2016-11-04"), null);
        long expectedTi = LocalTime.parse("13:51:30").toNanoOfDay();
        String expectedTtz = "11:51:30Z";  //time is stored with TZ, should be read back at GMT
        double interval = MicroDuration.durationMicros(1, 2, 3, 4, 5, 0, PostgresValueConverter.DAYS_PER_MONTH_AVG);
    
        return Arrays.asList(new SchemaAndValueField("ts", NanoTimestamp.builder().optional().build(), expectedTs),
                             new SchemaAndValueField("tz", ZonedTimestamp.builder().optional().build(), expectedTz),
                             new SchemaAndValueField("date", Date.builder().optional().build(), expectedDate),
                             new SchemaAndValueField("ti", NanoTime.builder().optional().build(), expectedTi),
                             new SchemaAndValueField("ttz", ZonedTime.builder().optional().build(), expectedTtz),
                             new SchemaAndValueField("it", MicroDuration.builder().optional().build(), interval));
    }
    
    protected List<SchemaAndValueField> schemaAndValuesForMoneyTypes() {
        return Collections.singletonList(new SchemaAndValueField("csh", Decimal.builder(0).optional().build(),
                                                                 BigDecimal.valueOf(1234.11d)));
    }
    
    protected Map<String, List<SchemaAndValueField>> schemaAndValuesByTableName() {
        return ALL_STMTS.stream().collect(Collectors.toMap(AbstractRecordsProducerTest::tableNameFromInsertStmt,
                                                           this::schemasAndValuesForTable));
    }
    
    protected List<SchemaAndValueField> schemasAndValuesForTable(String insertTableStatement) {
        switch (insertTableStatement) {
            case INSERT_NUMERIC_TYPES_STMT:
                return schemasAndValuesForNumericType();
            case INSERT_BIN_TYPES_STMT:
                return schemaAndValuesForBinTypes();
            case INSERT_CASH_TYPES_STMT:
                return schemaAndValuesForMoneyTypes();
            case INSERT_DATE_TIME_TYPES_STMT:
                return schemaAndValuesForDateTimeTypes();
            case INSERT_GEOM_TYPES_STMT:
                return schemaAndValuesForGeomTypes();
            case INSERT_STRING_TYPES_STMT:
                return schemasAndValuesForStringTypes();
            case INSERT_TEXT_TYPES_STMT:
                return schemasAndValuesForTextTypes();
            default:
                throw new IllegalArgumentException("unknown statement:" + insertTableStatement);
        }
    }
    
    protected void assertRecordSchemaAndValues(List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                               SourceRecord record,
                                               String envelopeFieldName) {
        Struct content = ((Struct) record.value()).getStruct(envelopeFieldName);
        assertNotNull("expected there to be content in Envelope under " + envelopeFieldName, content);
        expectedSchemaAndValuesByColumn.forEach(schemaAndValueField -> schemaAndValueField.assertFor(content));
    }
    
    protected void assertRecordOffset(SourceRecord record, boolean expectSnapshot, boolean expectedLastSnapshotRecord) {
        Map<String, ?> offset = record.sourceOffset();
        assertNotNull(offset.get(SourceInfo.TXID_KEY));
        assertNotNull(offset.get(SourceInfo.TIMESTAMP_KEY));
        assertNotNull(offset.get(SourceInfo.LSN_KEY));
        Object snapshot = offset.get(SourceInfo.SNAPSHOT_KEY);
        Object lastSnapshotRecord = offset.get(SourceInfo.LAST_SNAPSHOT_RECORD_KEY);
        if (expectSnapshot) {
            Assert.assertTrue("Snapshot marker expected but not found", (Boolean) snapshot);
            assertEquals("Last snapshot record marker mismatch", expectedLastSnapshotRecord, lastSnapshotRecord);
        } else {
            assertNull("Snapshot marker not expected, but found", snapshot);
            assertNull("Last snapshot marker not expected, but found", lastSnapshotRecord);
        }
    }
    
    protected static String tableNameFromInsertStmt(String statement) {
        Matcher matcher = INSERT_TABLE_MATCHING_PATTERN.matcher(statement);
        assertTrue("invalid statement: " + statement, matcher.matches());
        return matcher.group(1);
    }
    
    protected static class SchemaAndValueField {
        private final Object schema;
        private final Object value;
        private final String fieldName;
    
        public SchemaAndValueField(String fieldName, Object schema, Object value) {
            this.schema = schema;
            this.value = value;
            this.fieldName = fieldName;
        }
        
        protected void assertFor(Struct content) {
            assertSchema(content);
            assertValue(content);
        }
        
        private void assertValue(Struct content) {
            if (value == null) {
                assertNull(fieldName + " is present in the actual content", content.get(fieldName));
                return;
            }
            Object actualValue = content.get(fieldName);
            assertNotNull("No value found for " + fieldName, actualValue);
            assertEquals("Incorrect value type for " + fieldName, value.getClass(), actualValue.getClass());
            if (actualValue instanceof byte[]) {
                assertArrayEquals("Values don't match for " + fieldName, (byte[]) value, (byte[]) actualValue);
            } else {
                assertEquals("Values don't match for " + fieldName, value, actualValue);
            }
        }
        
        private void assertSchema(Struct content) {
            if (schema == null) {
                return;
            }
            Schema schema = content.schema();
            Field field = schema.field(fieldName);
            assertNotNull(fieldName + " not found in schema " + schema, field);
            assertEquals("Schema for " + field + " does not match the actual value", this.schema, field.schema());
        }
    }
    
    protected TestConsumer testConsumer(int expectedRecordsCount) {
         return new TestConsumer(expectedRecordsCount);
    }
    
    protected static class TestConsumer implements Consumer<SourceRecord> {
        private ConcurrentLinkedQueue<SourceRecord> records;
        private VariableLatch latch;
    
        protected TestConsumer(int expectedRecordsCount) {
            this.latch = new VariableLatch(expectedRecordsCount);
            this.records = new ConcurrentLinkedQueue<>();
        }
    
        @Override
        public void accept(SourceRecord record) {
            if (latch.getCount() == 0) {
                fail("received more events than expected");
            }
            records.add(record);
            latch.countDown();
        }
        
        protected void expects(int expectedRecordsCount) {
            assert latch.getCount() == 0;
            this.latch.countUp(expectedRecordsCount);
        }
        
        protected SourceRecord remove() {
            return records.remove();
        }
    
        protected boolean isEmpty() {
            return records.isEmpty();
        }
       
        protected void process(Consumer<SourceRecord> consumer) {
            records.forEach(consumer);
        }
        
        protected void clear() {
            records.clear();
        }
   
        protected void await(long timeout, TimeUnit unit) throws InterruptedException {
            if (!latch.await(timeout, unit)) {
                fail("Consumer expected " + latch.getCount() + " records, but received " + records.size());
            }
        }
    }
}
