/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static io.debezium.relational.RelationalChangeRecordEmitter.PK_UPDATE_NEWKEY_FIELD;
import static io.debezium.relational.RelationalChangeRecordEmitter.UNAVAILABLE_AFTER_HEADER;
import static io.debezium.relational.RelationalChangeRecordEmitter.UNAVAILABLE_BEFORE_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.data.Envelope.Operation;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.pipeline.spi.ChangeRecordEmitter.Receiver;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.util.Clock;

public class RelationalChangeRecordEmitterTest {

    private static final Object UNAVAILABLE = new Object();

    private final Schema sourceSchema = SchemaBuilder.struct().build();
    private final OffsetContext offset = mock(OffsetContext.class);
    private final List<Operation> operations = new ArrayList<>();
    private final List<Headers> headers = new ArrayList<>();
    private final Receiver<Partition> receiver = (partition, schema, operation, key, value, offsetContext, recordHeaders) -> {
        operations.add(operation);
        headers.add(recordHeaders == null ? new ConnectHeaders() : recordHeaders);
    };
    private TableSchema tableSchema;

    @BeforeEach
    public void beforeEach() {
        Table table = Table.editor()
                .tableId(new TableId("catalog", "schema", "table"))
                .addColumns(
                        Column.editor().name("id").type("INTEGER").jdbcType(Types.INTEGER).optional(false).create(),
                        Column.editor().name("lob").type("VARCHAR").jdbcType(Types.VARCHAR).optional(true).create())
                .setPrimaryKeyNames("id")
                .create();
        Properties topicProperties = new Properties();
        topicProperties.put("topic.prefix", "test");
        tableSchema = new TableSchemaBuilder(new JdbcValueConverters(), null, SchemaNameAdjuster.NO_OP, new CustomConverterRegistry(null),
                sourceSchema, FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP), false, EventConvertingFailureHandlingMode.FAIL)
                .create(new SchemaTopicNamingStrategy(topicProperties, false), table, null, null, null);
        when(offset.getSourceInfo()).thenReturn(new Struct(sourceSchema));
    }

    @Test
    @FixFor("debezium/dbz#3052")
    public void shouldListUnavailableColumnsInHeaders() throws InterruptedException {
        emit(Operation.CREATE, null, new Object[]{ 1, UNAVAILABLE });
        emit(Operation.UPDATE, new Object[]{ 1, "a" }, new Object[]{ 1, UNAVAILABLE });
        emit(Operation.UPDATE, new Object[]{ 1, UNAVAILABLE }, new Object[]{ 1, "b" });
        emit(Operation.DELETE, new Object[]{ 1, UNAVAILABLE }, null);

        assertThat(operations).containsExactly(Operation.CREATE, Operation.UPDATE, Operation.UPDATE, Operation.DELETE);
        assertHeaders(headers.get(0), null, List.of("lob"));
        assertHeaders(headers.get(1), null, List.of("lob"));
        assertHeaders(headers.get(2), List.of("lob"), null);
        assertHeaders(headers.get(3), List.of("lob"), null);
    }

    @Test
    @FixFor("debezium/dbz#3052")
    public void shouldNotAddHeadersWhenAllColumnsAreAvailable() throws InterruptedException {
        emit(Operation.UPDATE, new Object[]{ 1, "a" }, new Object[]{ 1, "b" });

        assertHeaders(headers.get(0), null, null);
    }

    @Test
    @FixFor("debezium/dbz#3052")
    public void shouldBuildPrimaryKeyChangeHeadersFromValuesFilledInBySubclass() throws InterruptedException {
        Object[] newValues = { 2, UNAVAILABLE };
        new TestEmitter(Operation.UPDATE, new Object[]{ 1, UNAVAILABLE }, newValues) {
            @Override
            protected void emitUpdateAsPrimaryKeyChangeRecord(Receiver<Partition> receiver, TableSchema tableSchema, Struct oldKey,
                                                              Struct newKey, Struct oldValue, Struct newValue)
                    throws InterruptedException {
                newValues[1] = "reselected";
                super.emitUpdateAsPrimaryKeyChangeRecord(receiver, tableSchema, oldKey, newKey, oldValue, newValue);
            }
        }.emitChangeRecords(tableSchema, receiver);

        assertThat(operations).containsExactly(Operation.DELETE, Operation.CREATE);
        assertThat(headers.get(0).lastWithName(PK_UPDATE_NEWKEY_FIELD)).isNotNull();
        assertHeaders(headers.get(0), List.of("lob"), null);
        assertHeaders(headers.get(1), null, null);
    }

    private void emit(Operation operation, Object[] oldValues, Object[] newValues) throws InterruptedException {
        new TestEmitter(operation, oldValues, newValues).emitChangeRecords(tableSchema, receiver);
    }

    private static void assertHeaders(Headers headers, List<String> before, List<String> after) {
        assertThat(valueOf(headers.lastWithName(UNAVAILABLE_BEFORE_HEADER))).isEqualTo(before);
        assertThat(valueOf(headers.lastWithName(UNAVAILABLE_AFTER_HEADER))).isEqualTo(after);
    }

    private static Object valueOf(Header header) {
        return header == null ? null : header.value();
    }

    private class TestEmitter extends RelationalChangeRecordEmitter<Partition> {

        private final Operation operation;
        private final Object[] oldValues;
        private final Object[] newValues;

        TestEmitter(Operation operation, Object[] oldValues, Object[] newValues) {
            super(() -> Map.of(), offset, Clock.system(), null);
            this.operation = operation;
            this.oldValues = oldValues;
            this.newValues = newValues;
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        protected Object[] getOldColumnValues() {
            return oldValues;
        }

        @Override
        protected Object[] getNewColumnValues() {
            return newValues;
        }

        @Override
        public boolean skipMessagesWithoutChange() {
            return false;
        }

        @Override
        protected boolean isUnavailableValue(Object value) {
            return value == UNAVAILABLE;
        }
    }
}
