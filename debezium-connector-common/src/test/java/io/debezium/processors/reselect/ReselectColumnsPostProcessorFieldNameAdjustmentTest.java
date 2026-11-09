/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ResultSetConsumer;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;

/**
 * Unit tests for {@link ReselectColumnsPostProcessor} when {@code field.name.adjustment.mode} makes the
 * event's field names differ from the relational column names, e.g. {@code ID$$} emitted as {@code ID__}.
 * The JDBC layer is mocked, as there is no database to re-select from, while the field namer is the real
 * Avro one used by the connectors.
 */
public class ReselectColumnsPostProcessorFieldNameAdjustmentTest {

    private static final String DB = "testdb";
    private static final String SCHEMA = "s1";
    private static final String RESELECTED_TABLE = "t1";
    private static final String OTHER_TABLE = "t2";
    private static final String UNAVAILABLE = "__debezium_unavailable_value";
    private static final String INCLUDE_LIST = "s1\\.t1:DATA,s1\\.t1:DOC\\$";

    private final FieldNamer<Column> fieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.AVRO_FIELD_NAMER);

    private JdbcConnection jdbcConnection;
    private BeanRegistry beanRegistry;
    private ServiceRegistry serviceRegistry;

    private TableId reselectedTableId;
    private TableId otherTableId;
    private Table reselectedTable;
    private Table otherTable;

    @BeforeEach
    public void before() throws Exception {
        reselectedTableId = new TableId(DB, SCHEMA, RESELECTED_TABLE);
        otherTableId = new TableId(DB, SCHEMA, OTHER_TABLE);

        reselectedTable = Table.editor()
                .tableId(reselectedTableId)
                .addColumn(Column.editor().name("ID$$").optional(false).create())
                .addColumn(Column.editor().name("DATA").create())
                .addColumn(Column.editor().name("DOC$").create())
                .setPrimaryKeyNames("ID$$")
                .create();

        otherTable = Table.editor()
                .tableId(otherTableId)
                .addColumn(Column.editor().name("COMPANYID$$").optional(false).create())
                .addColumn(Column.editor().name("PHONE").create())
                .setPrimaryKeyNames("COMPANYID$$")
                .create();

        jdbcConnection = mock(JdbcConnection.class);
        final RelationalDatabaseSchema relationalSchema = mock(RelationalDatabaseSchema.class);
        final RelationalDatabaseConnectorConfig connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        final ValueConverterProvider valueConverterProvider = mock(ValueConverterProvider.class);
        final CustomConverterRegistry customConverterRegistry = mock(CustomConverterRegistry.class);
        beanRegistry = mock(BeanRegistry.class);
        serviceRegistry = mock(ServiceRegistry.class);

        when(jdbcConnection.createTableId(DB, SCHEMA, RESELECTED_TABLE)).thenReturn(reselectedTableId);
        when(jdbcConnection.createTableId(DB, SCHEMA, OTHER_TABLE)).thenReturn(otherTableId);
        when(jdbcConnection.getQualifiedTableName(reselectedTableId)).thenReturn(SCHEMA + "." + RESELECTED_TABLE);
        when(jdbcConnection.getQualifiedTableName(otherTableId)).thenReturn(SCHEMA + "." + OTHER_TABLE);
        when(jdbcConnection.reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class)))
                .thenAnswer(this::answerReselect);

        when(connectorConfig.getUnavailableValuePlaceholder()).thenReturn(UNAVAILABLE.getBytes());
        when(connectorConfig.isSignalDataCollection(any(TableId.class))).thenReturn(false);
        when(connectorConfig.getFieldNamer()).thenReturn(fieldNamer);
        when(relationalSchema.tableFor(reselectedTableId)).thenReturn(reselectedTable);
        when(relationalSchema.tableFor(otherTableId)).thenReturn(otherTable);
        when(valueConverterProvider.converter(any(Column.class), any())).thenReturn(null);
        when(customConverterRegistry.getValueConverter(any(TableId.class), any(Column.class))).thenReturn(Optional.empty());

        when(beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, RelationalDatabaseConnectorConfig.class))
                .thenReturn(connectorConfig);
        when(beanRegistry.lookupByName(StandardBeanNames.VALUE_CONVERTER, ValueConverterProvider.class))
                .thenReturn(valueConverterProvider);
        when(beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class))
                .thenReturn(jdbcConnection);
        when(beanRegistry.lookupByName(StandardBeanNames.DATABASE_SCHEMA, RelationalDatabaseSchema.class))
                .thenReturn(relationalSchema);
        when(serviceRegistry.tryGetService(CustomConverterRegistry.class)).thenReturn(customConverterRegistry);
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void tableNotInIncludeListWithAdjustedKeyColumnPassesThroughUntouched() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, false, null);

        final Struct after = otherAfter(1, UNAVAILABLE);
        processor.apply(otherKey(1), envelope(after, OTHER_TABLE));

        assertThat(after.get("PHONE")).isEqualTo(UNAVAILABLE);
        verify(jdbcConnection, never()).reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void adjustedPrimaryKeyColumnIsQueriedByRelationalName() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, false, null);

        final Struct after = reselectedAfter(7, UNAVAILABLE, "real-doc");
        processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DATA")).isEqualTo("db-DATA");
        verify(jdbcConnection).reselectColumns(eq(reselectedTable), eq(List.of("DATA")), eq(List.of("ID$$")), eq(List.of(7)),
                any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void adjustedEventKeyFieldIsQueriedByRelationalName() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, false, null);

        final Struct after = reselectedAfter(7, UNAVAILABLE, "real-doc");
        processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DATA")).isEqualTo("db-DATA");
        verify(jdbcConnection).reselectColumns(eq(reselectedTable), eq(List.of("DATA")), eq(List.of("ID$$")), eq(List.of(7)),
                any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void adjustedColumnIsMatchedQueriedAndWrittenBackUnderFieldName() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, false, null);

        final Struct after = reselectedAfter(7, "real-data", UNAVAILABLE);
        processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DOC_")).isEqualTo("db-DOC$");
        assertThat(after.get("DATA")).isEqualTo("real-data");
        verify(jdbcConnection).reselectColumns(eq(reselectedTable), eq(List.of("DOC$")), eq(List.of("ID$$")), eq(List.of(7)),
                any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void modifiedAdjustedColumnIsServedFromCache() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, true, null);

        // Event 1 carries a real value for DOC$, which is cached on modify.
        processor.apply(reselectedKey(7), envelope(reselectedAfter(7, "real-data", "new-doc"), RESELECTED_TABLE));

        // Event 2 re-emits DOC$ as a placeholder, which is served from the cache without a query.
        final Struct after = reselectedAfter(7, "real-data", UNAVAILABLE);
        processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DOC_")).isEqualTo("new-doc");
        verify(jdbcConnection, never()).reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void keyColumnMissingFromEventFailsWithClearMessage() {
        final ReselectColumnsPostProcessor processor = newProcessor(false, false, "fail");

        final Struct after = reselectedAfterWithoutKey(UNAVAILABLE);
        assertThatThrownBy(() -> processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("ID$$")
                .hasMessageContaining("reselect.use.event.key");
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void keyColumnMissingFromEventIsSkippedWhenWarning() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, false, "warn");

        final Struct after = reselectedAfterWithoutKey(UNAVAILABLE);
        processor.apply(reselectedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DATA")).isEqualTo(UNAVAILABLE);
        verify(jdbcConnection, never()).reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void eventKeyFieldMatchingNoColumnFailsWithClearMessage() {
        final ReselectColumnsPostProcessor processor = newProcessor(true, false, "fail");

        final Struct after = reselectedAfter(7, UNAVAILABLE, "real-doc");
        assertThatThrownBy(() -> processor.apply(unmatchedKey(7), envelope(after, RESELECTED_TABLE)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("UNKNOWN_KEY")
                .hasMessageContaining(reselectedTableId.toString());
    }

    @Test
    @FixFor("debezium/dbz#3050")
    public void eventKeyFieldMatchingNoColumnIsSkippedWhenWarning() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, false, "warn");

        final Struct after = reselectedAfter(7, UNAVAILABLE, "real-doc");
        processor.apply(unmatchedKey(7), envelope(after, RESELECTED_TABLE));

        assertThat(after.get("DATA")).isEqualTo(UNAVAILABLE);
        verify(jdbcConnection, never()).reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    private boolean answerReselect(InvocationOnMock invocation) throws Exception {
        final List<String> columns = invocation.getArgument(1);
        final ResultSetConsumer consumer = invocation.getArgument(5);
        final ResultSet rs = mock(ResultSet.class);
        for (String column : columns) {
            when(rs.getObject(column)).thenReturn("db-" + column);
        }
        consumer.accept(rs);
        return true;
    }

    private ReselectColumnsPostProcessor newProcessor(boolean useEventKey, boolean cacheEnabled, String errorHandlingMode) {
        final ReselectColumnsPostProcessor processor = new ReselectColumnsPostProcessor();
        final Map<String, Object> props = new HashMap<>();
        props.put("reselect.columns.include.list", INCLUDE_LIST);
        props.put("reselect.null.values", "false");
        props.put("reselect.unavailable.values", "true");
        props.put("reselect.use.event.key", String.valueOf(useEventKey));
        props.put("reselect.cache.enabled", String.valueOf(cacheEnabled));
        if (errorHandlingMode != null) {
            props.put("reselect.error.handling.mode", errorHandlingMode);
        }
        processor.configure(props);
        processor.injectBeanRegistry(beanRegistry);
        processor.injectServiceRegistry(serviceRegistry);
        return processor;
    }

    private String fieldName(Table table, String columnName) {
        return fieldNamer.fieldNameFor(table.columnWithName(columnName));
    }

    private Schema reselectedAfterSchema() {
        return SchemaBuilder.struct().name("t1.Value")
                .field(fieldName(reselectedTable, "ID$$"), Schema.INT32_SCHEMA)
                .field(fieldName(reselectedTable, "DATA"), Schema.OPTIONAL_STRING_SCHEMA)
                .field(fieldName(reselectedTable, "DOC$"), Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    private Struct reselectedAfter(int id, String data, String doc) {
        return new Struct(reselectedAfterSchema())
                .put("ID__", id)
                .put("DATA", data)
                .put("DOC_", doc);
    }

    private Struct reselectedAfterWithoutKey(String data) {
        // The key column was removed from the event, e.g. through column.exclude.list
        final Schema schema = SchemaBuilder.struct().name("t1.Value")
                .field("DATA", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        return new Struct(schema).put("DATA", data);
    }

    private Struct reselectedKey(int id) {
        final Schema keySchema = SchemaBuilder.struct().name("t1.Key").field("ID__", Schema.INT32_SCHEMA).build();
        return new Struct(keySchema).put("ID__", id);
    }

    private Struct unmatchedKey(int id) {
        // A key field that maps to no column of the table
        final Schema keySchema = SchemaBuilder.struct().name("t1.Key").field("UNKNOWN_KEY", Schema.INT32_SCHEMA).build();
        return new Struct(keySchema).put("UNKNOWN_KEY", id);
    }

    private Struct otherAfter(int id, String phone) {
        final Schema schema = SchemaBuilder.struct().name("t2.Value")
                .field(fieldName(otherTable, "COMPANYID$$"), Schema.INT32_SCHEMA)
                .field(fieldName(otherTable, "PHONE"), Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        return new Struct(schema).put("COMPANYID__", id).put("PHONE", phone);
    }

    private Struct otherKey(int id) {
        final Schema keySchema = SchemaBuilder.struct().name("t2.Key").field("COMPANYID__", Schema.INT32_SCHEMA).build();
        return new Struct(keySchema).put("COMPANYID__", id);
    }

    private Struct envelope(Struct after, String tableName) {
        final Schema sourceSchema = SchemaBuilder.struct().name("source")
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct().name("envelope")
                .field(Envelope.FieldName.AFTER, after.schema())
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .build();
        final Struct source = new Struct(sourceSchema)
                .put(AbstractSourceInfo.DATABASE_NAME_KEY, DB)
                .put(AbstractSourceInfo.SCHEMA_NAME_KEY, SCHEMA)
                .put(AbstractSourceInfo.TABLE_NAME_KEY, tableName);
        return new Struct(envelopeSchema)
                .put(Envelope.FieldName.AFTER, after)
                .put(Envelope.FieldName.SOURCE, source)
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
    }
}
