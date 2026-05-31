/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ResultSetConsumer;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.service.spi.ServiceRegistry;

/**
 * Unit tests for the optional per-column local re-selection cache in {@link ReselectColumnsPostProcessor}.
 * The JDBC layer is mocked and the {@link ResultSetConsumer} callback is driven against a mocked
 * {@link ResultSet}, so the tests assert which columns are queried (and how often) without a real
 * database connection.
 *
 * @author Gaurav Miglani
 */
public class ReselectColumnsPostProcessorCacheTest {

    private static final String DB = "testdb";
    private static final String SCHEMA = "s1";
    private static final String TABLE = "t1";
    private static final String UNAVAILABLE = "__debezium_unavailable_value";

    private JdbcConnection jdbcConnection;
    private RelationalDatabaseSchema relationalSchema;
    private RelationalDatabaseConnectorConfig connectorConfig;
    private ValueConverterProvider valueConverterProvider;
    private CustomConverterRegistry customConverterRegistry;
    private BeanRegistry beanRegistry;
    private ServiceRegistry serviceRegistry;

    private Table table;
    private TableId tableId;
    private Schema valueSchema;

    @BeforeEach
    public void before() throws Exception {
        tableId = new TableId(DB, SCHEMA, TABLE);

        final Column id = Column.editor().name("id").optional(false).create();
        final Column data = Column.editor().name("data").create();
        final Column name = Column.editor().name("name").create();
        table = Table.editor()
                .tableId(tableId)
                .addColumn(id)
                .addColumn(data)
                .addColumn(name)
                .setPrimaryKeyNames("id")
                .create();

        jdbcConnection = mock(JdbcConnection.class);
        relationalSchema = mock(RelationalDatabaseSchema.class);
        connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        valueConverterProvider = mock(ValueConverterProvider.class);
        customConverterRegistry = mock(CustomConverterRegistry.class);
        beanRegistry = mock(BeanRegistry.class);
        serviceRegistry = mock(ServiceRegistry.class);

        when(jdbcConnection.createTableId(DB, SCHEMA, TABLE)).thenReturn(tableId);
        when(jdbcConnection.getQualifiedTableName(tableId)).thenReturn(SCHEMA + "." + TABLE);

        // Drive the ResultSetConsumer with a mocked ResultSet that returns "db-<column>" per column,
        // and report the row as found so we can assert exactly which columns reached the database.
        when(jdbcConnection.reselectColumns(eq(table), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class)))
                .thenAnswer(this::answerReselect);

        when(connectorConfig.getUnavailableValuePlaceholder()).thenReturn(UNAVAILABLE.getBytes());
        when(connectorConfig.isSignalDataCollection(any(TableId.class))).thenReturn(false);
        when(relationalSchema.tableFor(tableId)).thenReturn(table);
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

        valueSchema = buildValueSchema();
    }

    @Test
    public void cacheDisabledQueriesDatabaseEveryTime() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(false, 1000, 5000);

        processor.apply(buildKey(), placeholderEvent(true, false));
        processor.apply(buildKey(), placeholderEvent(true, false));

        verify(jdbcConnection, times(2)).reselectColumns(eq(table), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void cacheEnabledSecondApplyServedFromCache() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        final Struct first = placeholderEvent(true, false);
        processor.apply(buildKey(), first);
        assertThat(first.getStruct(Envelope.FieldName.AFTER).get("data")).isEqualTo("db-data");

        final Struct second = placeholderEvent(true, false);
        processor.apply(buildKey(), second);
        assertThat(second.getStruct(Envelope.FieldName.AFTER).get("data")).isEqualTo("db-data");

        verify(jdbcConnection, times(1)).reselectColumns(eq(table), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void partialHitServesQueryAndModifyCachedColumnsWithoutRequery() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        // Event 1: 'data' is a placeholder -> queried and cached as "db-data". 'name' arrives with a
        // real value ("real-name") and is cached on modify (see cacheModifiedColumns).
        processor.apply(buildKey(), placeholderEvent(true, false));
        // Event 2: both 'data' and 'name' are placeholders -> 'data' hits the query-cache and 'name'
        // hits the modify-cache, so neither column is re-queried. Serving the cached "real-name" is
        // correct: 'name' re-emitted as a placeholder means it was unchanged, so its prior real value
        // is still the row's current state.
        final Struct second = placeholderEvent(true, true);
        processor.apply(buildKey(), second);

        assertThat(second.getStruct(Envelope.FieldName.AFTER).get("data")).isEqualTo("db-data");
        assertThat(second.getStruct(Envelope.FieldName.AFTER).get("name")).isEqualTo("real-name");

        // 'data' was queried exactly once (event 1); 'name' was never queried (served from cache).
        verify(jdbcConnection).reselectColumns(eq(table), eq(List.of("data")), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
        verify(jdbcConnection, never()).reselectColumns(eq(table), eq(List.of("name")), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void modifiedColumnIsCachedAndServedToLaterPlaceholderEvent() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        // Event 1: 'data' placeholder -> queried and cached as "db-data".
        processor.apply(buildKey(), placeholderEvent(true, false));

        // Event 2: 'data' arrives with a REAL value (modified) -> the new value is cached directly,
        // refreshing the entry without a query.
        final Struct modify = new Struct(valueSchema)
                .put(Envelope.FieldName.AFTER, afterStruct(1, "BBB", "real-name"))
                .put(Envelope.FieldName.SOURCE, sourceStruct())
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
        processor.apply(buildKey(), modify);

        // Event 3: 'data' placeholder again -> served from the cache with the modified value, no re-query.
        final Struct third = placeholderEvent(true, false);
        processor.apply(buildKey(), third);

        assertThat(third.getStruct(Envelope.FieldName.AFTER).get("data")).isEqualTo("BBB");
        // Only Event 1 hit the database for 'data'; Events 2 and 3 were served/refreshed from the cache.
        verify(jdbcConnection, times(1)).reselectColumns(eq(table), eq(List.of("data")), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void cachedNullValueIsServedWithoutRequery() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        // Event 1: 'data' arrives with a real null value -> cached as null.
        final Struct modify = new Struct(valueSchema)
                .put(Envelope.FieldName.AFTER, afterStruct(1, null, "real-name"))
                .put(Envelope.FieldName.SOURCE, sourceStruct())
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
        processor.apply(buildKey(), modify);

        // Event 2: 'data' placeholder -> the cached null is a hit and is served without a query.
        final Struct second = placeholderEvent(true, false);
        processor.apply(buildKey(), second);

        assertThat(second.getStruct(Envelope.FieldName.AFTER).get("data")).isNull();
        verify(jdbcConnection, never()).reselectColumns(eq(table), eq(List.of("data")), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void cacheEntryExpiresAfterTtl() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 50);

        processor.apply(buildKey(), placeholderEvent(true, false));
        Thread.sleep(80); // exceed the 50ms TTL
        processor.apply(buildKey(), placeholderEvent(true, false));

        verify(jdbcConnection, times(2)).reselectColumns(eq(table), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void differentKeysDoNotShareCacheEntry() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        processor.apply(buildKey(1), placeholderEvent(1, true, false));
        processor.apply(buildKey(2), placeholderEvent(2, true, false));

        verify(jdbcConnection, times(2)).reselectColumns(eq(table), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    @Test
    public void readEventsAreNeverReselected() throws Exception {
        final ReselectColumnsPostProcessor processor = newProcessor(true, 1000, 60_000);

        final Struct read = new Struct(valueSchema)
                .put(Envelope.FieldName.AFTER, afterStruct(1, UNAVAILABLE, "n"))
                .put(Envelope.FieldName.SOURCE, sourceStruct())
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.READ.code());
        processor.apply(buildKey(), read);

        verify(jdbcConnection, never()).reselectColumns(any(Table.class), anyList(), anyList(), anyList(), any(Struct.class), any(ResultSetConsumer.class));
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

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

    private ReselectColumnsPostProcessor newProcessor(boolean cacheEnabled, int maxSize, long ttlMs) {
        final ReselectColumnsPostProcessor processor = new ReselectColumnsPostProcessor();
        final Map<String, Object> props = new HashMap<>();
        props.put("reselect.null.values", "false");
        props.put("reselect.unavailable.values", "true");
        props.put("reselect.cache.enabled", String.valueOf(cacheEnabled));
        props.put("reselect.cache.max.size", String.valueOf(maxSize));
        props.put("reselect.cache.ttl.ms", String.valueOf(ttlMs));
        processor.configure(props);
        processor.injectBeanRegistry(beanRegistry);
        processor.injectServiceRegistry(serviceRegistry);
        return processor;
    }

    private Schema buildValueSchema() {
        final Schema afterSchema = SchemaBuilder.struct().name("after")
                .field("id", Schema.INT32_SCHEMA)
                .field("data", Schema.OPTIONAL_STRING_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Schema sourceSchema = SchemaBuilder.struct().name("source")
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .build();
        return SchemaBuilder.struct().name("envelope")
                .field(Envelope.FieldName.AFTER, afterSchema)
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .build();
    }

    private Struct buildKey() {
        return buildKey(1);
    }

    private Struct buildKey(int id) {
        final Schema keySchema = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();
        return new Struct(keySchema).put("id", id);
    }

    private Struct placeholderEvent(boolean dataToast, boolean nameToast) {
        return placeholderEvent(1, dataToast, nameToast);
    }

    private Struct placeholderEvent(int id, boolean dataToast, boolean nameToast) {
        final Object data = dataToast ? UNAVAILABLE : "real-data";
        final Object name = nameToast ? UNAVAILABLE : "real-name";
        return new Struct(valueSchema)
                .put(Envelope.FieldName.AFTER, afterStruct(id, data, name))
                .put(Envelope.FieldName.SOURCE, sourceStruct())
                .put(Envelope.FieldName.OPERATION, Envelope.Operation.UPDATE.code());
    }

    private Struct afterStruct(int id, Object data, Object name) {
        return new Struct(valueSchema.field(Envelope.FieldName.AFTER).schema())
                .put("id", id)
                .put("data", data)
                .put("name", name);
    }

    private Struct sourceStruct() {
        return new Struct(valueSchema.field(Envelope.FieldName.SOURCE).schema())
                .put(AbstractSourceInfo.DATABASE_NAME_KEY, DB)
                .put(AbstractSourceInfo.SCHEMA_NAME_KEY, SCHEMA)
                .put(AbstractSourceInfo.TABLE_NAME_KEY, TABLE);
    }
}
