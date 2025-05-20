/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

import oracle.sql.CharacterSet;

/**
 * Basic tests for {@link OracleDatabaseSchema} class using mocked JDBC connection.
 *
 * @author vjuranek
 */
public class OracleDatabaseSchemaTest {

    protected OracleConnection connection;
    protected OracleDatabaseSchema schema;

    @Before
    public void before() throws Exception {
        DebeziumOpenLineageEmitter.init(Configuration.empty(), "oracle");
        this.connection = Mockito.mock(OracleConnection.class);
        Mockito.when(this.connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.UTF8_CHARSET));
        this.schema = createOracleDatabaseSchema();
    }

    @After
    public void after() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
    }

    @Test
    @FixFor("DBZ-8465")
    public void shouldGetNullFromCacheForNonExistingItem() {
        // We should get null as the item is not in the cache.
        assertThat(schema.getTableIdByObjectId(0L, 0L)).isNull();
        // Try ti again - here the item should be in a cache with NO_SUCH_TABLE placeholder,
        // but we should still get null as before.
        assertThat(schema.getTableIdByObjectId(0L, 0L)).isNull();
    }

    private OracleDatabaseSchema createOracleDatabaseSchema() {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(TestHelper.defaultConfig().build());
        final TopicNamingStrategy topicNamingStrategy = SchemaTopicNamingStrategy.create(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        final OracleValueConverters converters = connectorConfig.getAdapter().getValueConverter(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);
        final StreamingAdapter.TableNameCaseSensitivity sensitivity = StreamingAdapter.TableNameCaseSensitivity.INSENSITIVE;

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicNamingStrategy,
                sensitivity,
                false);

        Table table = Table.editor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"))
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }
}
