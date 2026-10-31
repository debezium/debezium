/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.logminer.parser.ParserUtils;
import io.debezium.connector.oracle.relational.OracleColumnEditor;
import io.debezium.connector.oracle.relational.OracleRelationalMetadataFactory;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.CustomConverterRegistry;
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

    @BeforeEach
    void before() throws Exception {
        this.connection = Mockito.mock(OracleConnection.class);
        Mockito.when(this.connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.UTF8_CHARSET));
        Mockito.when(this.connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        this.schema = createOracleDatabaseSchema();
    }

    @AfterEach
    void after() {
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

    @Test
    @FixFor("debezium/dbz#2798")
    public void shouldResolveObjectIdOnceTableIsRegistered() {
        // The first lookup records the object id as unknown, both in the cache and in the last-lookup fast path.
        assertThat(schema.getTableIdByObjectId(250282L, null)).isNull();

        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ2798");
        schema.refresh(OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(tableId)
                .addColumn(OracleRelationalMetadataFactory.INSTANCE.columnEditor().name("ID").type("VARCHAR2(50)").create())
                .addAttribute(Attribute.editor().name(OracleDatabaseSchema.ATTRIBUTE_OBJECT_ID).value(250282L).create())
                .addAttribute(Attribute.editor().name(OracleDatabaseSchema.ATTRIBUTE_DATA_OBJECT_ID).value(250282L).create())
                .create());

        // Registering the table must supersede the unknown answer for the same object id.
        assertThat(schema.getTableIdByObjectId(250282L, null)).isEqualTo(tableId);
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldRegisterExtendedStringColumnsByOctetLength() {
        schema.close();
        schema = createOracleDatabaseSchema(true, true);

        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ2794");
        schema.refresh(OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(tableId)
                .addColumn(column("ID", Types.NUMERIC, "NUMBER", 9, Column.UNSET_INT_VALUE))
                // VARCHAR2(2000 CHAR) on AL32UTF8
                .addColumn(column("C1", Types.VARCHAR, "VARCHAR2", 2000, 8000))
                // VARCHAR2(2000 BYTE)
                .addColumn(column("C2", Types.VARCHAR, "VARCHAR2", 2000, 2000))
                // NVARCHAR2(2001) on AL16UTF16
                .addColumn(column("C3", Types.NVARCHAR, "NVARCHAR2", 2001, 4002))
                // NVARCHAR2(2000) on AL16UTF16
                .addColumn(column("C4", Types.NVARCHAR, "NVARCHAR2", 2000, 4000))
                // VARCHAR2(4000 BYTE)
                .addColumn(column("C5", Types.VARCHAR, "VARCHAR2", 4000, 4000))
                .setPrimaryKeyNames("ID")
                .create());

        assertThat(schema.getLobColumnsForTable(tableId)).extracting(Column::name).containsExactly("C1", "C3");

        final Table table = schema.tableFor(tableId);
        assertThat(ParserUtils.getColumnUnavailableValue(null, table.columnWithName("C1"))).isSameAs(OracleValueConverters.UNAVAILABLE_VALUE);
        assertThat(ParserUtils.getColumnUnavailableValue(null, table.columnWithName("C2"))).isNull();
        assertThat(schema.isColumnUnavailableValuePlaceholder(table.columnWithName("C1"), "__debezium_unavailable_value")).isTrue();
        assertThat(schema.isColumnUnavailableValuePlaceholder(table.columnWithName("C2"), "__debezium_unavailable_value")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldNotRegisterExtendedStringColumnsWithoutExtendedStringSupport() {
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ2794");
        schema.refresh(OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(tableId)
                .addColumn(column("ID", Types.NUMERIC, "NUMBER", 9, Column.UNSET_INT_VALUE))
                .addColumn(column("C1", Types.VARCHAR, "VARCHAR2", 2000, 8000))
                .setPrimaryKeyNames("ID")
                .create());

        assertThat(schema.getLobColumnsForTable(tableId)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldFallBackToDeclaredLengthWhenOctetLengthIsUnset() {
        schema.close();
        schema = createOracleDatabaseSchema(true, true);

        // Columns restored from schema history written by older versions have no octet length
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ2794");
        schema.refresh(OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(tableId)
                .addColumn(column("ID", Types.NUMERIC, "NUMBER", 9, Column.UNSET_INT_VALUE))
                .addColumn(column("C1", Types.VARCHAR, "VARCHAR2", 8000, Column.UNSET_INT_VALUE))
                .addColumn(column("C2", Types.VARCHAR, "VARCHAR2", 2000, Column.UNSET_INT_VALUE))
                .addColumn(column("C3", Types.NVARCHAR, "NVARCHAR2", 2001, Column.UNSET_INT_VALUE))
                .setPrimaryKeyNames("ID")
                .create());

        assertThat(schema.getLobColumnsForTable(tableId)).extracting(Column::name).containsExactly("C1", "C3");
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldWarnWhenDatabaseCharacterSetIsMultibyteButNotUnicode() {
        final LogInterceptor logInterceptor = new LogInterceptor(OracleDatabaseSchema.class);
        final String message = "is a multibyte character set that is not Unicode";

        schema.close();
        schema = createOracleDatabaseSchema(true, true);
        assertThat(logInterceptor.containsWarnMessage(message)).isFalse();

        // Extended strings are not captured without lob.enabled, so the warning does not apply
        schema.close();
        Mockito.when(this.connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.ZHS16GBK_CHARSET));
        schema = createOracleDatabaseSchema(true, false);
        assertThat(logInterceptor.containsWarnMessage(message)).isFalse();

        schema.close();
        schema = createOracleDatabaseSchema(true, true);
        assertThat(logInterceptor.containsWarnMessage(message)).isTrue();
        assertThat(logInterceptor.containsWarnMessage("estimates 2 bytes per character")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldResolveMaxBytesPerCharFromCharacterSet() {
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET))).isEqualTo(4);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.UTF8_CHARSET))).isEqualTo(3);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET))).isEqualTo(2);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET))).isEqualTo(1);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.US7ASCII_CHARSET))).isEqualTo(1);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.ZHS16GBK_CHARSET))).isEqualTo(2);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.JA16SJIS_CHARSET))).isEqualTo(2);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.JA16EUC_CHARSET))).isEqualTo(3);
        assertThat(OracleDatabaseSchema.getMaxBytesPerChar(CharacterSet.make(CharacterSet.ZHT32EUC_CHARSET))).isEqualTo(4);
    }

    private static Column column(String name, int jdbcType, String typeName, int length, int octetLength) {
        final OracleColumnEditor editor = OracleRelationalMetadataFactory.INSTANCE.columnEditor();
        editor.name(name).jdbcType(jdbcType).type(typeName).length(length);
        return editor.octetLength(octetLength).create();
    }

    private OracleDatabaseSchema createOracleDatabaseSchema() {
        return createOracleDatabaseSchema(false, false);
    }

    private OracleDatabaseSchema createOracleDatabaseSchema(boolean extendedStringsSupported, boolean lobEnabled) {
        Configuration configuration = TestHelper.defaultConfig().with(OracleConnectorConfig.LOB_ENABLED, lobEnabled).build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(configuration);
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
                extendedStringsSupported,
                new CustomConverterRegistry(List.of()),
                new OracleTaskContext(configuration, connectorConfig));

        Table table = OracleRelationalMetadataFactory.INSTANCE.tableEditor()
                .tableId(TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE"))
                .addColumn(OracleRelationalMetadataFactory.INSTANCE.columnEditor().name("ID").create())
                .addColumn(OracleRelationalMetadataFactory.INSTANCE.columnEditor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }
}
