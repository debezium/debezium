/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Collections;
import java.util.Properties;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.junit.relational.TestRelationalDatabaseConfig;
import io.debezium.relational.Key.CustomKeyMapper;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.common.ReplacementFunction;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.time.Date;

public class TableSchemaBuilderTest {

    private static final String AVRO_UNSUPPORTED_NAME = "9-`~!@#$%^&*()+=[]{}\\|;:\"'<>,.?/";
    private static final String AVRO_UNSUPPORTED_NAME_CONVERTED = "_9_______________________________";
    private static final String AVRO_UNICODE_NAME_CONVERTED = "_u0039_u002d_u0060_u007e_u0021_u0040_u0023_u0024_u0025_u005e_u0026_u002a_u0028_u0029_u002b_u003d_u005b_u005d_u007b_u007d_u005c_u007c_u003b_u003a_u0022_u0027_u003c_u003e_u002c_u002e_u003f_u002f";

    private final TableId id = new TableId("catalog", "schema", "table");
    private final Object[] data = new Object[]{ "c1value", 3.142d, java.sql.Date.valueOf("2001-10-31"), 4, new byte[]{ 71, 117, 110, 110, 97, 114 }, null, "c7value",
            "c8value", "c9value", null };
    private final Object[] keyData = new Object[]{ "c1value", 3.142d };
    private Table table;
    private Column c1;
    private Column c2;
    private Column c3;
    private Column c4;
    private Column c5;
    private Column c6;
    private Column c7;
    private Column c8;
    private Column c9;
    private Column c10;

    private TableSchema schema;
    private TopicNamingStrategy topicNamingStrategy;
    private SchemaNameAdjuster adjuster;
    private final CustomConverterRegistry customConverterRegistry = new CustomConverterRegistry(null);
    private Properties topicProperties;
    private FieldNamer<Column> defaultFieldNamer;
    private FieldNamer<Column> avroFieldNamer;

    @Before
    public void beforeEach() {
        adjuster = SchemaNameAdjuster.create(ReplacementFunction.UNDERSCORE_REPLACEMENT, (original, replacement, conflict) -> {
            fail("Should not have come across an invalid schema name");
        });
        defaultFieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP);
        avroFieldNamer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.AVRO_FIELD_NAMER);
        topicProperties = new Properties();
        topicProperties.put("topic.prefix", "test");
        topicNamingStrategy = new SchemaTopicNamingStrategy(topicProperties, false);
        schema = null;
        table = Table.editor()
                .tableId(id)
                .addColumns(Column.editor().name("C1")
                        .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                        .optional(false)
                        .generated(true)
                        .create(),
                        Column.editor().name("C2")
                                .type("NUMBER").jdbcType(Types.NUMERIC).length(5).scale(3)
                                .create(),
                        Column.editor().name("C3")
                                .type("DATE").jdbcType(Types.DATE).length(4)
                                .optional(true)
                                .create(),
                        Column.editor().name("C4")
                                .type("COUNTER").jdbcType(Types.INTEGER)
                                .autoIncremented(true)
                                .optional(true)
                                .create(),
                        Column.editor().name("C5")
                                .type("BINARY").jdbcType(Types.BINARY)
                                .optional(false)
                                .length(16)
                                .create(),
                        Column.editor().name("C6")
                                .type("SMALLINT").jdbcType(Types.SMALLINT)
                                .optional(false)
                                .length(1)
                                .create(),
                        Column.editor().name("7C7") // test invalid Avro name (starts with digit)
                                .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                                .optional(false)
                                .create(),
                        Column.editor().name("C-8") // test invalid Avro name ( contains dash )
                                .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                                .optional(false)
                                .create(),
                        Column.editor().name(AVRO_UNSUPPORTED_NAME)
                                .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                                .optional(false)
                                .create(),
                        Column.editor().name("UP$ID")
                                .type("NUMBER").jdbcType(Types.INTEGER)
                                .optional(false)
                                .create())
                .setPrimaryKeyNames("C1", "C2")
                .create();
        c1 = table.columnWithName("C1");
        c2 = table.columnWithName("C2");
        c3 = table.columnWithName("C3");
        c4 = table.columnWithName("C4");
        c5 = table.columnWithName("C5");
        c6 = table.columnWithName("C6");
        c7 = table.columnWithName("7C7");
        c8 = table.columnWithName("C-8");
        c9 = table.columnWithName(AVRO_UNSUPPORTED_NAME);
        c10 = table.columnWithName("UP$ID");
    }

    @Test
    public void checkPreconditions() {
        assertThat(c1).isNotNull();
        assertThat(c2).isNotNull();
        assertThat(c3).isNotNull();
        assertThat(c4).isNotNull();
        assertThat(c5).isNotNull();
        assertThat(c6).isNotNull();
        assertThat(c7).isNotNull();
        assertThat(c8).isNotNull();
        assertThat(c9).isNotNull();
        assertThat(c10).isNotNull();
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailToBuildTableSchemaFromNullTable() {
        new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, null, null, null, null);
    }

    @Test
    public void shouldBuildTableSchemaFromTable() {
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
    }

    @Test
    @FixFor("DBZ-1089")
    public void shouldBuildCorrectSchemaNames() {
        // table id with catalog and schema
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.schema.table.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.schema.table.Value");

        // only catalog
        table = table.edit()
                .tableId(new TableId("testDb", null, "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(new DefaultTopicNamingStrategy(topicProperties), table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.testDb.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.testDb.testTable.Value");

        // only schema
        table = table.edit()
                .tableId(new TableId(null, "testSchema", "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.testSchema.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.testSchema.testTable.Value");

        // neither catalog nor schema
        table = table.edit()
                .tableId(new TableId(null, null, "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.testTable.Value");
    }

    @Test
    @FixFor("DBZ-6559")
    public void shouldBuildCorrectSchemaNamesWhenPrefixHasInvalidChar() {

        topicProperties = new Properties();
        topicProperties.put("topic.prefix", "3prefix");
        topicNamingStrategy = new SchemaTopicNamingStrategy(topicProperties, false);

        // table id with catalog and schema
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("_3prefix.schema.table.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("_3prefix.schema.table.Value");

        // only catalog
        table = table.edit()
                .tableId(new TableId("testDb", null, "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(new DefaultTopicNamingStrategy(topicProperties), table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("_3prefix.testDb.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("_3prefix.testDb.testTable.Value");

        // only schema
        table = table.edit()
                .tableId(new TableId(null, "testSchema", "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("_3prefix.testSchema.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("_3prefix.testSchema.testTable.Value");

        // neither catalog nor schema
        table = table.edit()
                .tableId(new TableId(null, null, "testTable"))
                .create();

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("_3prefix.testTable.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("_3prefix.testTable.Value");
    }

    @Test
    @FixFor("DBZ-2975")
    public void shouldBuildCorrectSchemaNamesInMultiPartitionMode() {
        // table id with catalog and schema
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, true,
                EventConvertingFailureHandlingMode.FAIL)
                .create(new SchemaTopicNamingStrategy(topicProperties, true), table, null, null, null);
        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.catalog.schema.table.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.catalog.schema.table.Value");
    }

    @Test
    public void shouldBuildTableSchemaFromTableWithoutPrimaryKey() {
        table = table.edit().setPrimaryKeyNames().create();
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        // Check the keys ...
        assertThat(schema.keySchema()).isNull();
        assertThat(schema.keyFromColumnData(data)).isNull();
        // Check the values ...
        Schema values = schema.valueSchema();
        assertThat(values).isNotNull();
        assertThat(values.field("C1").name()).isEqualTo("C1");
        assertThat(values.field("C1").index()).isEqualTo(0);
        assertThat(values.field("C1").schema()).isEqualTo(SchemaBuilder.string().build());
        assertThat(values.field("C2").name()).isEqualTo("C2");
        assertThat(values.field("C2").index()).isEqualTo(1);
        assertThat(values.field("C2").schema()).isEqualTo(Decimal.builder(3).parameter("connect.decimal.precision", "5").optional().build()); // scale of 3
        assertThat(values.field("C3").name()).isEqualTo("C3");
        assertThat(values.field("C3").index()).isEqualTo(2);
        assertThat(values.field("C3").schema()).isEqualTo(Date.builder().optional().build()); // optional date
        assertThat(values.field("C4").name()).isEqualTo("C4");
        assertThat(values.field("C4").index()).isEqualTo(3);
        assertThat(values.field("C4").schema()).isEqualTo(SchemaBuilder.int32().optional().build()); // JDBC INTEGER = 32 bits
        assertThat(values.field("C5").index()).isEqualTo(4);
        assertThat(values.field("C5").schema()).isEqualTo(SchemaBuilder.bytes().build()); // JDBC BINARY = bytes
        assertThat(values.field("C6").index()).isEqualTo(5);
        assertThat(values.field("C6").schema()).isEqualTo(SchemaBuilder.int16().build());

        // Column starting with digit is left as-is
        assertThat(values.field("7C7").name()).isEqualTo("7C7");
        assertThat(values.field("7C7").index()).isEqualTo(6);
        assertThat(values.field("7C7").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column C-8 has -, left as-is
        assertThat(values.field("C-8").name()).isEqualTo("C-8");
        assertThat(values.field("C-8").index()).isEqualTo(7);
        assertThat(values.field("C-8").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column AVRO_UNSUPPORTED_NAME contains all invalid characters, left as-is
        assertThat(values.field(AVRO_UNSUPPORTED_NAME).name()).isEqualTo(AVRO_UNSUPPORTED_NAME);
        assertThat(values.field(AVRO_UNSUPPORTED_NAME).index()).isEqualTo(8);
        assertThat(values.field(AVRO_UNSUPPORTED_NAME).schema()).isEqualTo(SchemaBuilder.string().build());

        // Column UP$ID contains an invalid character, left as-is
        assertThat(values.field("UP$ID").name()).isEqualTo("UP$ID");
        assertThat(values.field("UP$ID").index()).isEqualTo(9);
        assertThat(values.field("UP$ID").schema()).isEqualTo(SchemaBuilder.int32().build());

        Struct value = schema.valueFromColumnData(data);
        assertThat(value).isNotNull();
        assertThat(value.get("C1")).isEqualTo("c1value");
        assertThat(value.get("C2")).isEqualTo(BigDecimal.valueOf(3.142d));
        assertThat(value.get("C3")).isEqualTo(11626);
        assertThat(value.get("C4")).isEqualTo(4);
        assertThat(value.get("C5")).isEqualTo(ByteBuffer.wrap(new byte[]{ 71, 117, 110, 110, 97, 114 }));
        assertThat(value.get("C6")).isEqualTo(Short.valueOf((short) 0));
    }

    @Test
    @FixFor("DBZ-1044")
    public void shouldSanitizeFieldNamesAndBuildTableSchemaFromTableWithoutPrimaryKey() {
        table = table.edit().setPrimaryKeyNames().create();
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), avroFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        // Check the keys ...
        assertThat(schema.keySchema()).isNull();
        assertThat(schema.keyFromColumnData(data)).isNull();
        // Check the values ...
        Schema values = schema.valueSchema();
        assertThat(values).isNotNull();
        assertThat(values.field("C1").name()).isEqualTo("C1");
        assertThat(values.field("C1").index()).isEqualTo(0);
        assertThat(values.field("C1").schema()).isEqualTo(SchemaBuilder.string().build());
        assertThat(values.field("C2").name()).isEqualTo("C2");
        assertThat(values.field("C2").index()).isEqualTo(1);
        assertThat(values.field("C2").schema()).isEqualTo(Decimal.builder(3).parameter("connect.decimal.precision", "5").optional().build()); // scale of 3
        assertThat(values.field("C3").name()).isEqualTo("C3");
        assertThat(values.field("C3").index()).isEqualTo(2);
        assertThat(values.field("C3").schema()).isEqualTo(Date.builder().optional().build()); // optional date
        assertThat(values.field("C4").name()).isEqualTo("C4");
        assertThat(values.field("C4").index()).isEqualTo(3);
        assertThat(values.field("C4").schema()).isEqualTo(SchemaBuilder.int32().optional().build()); // JDBC INTEGER = 32 bits
        assertThat(values.field("C5").index()).isEqualTo(4);
        assertThat(values.field("C5").schema()).isEqualTo(SchemaBuilder.bytes().build()); // JDBC BINARY = bytes
        assertThat(values.field("C6").index()).isEqualTo(5);
        assertThat(values.field("C6").schema()).isEqualTo(SchemaBuilder.int16().build());

        // Column starting with digit should be prefixed, original field should not exist
        assertThat(values.field("7C7")).isNull();

        // Column starting with digit is prefixed with _
        assertThat(values.field("_7C7").name()).isEqualTo("_7C7");
        assertThat(values.field("_7C7").index()).isEqualTo(6);
        assertThat(values.field("_7C7").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column containing '-' should have '-' replaced with '_', field should not exist
        assertThat(values.field("C-8")).isNull();

        // Column C-8 has - replaced with _
        assertThat(values.field("C_8").name()).isEqualTo("C_8");
        assertThat(values.field("C_8").index()).isEqualTo(7);
        assertThat(values.field("C_8").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column AVRO_UNSUPPORTED_NAME should be all underscores
        assertThat(values.field(AVRO_UNSUPPORTED_NAME_CONVERTED).name()).isEqualTo(AVRO_UNSUPPORTED_NAME_CONVERTED);
        assertThat(values.field(AVRO_UNSUPPORTED_NAME_CONVERTED).index()).isEqualTo(8);
        assertThat(values.field(AVRO_UNSUPPORTED_NAME_CONVERTED).schema()).isEqualTo(SchemaBuilder.string().build());

        // Column UP$ID should has $ converted to underscore
        assertThat(values.field("UP_ID").name()).isEqualTo("UP_ID");
        assertThat(values.field("UP_ID").index()).isEqualTo(9);
        assertThat(values.field("UP_ID").schema()).isEqualTo(SchemaBuilder.int32().build());

        Struct value = schema.valueFromColumnData(data);
        assertThat(value).isNotNull();
        assertThat(value.get("C1")).isEqualTo("c1value");
        assertThat(value.get("C2")).isEqualTo(BigDecimal.valueOf(3.142d));
        assertThat(value.get("C3")).isEqualTo(11626);
        assertThat(value.get("C4")).isEqualTo(4);
        assertThat(value.get("C5")).isEqualTo(ByteBuffer.wrap(new byte[]{ 71, 117, 110, 110, 97, 114 }));
        assertThat(value.get("C6")).isEqualTo(Short.valueOf((short) 0));
    }

    @Test
    @FixFor("DBZ-5743")
    public void shouldSanitizeUnicodeFieldNamesAndBuildTableSchemaFromTableWithoutPrimaryKey() {
        table = table.edit().setPrimaryKeyNames().create();
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), FieldNameSelector.defaultSelector(SchemaNameAdjuster.AVRO_UNICODE_FIELD_NAMER),
                false, EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        // Check the keys ...
        assertThat(schema.keySchema()).isNull();
        assertThat(schema.keyFromColumnData(data)).isNull();
        // Check the values ...
        Schema values = schema.valueSchema();
        assertThat(values).isNotNull();
        assertThat(values.field("C1").name()).isEqualTo("C1");
        assertThat(values.field("C1").index()).isEqualTo(0);
        assertThat(values.field("C1").schema()).isEqualTo(SchemaBuilder.string().build());
        assertThat(values.field("C2").name()).isEqualTo("C2");
        assertThat(values.field("C2").index()).isEqualTo(1);
        assertThat(values.field("C2").schema()).isEqualTo(Decimal.builder(3).parameter("connect.decimal.precision", "5").optional().build()); // scale of 3
        assertThat(values.field("C3").name()).isEqualTo("C3");
        assertThat(values.field("C3").index()).isEqualTo(2);
        assertThat(values.field("C3").schema()).isEqualTo(Date.builder().optional().build()); // optional date
        assertThat(values.field("C4").name()).isEqualTo("C4");
        assertThat(values.field("C4").index()).isEqualTo(3);
        assertThat(values.field("C4").schema()).isEqualTo(SchemaBuilder.int32().optional().build()); // JDBC INTEGER = 32 bits
        assertThat(values.field("C5").index()).isEqualTo(4);
        assertThat(values.field("C5").schema()).isEqualTo(SchemaBuilder.bytes().build()); // JDBC BINARY = bytes
        assertThat(values.field("C6").index()).isEqualTo(5);
        assertThat(values.field("C6").schema()).isEqualTo(SchemaBuilder.int16().build());

        // Column starting with digit should be prefixed, original field should not exist
        assertThat(values.field("7C7")).isNull();

        // Column starting with digit is prefixed with _
        assertThat(values.field("_u0037C7").name()).isEqualTo("_u0037C7");
        assertThat(values.field("_u0037C7").index()).isEqualTo(6);
        assertThat(values.field("_u0037C7").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column containing '-' should have '-' replaced with '_', field should not exist
        assertThat(values.field("C-8")).isNull();

        // Column C-8 has - replaced with _
        assertThat(values.field("C_u002d8").name()).isEqualTo("C_u002d8");
        assertThat(values.field("C_u002d8").index()).isEqualTo(7);
        assertThat(values.field("C_u002d8").schema()).isEqualTo(SchemaBuilder.string().build());

        // Column AVRO_UNSUPPORTED_NAME should be all underscores
        assertThat(values.field(AVRO_UNICODE_NAME_CONVERTED).name()).isEqualTo(AVRO_UNICODE_NAME_CONVERTED);
        assertThat(values.field(AVRO_UNICODE_NAME_CONVERTED).index()).isEqualTo(8);
        assertThat(values.field(AVRO_UNICODE_NAME_CONVERTED).schema()).isEqualTo(SchemaBuilder.string().build());

        // Column UP$ID should has $ converted to underscore
        assertThat(values.field("UP_u0024ID").name()).isEqualTo("UP_u0024ID");
        assertThat(values.field("UP_u0024ID").index()).isEqualTo(9);
        assertThat(values.field("UP_u0024ID").schema()).isEqualTo(SchemaBuilder.int32().build());

        Struct value = schema.valueFromColumnData(data);
        assertThat(value).isNotNull();
        assertThat(value.get("C1")).isEqualTo("c1value");
        assertThat(value.get("C2")).isEqualTo(BigDecimal.valueOf(3.142d));
        assertThat(value.get("C3")).isEqualTo(11626);
        assertThat(value.get("C4")).isEqualTo(4);
        assertThat(value.get("C5")).isEqualTo(ByteBuffer.wrap(new byte[]{ 71, 117, 110, 110, 97, 114 }));
        assertThat(value.get("C6")).isEqualTo(Short.valueOf((short) 0));
        assertThat(value.get("_u0037C7")).isEqualTo("c7value");
        assertThat(value.get("C_u002d8")).isEqualTo("c8value");
        assertThat(value.get(AVRO_UNICODE_NAME_CONVERTED)).isEqualTo("c9value");
        assertThat(value.get("UP_u0024ID")).isEqualTo(0);
    }

    @Test
    @FixFor({ "DBZ-1044", "DBZ-2849" })
    public void shouldSanitizeFieldNamesAndValidateSerialization() {
        LogInterceptor logInterceptor = new LogInterceptor(TableSchemaBuilder.class);

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), avroFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);

        Struct key = schema.keyFromColumnData(keyData);
        Struct value = schema.valueFromColumnData(data);

        SourceRecord record = new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "sometopic", schema.keySchema(), key, schema.valueSchema(), value);
        VerifyRecord.isValid(record);

        assertThat(logInterceptor.containsErrorMessage("Failed to properly convert data value for 'catalog.schema.table.UP$ID' of type NUMBER"))
                .describedAs("Expected no value conversion failures")
                .isFalse();
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldBuildTableSchemaFromTableWithCustomKey() {
        table = table.edit().setPrimaryKeyNames().create();
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, CustomKeyMapper.getInstance("(.*).table:C2,C3", null));
        assertThat(schema).isNotNull();
        Schema keys = schema.keySchema();
        assertThat(keys).isNotNull();
        assertThat(keys.fields()).hasSize(2);
        assertThat(keys.field("C2").name()).isEqualTo("C2");
        assertThat(keys.field("C3").name()).isEqualTo("C3");
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldOverrideIdentityKey() {
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, CustomKeyMapper.getInstance("(.*).table:C2,C3", null));
        assertThat(schema).isNotNull();
        Schema keys = schema.keySchema();
        assertThat(keys).isNotNull();
        assertThat(keys.fields()).hasSize(2);
        assertThat(keys.field("C1")).isNull();
        assertThat(keys.field("C2").name()).isEqualTo("C2");
        assertThat(keys.field("C3").name()).isEqualTo("C3");
    }

    @Test
    @FixFor("DBZ-1015")
    public void shouldFallbackToIdentyKeyWhenCustomMapperIsNull() {
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        Schema keys = schema.keySchema();
        assertThat(keys).isNotNull();
        assertThat(keys.fields()).hasSize(2);
        assertThat(keys.field("C1").name()).isEqualTo("C1");
        assertThat(keys.field("C2").name()).isEqualTo("C2");
    }

    @Test
    @FixFor("DBZ-1015")
    public void customKeyMapperShouldMapMultipleTables() {
        TableId id2 = new TableId("catalog", "schema", "table2");
        Table table2 = Table.editor()
                .tableId(id2)
                .addColumns(Column.editor().name("C1")
                        .type("VARCHAR").jdbcType(Types.VARCHAR).length(10)
                        .optional(false)
                        .create(),
                        Column.editor().name("C2")
                                .type("NUMBER").jdbcType(Types.NUMERIC).length(5).scale(3)
                                .create(),
                        Column.editor().name("C3")
                                .type("DATE").jdbcType(Types.DATE).length(4)
                                .optional(true)
                                .create())
                .create();

        KeyMapper keyMapper = CustomKeyMapper.getInstance("(.*).table:C2,C3;(.*).table2:C1", null);

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, keyMapper);

        assertThat(schema).isNotNull();
        Schema keys = schema.keySchema();
        assertThat(keys).isNotNull();
        assertThat(keys.fields()).hasSize(2);
        assertThat(keys.field("C1")).isNull();
        assertThat(keys.field("C2").name()).isEqualTo("C2");
        assertThat(keys.field("C3").name()).isEqualTo("C3");

        TableSchema schema2 = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table2, null, null, keyMapper);

        assertThat(schema2).isNotNull();
        Schema key2 = schema2.keySchema();
        assertThat(key2).isNotNull();
        assertThat(key2.fields()).hasSize(1);
        assertThat(key2.field("C1").name()).isEqualTo("C1");
    }

    @Test
    @FixFor("DBZ-1507")
    public void defaultKeyMapperShouldOrderKeyColumnsBasedOnPrimaryKeyColumnNamesOrder() {
        TableId id2 = new TableId("catalog", "schema", "info");
        Table table2 = Table.editor()
                .tableId(id2)
                .addColumns(Column.editor().name("t1ID")
                        .type("INT").jdbcType(Types.INTEGER)
                        .optional(false)
                        .create(),
                        Column.editor().name("t2ID")
                                .type("INT").jdbcType(Types.INTEGER)
                                .optional(false)
                                .create())
                .setPrimaryKeyNames("t2ID", "t1ID")
                .create();

        TableSchema schema2 = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table2, null, null, null);

        Schema key2 = schema2.keySchema();
        assertThat(key2).isNotNull();
        assertThat(key2.fields()).hasSize(2);
        assertThat(key2.fields().get(0).name()).isEqualTo("t2ID");
        assertThat(key2.fields().get(1).name()).isEqualTo("t1ID");
    }

    @Test
    @FixFor("DBZ-2682")
    public void mapperConvertersShouldLeaveEmptyDatesAsZero() {
        TableId id2 = new TableId("catalog", "schema", "table2");
        Table table2 = Table.editor()
                .tableId(id2)
                .addColumns(
                        Column.editor().name("C1")
                                .type("DATE").jdbcType(Types.DATE)
                                .optional(false)
                                .create())
                .create();

        Configuration config = Configuration.create()
                .with("column.truncate.to.65536.chars", id2 + ".C1")
                .build();

        Object[] data = new Object[]{ null };

        ColumnMappers mappers = ColumnMappers.create(new TestRelationalDatabaseConfig(config, null, null, 0));

        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table2, null, mappers, null);

        Struct value = schema.valueFromColumnData(data);
        assertThat(value.get("C1")).isEqualTo(0);
    }

    @Test
    @FixFor("DBZ-6641")
    public void shouldUseDefaultOrCustomTopicNamingStrategySchemaRecordPrefix() {
        // table id with catalog and SchemaTopicNamingStrategy
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(topicNamingStrategy, table, null, null, null);
        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("test.schema.table.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("test.schema.table.Value");
        assertThat(schema.getEnvelopeSchema().schema().name()).isEqualTo("test.schema.table.Envelope");

        // custom topic naming strategy with custom record prefix
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(new CustomTopicNamingStrategy(topicProperties, null, "testSchemaPrefix"), table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("testSchemaPrefix.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("testSchemaPrefix.Value");
        assertThat(schema.getEnvelopeSchema().schema().name()).isEqualTo("test.schema.table.Envelope");

        // custom topic naming strategy with custom data change topic and schema prefix.
        schema = new TableSchemaBuilder(new JdbcValueConverters(), null, adjuster, customConverterRegistry,
                SchemaBuilder.struct().build(), defaultFieldNamer, false,
                EventConvertingFailureHandlingMode.FAIL)
                .create(new CustomTopicNamingStrategy(topicProperties, "testDataTopic", "testSchemaPrefix"), table, null, null, null);

        assertThat(schema).isNotNull();
        assertThat(schema.keySchema().name()).isEqualTo("testSchemaPrefix.Key");
        assertThat(schema.valueSchema().name()).isEqualTo("testSchemaPrefix.Value");
        assertThat(schema.getEnvelopeSchema().schema().name()).isEqualTo("testDataTopic.Envelope");
    }
}
