/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static io.debezium.relational.RelationalDatabaseConnectorConfig.SCHEMA_EXCLUDE_LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.data.Ltree;
import io.debezium.data.Bits;
import io.debezium.data.Json;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.data.Xml;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Strings;

/**
 * Unit test for {@link PostgresSchema}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresSchemaIT {

    @Rule
    public final SkipTestRule skipTest = new SkipTestRule();

    private static final String[] TEST_TABLES = new String[]{ "public.numeric_table", "public.numeric_decimal_table", "public.string_table",
            "public.cash_table", "public.bitbin_table", "public.network_address_table",
            "public.cidr_network_address_table", "public.macaddr_table",
            "public.time_table", "public.text_table", "public.geom_table", "public.range_table",
            "public.array_table", "\"Quoted_\"\" . Schema\".\"Quoted_\"\" . Table\"",
            "public.custom_table"
    };

    private PostgresSchema schema;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Test
    public void shouldLoadSchemaForBuiltinPostgresTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded(TEST_TABLES);
            Arrays.stream(TEST_TABLES).forEach(tableId -> assertKeySchema(tableId, "pk", SchemaBuilder.int32().defaultValue(0).build()));
            assertTableSchema("public.numeric_table", "si, i, bi, r, db, ss, bs, b",
                    Schema.OPTIONAL_INT16_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_FLOAT32_SCHEMA,
                    Schema.OPTIONAL_FLOAT64_SCHEMA, SchemaBuilder.int16().defaultValue((short) 0).build(),
                    SchemaBuilder.int64().defaultValue(0L).build(), Schema.OPTIONAL_BOOLEAN_SCHEMA);
            assertTableSchema("public.numeric_decimal_table", "d, dzs, dvs, n, nzs, nvs",
                    Decimal.builder(2).parameter(TestHelper.PRECISION_PARAMETER_KEY, "3").optional().build(),
                    Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "4").optional().build(),
                    VariableScaleDecimal.builder().optional().build(),
                    Decimal.builder(4).parameter(TestHelper.PRECISION_PARAMETER_KEY, "6").optional().build(),
                    Decimal.builder(0).parameter(TestHelper.PRECISION_PARAMETER_KEY, "4").optional().build(),
                    VariableScaleDecimal.builder().optional().build());
            assertTableSchema("public.string_table", "vc, vcv, ch, c, t, ct",
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA,
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.network_address_table", "i", Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.cidr_network_address_table", "i", Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.macaddr_table", "m", Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.cash_table", "csh", Decimal.builder(2).optional().build());
            assertTableSchema("public.bitbin_table", "ba, bol, bol2, bs, bs7, bv, bvl, bvunlimited1, bvunlimited2",
                    Schema.OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA,
                    Bits.builder(2).optional().build(), Bits.builder(7).optional().build(),
                    Bits.builder(2).optional().build(), Bits.builder(64).optional().build(),
                    Bits.builder(Integer.MAX_VALUE).optional().build(), Bits.builder(Integer.MAX_VALUE).optional().build());
            assertTableSchema("public.time_table", "ts, tz, date, ti, ttz, it",
                    MicroTimestamp.builder().optional().build(), ZonedTimestamp.builder().optional().build(),
                    Date.builder().optional().build(), MicroTime.builder().optional().build(), ZonedTime.builder().optional().build(),
                    MicroDuration.builder().optional().build());
            assertTableSchema("public.text_table", "j, jb, x, u",
                    Json.builder().optional().build(), Json.builder().optional().build(), Xml.builder().optional().build(),
                    Uuid.builder().optional().build());
            assertTableSchema("public.geom_table", "p", Point.builder().optional().build());
            assertTableSchema("public.range_table", "unbounded_exclusive_tsrange, bounded_inclusive_tsrange," +
                    "unbounded_exclusive_tstzrange, bounded_inclusive_tstzrange," +
                    "unbounded_exclusive_daterange, bounded_exclusive_daterange, int4_number_range, numerange, int8_number_range",
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA,
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA,
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.array_table", "int_array, bigint_array, text_array",
                    SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(), SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
            assertTableSchema("\"Quoted_\"\" . Schema\".\"Quoted_\"\" . Table\"", "\"Quoted_\"\" . Text_Column\"",
                    Schema.OPTIONAL_STRING_SCHEMA);
            assertTableSchema("public.custom_table", "lt", Ltree.builder().optional().build());
        }
    }

    @Test
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 10, reason = "MACADDR8 type is only supported on Postgres 10+")
    @FixFor("DBZ-1193")
    public void shouldLoadSchemaForMacaddr8PostgresType() throws Exception {
        String tableId = "public.macaddr8_table";
        String ddl = "CREATE TABLE macaddr8_table (pk SERIAL, m MACADDR8, PRIMARY KEY(pk));";

        TestHelper.execute(ddl);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded(tableId);
            assertKeySchema(tableId, "pk", SchemaBuilder.int32().defaultValue(0).build());
            assertTableSchema(tableId, "m", Schema.OPTIONAL_STRING_SCHEMA);
        }
    }

    @Test
    public void shouldLoadSchemaForExtensionPostgresTypes() throws Exception {
        TestHelper.executeDDL("postgres_create_tables.ddl");
        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig().with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, true).build());

        schema = TestHelper.getSchema(config);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded(TEST_TABLES);
            assertTableSchema("public.custom_table", "lt", Ltree.builder().optional().build());
            assertTableSchema("public.custom_table", "i", Schema.STRING_SCHEMA);
        }
    }

    @Test
    public void shouldLoadSchemaForPostgisTypes() throws Exception {
        TestHelper.executeDDL("init_postgis.ddl");
        TestHelper.executeDDL("postgis_create_tables.ddl");
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            final String[] testTables = new String[]{ "public.postgis_table" };
            assertTablesIncluded(testTables);
            Arrays.stream(testTables).forEach(tableId -> assertKeySchema(tableId, "pk", SchemaBuilder.int32().defaultValue(0).build()));

            assertTableSchema("public.postgis_table", "p, ml",
                    Geometry.builder().optional().build(), Geography.builder().optional().build());
        }
    }

    @Test
    public void shouldApplyFilters() throws Exception {
        String statements = "CREATE SCHEMA s1; " +
                "CREATE SCHEMA s2; " +
                "DROP TABLE IF EXISTS s1.A;" +
                "DROP TABLE IF EXISTS s1.B;" +
                "DROP TABLE IF EXISTS s2.A;" +
                "DROP TABLE IF EXISTS s2.B;" +
                "CREATE TABLE s1.A (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s1.B (pk SERIAL, ba integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.A (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                "CREATE TABLE s2.B (pk SERIAL, ba integer, PRIMARY KEY(pk));";
        TestHelper.execute(statements);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(SCHEMA_EXCLUDE_LIST, "s1").build());
        final TypeRegistry typeRegistry = TestHelper.getTypeRegistry();
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded("s2.a", "s2.b");
            assertTablesExcluded("s1.a", "s1.b");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(SCHEMA_EXCLUDE_LIST, "s.*").build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.create()) {
            schema.refresh(connection, false);
            assertTablesExcluded("s1.a", "s2.a", "s1.b", "s2.b");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, "s1.A,s2.A").build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded("s1.b", "s2.b");
            assertTablesExcluded("s1.a", "s2.a");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, "s1.A,s2.A").build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded("s1.b", "s2.b");
            assertTablesExcluded("s1.a", "s2.a");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(SCHEMA_EXCLUDE_LIST, "s2")
                .with(PostgresConnectorConfig.TABLE_EXCLUDE_LIST, "s1.A")
                .build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded("s1.b");
            assertTablesExcluded("s1.a", "s2.a", "s2.b");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(PostgresConnectorConfig.COLUMN_EXCLUDE_LIST, ".*aa")
                .build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertColumnsExcluded("s1.a.aa", "s2.a.aa");
        }

        config = new PostgresConnectorConfig(TestHelper.defaultConfig().with(PostgresConnectorConfig.COLUMN_INCLUDE_LIST, ".*bb")
                .build());
        schema = TestHelper.getSchema(config, typeRegistry);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertColumnsExcluded("s1.a.aa", "s2.a.aa");
        }
    }

    @Test
    public void shouldDetectNewChangesAfterRefreshing() throws Exception {
        String statements = "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS table1;" +
                "CREATE TABLE table1 (pk SERIAL,  PRIMARY KEY(pk));";
        TestHelper.execute(statements);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded("public.table1");
        }
        statements = "DROP TABLE IF EXISTS table1;" +
                "DROP TABLE IF EXISTS table2;" +
                "CREATE TABLE table2 (pk SERIAL, strcol VARCHAR, PRIMARY KEY(pk));";
        TestHelper.execute(statements);
        String tableId = "public.table2";
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            assertTablesIncluded(tableId);
            assertTablesExcluded("public.table1");
            assertTableSchema(tableId, "strcol", Schema.OPTIONAL_STRING_SCHEMA);
        }

        statements = "ALTER TABLE table2 ADD COLUMN vc VARCHAR(2);" +
                "ALTER TABLE table2 ADD COLUMN si SMALLINT;" +
                "ALTER TABLE table2 DROP COLUMN strcol;";

        TestHelper.execute(statements);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, TableId.parse(tableId, false), false);
            assertTablesIncluded(tableId);
            assertTablesExcluded("public.table1");
            assertTableSchema(tableId, "vc, si",
                    Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA);
            assertColumnsExcluded(tableId + ".strcol");
        }
    }

    @Test
    public void shouldPopulateToastableColumnsCache() throws Exception {
        String statements = "CREATE SCHEMA IF NOT EXISTS public;" +
                "DROP TABLE IF EXISTS table1;" +
                "CREATE TABLE table1 (pk SERIAL,  toasted text, untoasted int, PRIMARY KEY(pk));";
        TestHelper.execute(statements);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);
        TableId tableId = TableId.parse("public.table1", false);

        // Before refreshing, we should have an empty array for the table
        assertTrue(schema.getToastableColumnsForTableId(tableId).isEmpty());

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            // Load up initial schema info. This should not populate the toastable columns cache, as the cache is loaded
            // on-demand per-table refresh.
            schema.refresh(connection, false);
            assertTrue(schema.getToastableColumnsForTableId(tableId).isEmpty());

            // After refreshing w/ toastable column refresh disabled, we should still have an empty array
            schema.refresh(connection, tableId, false);
            assertTrue(schema.getToastableColumnsForTableId(tableId).isEmpty());

            // After refreshing w/ toastable column refresh enabled, we should have only the 'toasted' column in the cache
            schema.refresh(connection, tableId, true);
            assertThat(schema.getToastableColumnsForTableId(tableId)).containsOnly("toasted");
        }
    }

    @Test
    public void shouldProperlyGetDefaultColumnValues() throws Exception {
        String ddl = "DROP TABLE IF EXISTS default_column_test; CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"; CREATE TABLE default_column_test (" +
                "pk SERIAL, " +
                "ss SMALLSERIAL, " +
                "bs BIGSERIAL, " +
                "bigint BIGINT default 9223372036854775807, " +
                "bit_as_boolean BIT(1) default B'1', " +
                "bit BIT(2) default B'11', " +
                "varbit VARBIT(5) default B'110', " +
                "boolean BOOLEAN not null default TRUE, " +
                // box
                // bytea
                "char CHAR(10) default 'abcd', " +
                "varchar VARCHAR(100) default 'abcde', " +
                // cidr
                "date DATE default '2021-03-19'::date, " +
                "date_func DATE default NOW()::date, " +
                "double float8 default 123456789.1234567890123, " +
                // inet
                "integer INT default 2147483647, " +
                "integer_func1 INT default ABS(-1), " +
                "integer_func2 INT default DIV(2, 1), " +
                "integer_opt INT, " +
                "interval INTERVAL default INTERVAL '1 hour', " +
                "interval_func1 INTERVAL default make_interval(hours := 1), " +
                "json JSON default '{}', " +
                "json_opt JSON, " +
                "jsonb JSONB default '{}', " +
                // line
                // lseg
                // macaddr
                // macaddr8
                // money
                "numeric NUMERIC(10, 5) default 12345.67891, " +
                "numeric_var NUMERIC default 0, " +
                // path
                // pg_lsn
                // point
                // polygon
                "real FLOAT4 default 1234567890.5, " +
                "smallint INT2 default 32767, " +
                "text TEXT default 'asdf', " +
                "text_arr_empty_default text[] DEFAULT '{}'::text[]," +
                "text_arr_nonempty_default text[] DEFAULT '{a,b}'::text[]," +
                "varchar_arr_empty_default text[] DEFAULT '{}'::varchar[]," +
                "varchar_arr_nonempty_default text[] DEFAULT '{a,b}'::varchar[]," +
                "text_parens TEXT default 'text(parens)'," +
                "text_func3 TEXT default concat('foo', 'bar', 'baz'), " +
                "time_hm TIME default '12:34'::time, " +
                "time_hms TIME default '12:34:56'::time, " +
                "time_func TIME default NOW()::time, " +
                // time with time zone
                "timestamp TIMESTAMP default '2021-03-20 13:44:28'::timestamp, " +
                "timestamp_func TIMESTAMP default NOW()::timestamp, " +
                "timestamp_opt TIMESTAMP, " +
                "timestamptz TIMESTAMPTZ default '2021-03-20 14:44:28 +1'::timestamptz, " +
                "timestamptz_func TIMESTAMPTZ default NOW()::timestamptz, " +
                "timestamptz_opt TIMESTAMPTZ, " +
                // tsquery
                // tsvector
                // txid_snapshot
                "uuid UUID default '76019d1a-ad2e-4b22-96e9-1a6d6543c818'::uuid, " +
                "uuid_func UUID default uuid_generate_v4(), " +
                "uuid_opt UUID, " +
                "xml XML default '<foo>bar</foo>'" +
                ");";

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);

        final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder = (typeRegistry) -> PostgresValueConverter.of(
                config,
                TestHelper.getDatabaseCharset(),
                typeRegistry);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {

            PostgresDefaultValueConverter defaultValueConverter = connection.getDefaultValueConverter();

            connection.execute(ddl);
            schema.refresh(connection, false);

            List<Column> columns = tableFor("public.default_column_test").columns();
            assertColumnDefault("pk", 0, columns, defaultValueConverter);
            assertColumnDefault("ss", (short) 0, columns, defaultValueConverter);
            assertColumnDefault("bs", 0L, columns, defaultValueConverter);
            assertColumnDefault("bigint", 9223372036854775807L, columns, defaultValueConverter);
            assertColumnDefault("bit_as_boolean", true, columns, defaultValueConverter);
            assertColumnDefault("bit", new byte[]{ 3 }, columns, defaultValueConverter);
            assertColumnDefault("varbit", new byte[]{ 6 }, columns, defaultValueConverter);
            assertColumnDefault("boolean", true, columns, defaultValueConverter);
            assertColumnDefault("char", "abcd", columns, defaultValueConverter);
            assertColumnDefault("varchar", "abcde", columns, defaultValueConverter);

            assertColumnDefault("date", (int) LocalDate.of(2021, 3, 19).toEpochDay(), columns, defaultValueConverter);
            assertColumnDefault("date_func", 0, columns, defaultValueConverter);

            assertColumnDefault("double", 123456789.1234567890123, columns, defaultValueConverter);
            assertColumnDefault("integer", 2147483647, columns, defaultValueConverter);
            assertColumnDefault("integer_func1", 0, columns, defaultValueConverter);
            assertColumnDefault("integer_func2", 0, columns, defaultValueConverter);
            assertColumnDefault("integer_opt", null, columns, defaultValueConverter);

            assertColumnDefault("interval", TimeUnit.HOURS.toMicros(1), columns, defaultValueConverter);
            assertColumnDefault("interval_func1", 0L, columns, defaultValueConverter);

            assertColumnDefault("json", "{}", columns, defaultValueConverter);
            assertColumnDefault("json_opt", null, columns, defaultValueConverter);
            assertColumnDefault("jsonb", "{}", columns, defaultValueConverter);

            assertColumnDefault("numeric", new BigDecimal("12345.67891"), columns, defaultValueConverter);
            // KAFKA-12694: default value for Struct currently exported as null
            assertColumnDefault("numeric_var", null, columns, defaultValueConverter);
            assertColumnDefault("real", 1234567890.5f, columns, defaultValueConverter);
            assertColumnDefault("smallint", (short) 32767, columns, defaultValueConverter);

            assertColumnDefault("text", "asdf", columns, defaultValueConverter);
            assertColumnDefault("text_arr_empty_default", new ArrayList<>(), columns, defaultValueConverter);
            assertColumnDefault("text_arr_nonempty_default", Arrays.asList("a", "b"), columns, defaultValueConverter);
            assertColumnDefault("varchar_arr_empty_default", new ArrayList<>(), columns, defaultValueConverter);
            assertColumnDefault("varchar_arr_nonempty_default", Arrays.asList("a", "b"), columns, defaultValueConverter);
            assertColumnDefault("text_parens", "text(parens)", columns, defaultValueConverter);
            assertColumnDefault("text_func3", "", columns, defaultValueConverter);

            assertColumnDefault("time_hm", TimeUnit.SECONDS.toMicros(LocalTime.of(12, 34).toSecondOfDay()), columns, defaultValueConverter);
            assertColumnDefault("time_hms", TimeUnit.SECONDS.toMicros(LocalTime.of(12, 34, 56).toSecondOfDay()), columns, defaultValueConverter);
            assertColumnDefault("time_func", 0L, columns, defaultValueConverter);
            assertColumnDefault("timestamp", TimeUnit.SECONDS.toMicros(1616247868), columns, defaultValueConverter);
            assertColumnDefault("timestamp_func", 0L, columns, defaultValueConverter);
            assertColumnDefault("timestamp_opt", null, columns, defaultValueConverter);
            assertColumnDefault("timestamptz", "2021-03-20T13:44:28.000000Z", columns, defaultValueConverter);
            assertColumnDefault("timestamptz_func", "1970-01-01T00:00:00.000000Z", columns, defaultValueConverter);
            assertColumnDefault("timestamptz_opt", null, columns, defaultValueConverter);

            assertColumnDefault("uuid", "76019d1a-ad2e-4b22-96e9-1a6d6543c818", columns, defaultValueConverter);
            assertColumnDefault("uuid_func", "00000000-0000-0000-0000-000000000000", columns, defaultValueConverter);
            assertColumnDefault("uuid_opt", null, columns, defaultValueConverter);
            assertColumnDefault("xml", "<foo>bar</foo>", columns, defaultValueConverter);
        }
    }

    @Test
    @FixFor("DBZ-5398")
    public void shouldLoadSchemaForUniqueIndexIncludingFunction() throws Exception {
        String statements = "CREATE SCHEMA IF NOT EXISTS public;"
                + "DROP TABLE IF EXISTS counter;"
                + "CREATE TABLE counter(\n"
                + "  campaign_id   text not null,\n"
                + "  group_id      text,\n"
                + "  sent_cnt      integer   default 0,\n"
                + "  time_sent_cnt integer   default 0,\n"
                + "  last_sent_dt  timestamp default LOCALTIMESTAMP,\n"
                + "  emd_ins_dt    timestamp default LOCALTIMESTAMP not null,\n"
                + "  emd_upd_dt    timestamp\n"
                + ");\n"
                + "create unique index uk_including_function on counter(campaign_id, COALESCE(group_id, ''::text));";

        TestHelper.execute(statements);
        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        schema = TestHelper.getSchema(config);
        TableId tableId = TableId.parse("public.counter", false);

        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            Table table = schema.tableFor(tableId);
            assertThat(table).isNotNull();
            assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        }

        statements = "drop index uk_including_function;"
                + "create unique index uk_including_expression on counter((campaign_id),(sent_cnt/ 2));";
        TestHelper.execute(statements);
        try (PostgresConnection connection = TestHelper.createWithTypeRegistry()) {
            schema.refresh(connection, false);
            Table table = schema.tableFor(tableId);
            assertThat(table).isNotNull();
            assertThat(table.primaryKeyColumnNames().size()).isEqualTo(0);
        }
    }

    private void assertColumnDefault(String columnName, Object expectedDefault, List<Column> columns, PostgresDefaultValueConverter defaultValueConverter) {
        Column column = columns.stream().filter(c -> c.name().equals(columnName)).findFirst().get();

        Object defaultValue = defaultValueConverter
                .parseDefaultValue(column, column.defaultValueExpression().orElse(null))
                .orElse(null);

        if (expectedDefault instanceof byte[]) {
            byte[] expectedBytes = (byte[]) expectedDefault;
            byte[] defaultBytes = (byte[]) defaultValue;
            assertArrayEquals(expectedBytes, defaultBytes);
        }
        else {
            if (Objects.isNull(defaultValue)) {
                assertTrue(Objects.isNull(expectedDefault));
            }
            else {
                assertTrue(defaultValue.equals(expectedDefault));
            }
        }
    }

    protected void assertKeySchema(String fullyQualifiedTableName, String fields, Schema... expectedSchemas) {
        TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
        Schema keySchema = tableSchema.keySchema();
        assertSchemaContent(keySchema, fields.split(","), expectedSchemas);
    }

    protected void assertTableSchema(String fullyQualifiedTableName, String fields, Schema... expectedSchemas) {
        TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
        Schema valueSchema = tableSchema.valueSchema();
        assertSchemaContent(valueSchema, fields.split(","), expectedSchemas);
    }

    private void assertSchemaContent(Schema actualSchema, String[] fields, Schema[] expectedSchemas) {
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].trim();

            Field field = actualSchema.field(Strings.unquoteIdentifierPart(fieldName));
            assertNotNull(fieldName + " not found in schema", field);
            VerifyRecord.assertConnectSchemasAreEqual(fieldName, field.schema(), expectedSchemas[i]);
        }
    }

    protected void assertTablesIncluded(String... fullyQualifiedTableNames) {
        Arrays.stream(fullyQualifiedTableNames).forEach(fullyQualifiedTableName -> {
            TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
            assertNotNull(fullyQualifiedTableName + " not included", tableSchema);
            assertThat(tableSchema.keySchema().name()).isEqualTo(validFullName(fullyQualifiedTableName, ".Key"));
            assertThat(tableSchema.valueSchema().name()).isEqualTo(validFullName(fullyQualifiedTableName, ".Value"));
        });
    }

    private String validFullName(String proposedName, String suffix) {
        TableId id = TableId.parse(proposedName, false);
        return SchemaNameAdjuster.validFullname(TestHelper.TEST_SERVER + "." + id.schema() + "." + id.table() + suffix);
    }

    protected void assertTablesExcluded(String... fullyQualifiedTableNames) {
        Arrays.stream(fullyQualifiedTableNames).forEach(fullyQualifiedTableName -> {
            assertThat(tableFor(fullyQualifiedTableName)).isNull();
            assertThat(schemaFor(fullyQualifiedTableName)).isNull();
        });
    }

    protected void assertColumnsExcluded(String... columnNames) {
        Arrays.stream(columnNames).forEach(fqColumnName -> {
            int lastDotIdx = fqColumnName.lastIndexOf(".");
            String fullyQualifiedTableName = fqColumnName.substring(0, lastDotIdx);
            String columnName = lastDotIdx > 0 ? fqColumnName.substring(lastDotIdx + 1) : fqColumnName;
            TableSchema tableSchema = schemaFor(fullyQualifiedTableName);
            assertNotNull(fullyQualifiedTableName + " not included", tableSchema);
            Schema valueSchema = tableSchema.valueSchema();
            assertNotNull(fullyQualifiedTableName + ".Value schema not included", valueSchema);
            assertNull(columnName + " not excluded;", valueSchema.field(columnName));
        });
    }

    private Table tableFor(String fqn) {
        return schema.tableFor(TableId.parse(fqn, false));
    }

    protected TableSchema schemaFor(String fqn) {
        Table table = tableFor(fqn);
        return table != null ? schema.schemaFor(table.id()) : null;
    }
}
