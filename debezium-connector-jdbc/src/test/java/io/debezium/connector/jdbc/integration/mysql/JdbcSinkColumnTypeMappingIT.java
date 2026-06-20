/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.integration.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.integration.AbstractJdbcSinkTest;
import io.debezium.connector.jdbc.junit.jupiter.MySqlSinkDatabaseContextProvider;
import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.data.Bits;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.doc.FixFor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;

/**
 * Column type mappings for MySQL
 *
 * @author Chris Cranford
 */
@Tag("all")
@Tag("it")
@Tag("it-mysql")
@ExtendWith(MySqlSinkDatabaseContextProvider.class)
public class JdbcSinkColumnTypeMappingIT extends AbstractJdbcSinkTest {

    private static final BigDecimal UNSIGNED_64BIT_MAX_VALUE = new BigDecimal("18446744073709551615");

    public JdbcSinkColumnTypeMappingIT(Sink sink) {
        super(sink);
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-6967")
    public void testShouldCoerceNioByteBufferTypeToByteArrayColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);

        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        JdbcSinkConnectorConfig config = getConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                "data",
                Schema.OPTIONAL_BYTES_SCHEMA,
                buffer,
                config);

        final String destinationTable = destinationTableName(createRecord);
        final String sql = "CREATE TABLE %s (id int not null, data binary(3), primary key(id))";
        getSink().execute(String.format(sql, destinationTable));

        consume(createRecord);

        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(1)).isEqualTo(1);
            assertThat(rs.getBytes(2)).isEqualTo(new byte[]{ 1, 2, 3 });
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2102")
    public void testShouldCreateAndWriteUnsignedBigIntColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema unsignedBigIntSchema = SchemaBuilder.int64()
                .optional()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .build();
        final Schema preciseUnsignedBigIntSchema = Decimal.builder(0)
                .optional()
                .parameter("__debezium.source.column.type", "BIGINT UNSIGNED")
                .parameter("connect.decimal.precision", "20")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("data", "precise_data"),
                List.of(unsignedBigIntSchema, preciseUnsignedBigIntSchema),
                List.of(-1L, UNSIGNED_64BIT_MAX_VALUE),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data", "BIGINT UNSIGNED");
        getSink().assertColumn(destinationTable, "precise_data", "BIGINT UNSIGNED");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getBigDecimal(2)).isEqualTo(UNSIGNED_64BIT_MAX_VALUE);
            assertThat(rs.getBigDecimal(3)).isEqualTo(UNSIGNED_64BIT_MAX_VALUE);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2102")
    public void testShouldCreateAndWriteMultiByteBitColumnType(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema bit9Schema = Bits.schema(9);
        final Schema bit8Schema = Bits.schema(8);
        final Schema bit1Schema = Bits.schema(1);
        final Schema bit64Schema = Bits.schema(64);
        final byte[] bit9Value = new byte[]{ 0x55, 0x01 };
        final byte[] bit8HighValue = new byte[]{ (byte) 0xff };
        final byte[] bit64MaxValue = new byte[]{ (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("data_zero", "data_one", "data_bytes", "data_buffer", "data_high", "data_max"),
                List.of(bit1Schema, bit1Schema, bit9Schema, bit9Schema, bit8Schema, bit64Schema),
                List.of(new byte[]{ 0x00 }, new byte[]{ 0x01 }, bit9Value, ByteBuffer.wrap(bit9Value), bit8HighValue, bit64MaxValue),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "data_zero", "BIT", 1);
        getSink().assertColumn(destinationTable, "data_one", "BIT", 1);
        getSink().assertColumn(destinationTable, "data_bytes", "BIT", 9);
        getSink().assertColumn(destinationTable, "data_buffer", "BIT", 9);
        getSink().assertColumn(destinationTable, "data_high", "BIT", 8);
        getSink().assertColumn(destinationTable, "data_max", "BIT", 64);
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getInt(2)).isZero();
            assertThat(rs.getInt(3)).isEqualTo(1);
            assertThat(rs.getInt(4)).isEqualTo(341);
            assertThat(rs.getInt(5)).isEqualTo(341);
            assertThat(rs.getInt(6)).isEqualTo(255);
            assertThat(rs.getBigDecimal(7)).isEqualTo(UNSIGNED_64BIT_MAX_VALUE);
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2102")
    public void testShouldCreateAndWriteEnumAndSetColumnTypesWithEscapedValues(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema enumSchema = Enum.schema(List.of(
                "plain", "a,b", "it's", "back\\slash", "back\\,comma", "ends\\", ""));
        final Schema setSchema = EnumSet.schema(List.of("plain", "it's", "back\\slash", "ends\\"));

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("enum_data", "set_data"),
                List.of(enumSchema, setSchema),
                List.of("a,b", "it's,back\\slash"),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "enum_data", "ENUM");
        getSink().assertColumn(destinationTable, "set_data", "SET");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString("enum_data")).isEqualTo("a,b");
            assertThat(rs.getString("set_data")).isEqualTo("it's,back\\slash");
            return null;
        });
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("debezium/dbz#2119")
    public void testShouldCreateAndWriteStructuredTemporalColumnTypes(SinkRecordFactory factory) throws Exception {
        final Map<String, String> properties = getDefaultSinkConfig();
        properties.put(JdbcSinkConnectorConfig.SCHEMA_EVOLUTION, JdbcSinkConnectorConfig.SchemaEvolutionMode.BASIC.getValue());
        properties.put(JdbcSinkConnectorConfig.PRIMARY_KEY_MODE, JdbcSinkConnectorConfig.PrimaryKeyMode.RECORD_KEY.getValue());
        properties.put(JdbcSinkConnectorConfig.INSERT_MODE, JdbcSinkConnectorConfig.InsertMode.UPSERT.getValue());
        startSinkConnector(properties);
        assertSinkConnectorIsRunning();

        final String tableName = randomTableName();
        final String topicName = topicName("server2", "schema", tableName);
        final Schema dateSchema = StructuredDate.schema();
        final Schema timestampSchema = StructuredTimestamp.builder()
                .parameter("__debezium.source.column.type", "DATETIME")
                .parameter("__debezium.source.column.length", "6")
                .build();
        final Schema durationSchema = StructuredDuration.builder()
                .parameter("__debezium.source.column.type", "TIME")
                .parameter("__debezium.source.column.length", "6")
                .build();

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(properties);
        final JdbcKafkaSinkRecord createRecord = factory.createRecordWithSchemaValue(
                topicName,
                (byte) 1,
                List.of("date_max", "datetime_max", "time_min"),
                List.of(dateSchema, timestampSchema, durationSchema),
                List.of(
                        StructuredDate.from(dateSchema, 9999, 12, 31),
                        StructuredTimestamp.from(timestampSchema, 9999, 12, 31, 23, 59, 59, 999_999_000),
                        StructuredDuration.from(durationSchema, 0, 0, 0, -838, -59, -58, -999_999_000)),
                config);

        final String destinationTable = destinationTableName(createRecord);

        consume(createRecord);

        getSink().assertColumn(destinationTable, "date_max", "DATE");
        getSink().assertColumn(destinationTable, "datetime_max", "DATETIME");
        getSink().assertColumn(destinationTable, "time_min", "TIME");
        getSink().assertRows(destinationTable, rs -> {
            assertThat(rs.getString(2)).isEqualTo("9999-12-31");
            assertThat(rs.getString(3)).isEqualTo("9999-12-31 23:59:59.999999");
            assertThat(rs.getString(4)).isEqualTo("-838:59:58.999999");
            return null;
        });
    }
}
