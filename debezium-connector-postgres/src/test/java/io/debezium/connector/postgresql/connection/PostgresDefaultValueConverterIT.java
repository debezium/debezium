/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static io.debezium.connector.postgresql.TestHelper.defaultJdbcConfig;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.SkipTestRule;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class PostgresDefaultValueConverterIT {
    @Rule
    public final SkipTestRule skipTest = new SkipTestRule();

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    public static JdbcConfiguration getJdbcConfig(RelationalDatabaseConnectorConfig.DecimalHandlingMode mode) {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "postgres")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 5432)
                .withDefault(JdbcConfiguration.USER, "postgres")
                .withDefault(JdbcConfiguration.PASSWORD, "postgres")
                .with(PostgresConnectorConfig.MAX_RETRIES, 2)
                .with(PostgresConnectorConfig.RETRY_DELAY_MS, 2000)
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, mode)
                .build();
    }

    private static final String TEST_SERVER = "test_server";
    private static final String DATABASE_CONFIG_PREFIX = "database.";

    final PostgresConnection postgresConnection = TestHelper.create();
    final PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(defaultJdbcConfig());
    final PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
            postgresConnectorConfig,
            Charset.defaultCharset(),
            new TypeRegistry(postgresConnection));

    @Test
    @FixFor("DBZ-4137")
    public void numericDefaultAsDecimal() {
        final PostgresConnection postgresConnection = TestHelper.create();
        final PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(defaultJdbcConfig());
        final PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                new TypeRegistry(postgresConnection));

        final PostgresDefaultValueConverter postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());

        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValue("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                (String) NumericalColumn.defaultValue());

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-3989")
    public void shouldTrimNumericalDefaultValueAndShouldNotTrimNonNumericalDefaultValue() {
        final PostgresDefaultValueConverter postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());

        final Column NumericalColumn = Column.editor().type("int8").jdbcType(Types.INTEGER).defaultValue(" 1 ").create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                (String) NumericalColumn.defaultValue());

        Assert.assertEquals(numericalConvertedValue, Optional.of(1));

        final Column nonNumericalColumn = Column.editor().type("text").jdbcType(Types.VARCHAR).defaultValue(" 1 ").create();
        final Optional<Object> nonNumericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                nonNumericalColumn,
                (String) nonNumericalColumn.defaultValue());

        Assert.assertEquals(nonNumericalConvertedValue, Optional.of(" 1 "));
    }

}
