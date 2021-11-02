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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;

public class PostgresDefaultValueConverterIT {

    private PostgresConnection postgresConnection;
    private PostgresDefaultValueConverter postgresDefaultValueConverter;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();

        postgresConnection = TestHelper.create();

        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(defaultJdbcConfig());
        PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                new TypeRegistry(postgresConnection));

        postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());
    }

    @After
    public void closeConnection() {
        if (postgresConnection != null) {
            postgresConnection.close();
        }
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValue() {
        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValue("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                (String) NumericalColumn.defaultValue());

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-4137")
    public void shouldReturnNullForNumericDefaultValueUsingDecimalHandlingModePrecise() {
        Configuration config = defaultJdbcConfig()
                .edit()
                .with(PostgresConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.PRECISE)
                .build();

        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(config);
        PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                new TypeRegistry(postgresConnection));

        PostgresDefaultValueConverter postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils());

        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValue("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                (String) NumericalColumn.defaultValue());

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }
}
