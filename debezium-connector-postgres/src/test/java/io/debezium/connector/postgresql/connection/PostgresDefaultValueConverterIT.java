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
import java.util.Arrays;
import java.util.List;
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
    private PostgresValueConverter postgresValueConverter;
    private PostgresDefaultValueConverter postgresDefaultValueConverter;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();

        postgresConnection = TestHelper.create();

        PostgresConnectorConfig postgresConnectorConfig = new PostgresConnectorConfig(defaultJdbcConfig());
        TypeRegistry typeRegistry = new TypeRegistry(postgresConnection);
        postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                typeRegistry);

        postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils(), typeRegistry);
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
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

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
        TypeRegistry typeRegistry = new TypeRegistry(postgresConnection);
        PostgresValueConverter postgresValueConverter = PostgresValueConverter.of(
                postgresConnectorConfig,
                Charset.defaultCharset(),
                typeRegistry);

        PostgresDefaultValueConverter postgresDefaultValueConverter = new PostgresDefaultValueConverter(
                postgresValueConverter, postgresConnection.getTimestampUtils(), typeRegistry);

        final Column NumericalColumn = Column.editor().type("numeric", "numeric(19, 4)")
                .jdbcType(Types.NUMERIC).defaultValueExpression("NULL::numeric").optional(true).create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.empty());
    }

    @Test
    @FixFor("DBZ-3989")
    public void shouldTrimNumericalDefaultValueAndShouldNotTrimNonNumericalDefaultValue() {
        final Column NumericalColumn = Column.editor().type("int8").jdbcType(Types.INTEGER).defaultValueExpression(" 1 ").create();
        final Optional<Object> numericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                NumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(numericalConvertedValue, Optional.of(1));

        final Column nonNumericalColumn = Column.editor().type("text").jdbcType(Types.VARCHAR).defaultValueExpression(" 1 ").create();
        final Optional<Object> nonNumericalConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                nonNumericalColumn,
                NumericalColumn.defaultValueExpression().orElse(null));

        Assert.assertEquals(nonNumericalConvertedValue, Optional.of(" 1 "));
    }

    @Test
    public void shouldWorkOnEmptyAndNonEmptyArrayDefaults() {
        Column textArrayColumn = Column.editor().type("_varchar").jdbcType(Types.ARRAY).defaultValueExpression("{a,b}").create();
        Optional<Object> textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 2);
        Assert.assertEquals(textArrConvertedValue.get(), Arrays.asList("a", "b"));

        textArrayColumn = Column.editor().type("_text").jdbcType(Types.ARRAY).defaultValueExpression("{a,\"b\"}").create();
        textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 2);
        Assert.assertEquals(textArrConvertedValue.get(), Arrays.asList("a", "b"));

        textArrayColumn = Column.editor().type("_text").jdbcType(Types.ARRAY).defaultValueExpression("{}").create();
        textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 0);

        textArrayColumn = Column.editor().type("_varchar").jdbcType(Types.ARRAY).defaultValueExpression("{a,b}").create();
        textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 2);
        Assert.assertEquals(textArrConvertedValue.get(), Arrays.asList("a", "b"));

        textArrayColumn = Column.editor().type("_varchar").jdbcType(Types.ARRAY).defaultValueExpression("{a,\"b\"}").create();
        textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 2);
        Assert.assertEquals(textArrConvertedValue.get(), Arrays.asList("a", "b"));

        textArrayColumn = Column.editor().type("_varchar").jdbcType(Types.ARRAY).defaultValueExpression("{}").create();
        textArrConvertedValue = postgresDefaultValueConverter.parseDefaultValue(
                textArrayColumn,
                textArrayColumn.defaultValueExpression().orElse(null));
        Assert.assertTrue(textArrConvertedValue.isPresent());
        Assert.assertEquals(((List<?>) textArrConvertedValue.get()).size(), 0);
    }

}
