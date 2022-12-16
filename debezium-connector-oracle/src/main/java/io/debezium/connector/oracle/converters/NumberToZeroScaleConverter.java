/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.math.BigDecimal;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

/**
 * Oracle allows for the {@code NUMBER} data type to have negative scale. This can cause issue
 * when converting into Avro format, as Avro specification forbids negative scales.
 * This converter converts sufficiently high number, those which are converted into {@code BigDecimal} type,
 * into {@code BigDecimal} with a zero scale. The drawback of using this converter is losing the information about
 * the number scale, but shouldn't be a big issue in typical case, as negative scale causes rounding of the number
 * when stored in the database, i.e. the value of the number is correct, we just lose the information that the number
 * could be rounded.
 *
 * For completeness the converter supports also other {@link RelationalDatabaseConnectorConfig.DecimalHandlingMode} modes.
 *
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlqr/Data-Types.html">Oracle data types</a>
 * @see <a href="https://avro.apache.org/docs/1.11.1/specification/#decimal">Avro Decimal specification</a>
 *
 * @author vjuranek
 */
public class NumberToZeroScaleConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NumberToZeroScaleConverter.class);

    public static final String DECIMAL_MODE_PROPERTY = "decimal.mode";

    private JdbcValueConverters.DecimalMode decimalMode;

    @Override
    public void configure(Properties props) {
        final String decimalModeConfig = props.getProperty(DECIMAL_MODE_PROPERTY);
        if (!Strings.isNullOrEmpty(decimalModeConfig)) {
            decimalMode = RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(decimalModeConfig).asDecimalMode();
        }
        else {
            decimalMode = RelationalDatabaseConnectorConfig.DecimalHandlingMode.PRECISE.asDecimalMode();
        }
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        int scale = field.scale().orElse(0);
        // Converter applies only for Oracle NUMBER type with negative scale and sufficiently big numbers which we provide as
        // Kafka BigDecimal type. Smaller numbers we provide as INT64 or smaller so there's no need to fix the scale.
        if ("NUMBER".equalsIgnoreCase(field.typeName()) && (scale < 0) && ((field.length().getAsInt() - scale) >= 19)) {
            final SchemaBuilder schemaBuilder = SpecialValueDecimal.builder(decimalMode, field.length().getAsInt(), 0);
            registration.register(schemaBuilder,
                    x -> x == null ? null
                            : SpecialValueDecimal.fromLogical(new SpecialValueDecimal((BigDecimal) x), decimalMode,
                                    field.name()));
        }
    }
}
