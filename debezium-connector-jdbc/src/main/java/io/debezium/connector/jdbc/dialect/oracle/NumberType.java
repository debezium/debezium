/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} that provides compatibility with other dialect's numeric
 * types to Oracle's numeric type.
 *
 * @author Chris Cranford
 */
public class NumberType extends AbstractType {

    public static final NumberType INSTANCE = new NumberType();

    @Override
    public String[] getRegistrationKeys() {
        // SMALLINT is provided by MySQL
        return new String[]{ "SMALLINT" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        Optional<String> columnType = getSourceColumnType(schema);
        DatabaseDialect dialect = getDialect();
        if (columnType.isPresent()) {
            Integer columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
            if (columnSize > 0) {
                return dialect.getJdbcTypeName(Types.NUMERIC, Size.precision(columnSize, 0));
            }
        }
        // Must explicitly specify (38,0) because Hibernate will otherwise use (38,-1), and
        // this is inaccurate as a negative scale has rounding impacts on Oracle.
        return dialect.getJdbcTypeName(Types.NUMERIC, Size.precision(38, 0));
    }
}
