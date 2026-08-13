/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} that provides support for {@code TINYINT} data types.
 *
 * @author Chris Cranford
 */
public class TinyIntType extends AbstractType {

    public static final TinyIntType INSTANCE = new TinyIntType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "TINYINT" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // A propagated source TINYINT that is emitted as an INT16 schema (e.g. SQL Server's
        // unsigned 0-255 TINYINT) does not fit a signed MySQL tinyint (-128 to 127); widen it to
        // smallint. This is checked before the display width, because a propagated width does not
        // change the range of values that the column has to hold. A signed MySQL TINYINT is
        // emitted as INT8 and is unaffected.
        if (schema.type() == Schema.Type.INT16) {
            return "smallint";
        }
        // A column with an explicit display width keeps its tinyint(n) form, e.g. a MySQL TINYINT(n).
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        if (columnSize > 0) {
            return String.format("tinyint(%d)", columnSize);
        }
        return "tinyint";
    }
}
