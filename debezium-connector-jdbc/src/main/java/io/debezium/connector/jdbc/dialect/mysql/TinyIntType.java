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
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        // A propagated source TINYINT emitted as an INT16 schema cannot fit a signed MySQL tinyint
        // (-128 to 127) and must be widened to smallint. The tricky part is that both a MySQL
        // BOOLEAN and SQL Server's unsigned 0-255 TINYINT reach this branch as INT16 while also
        // carrying a propagated display width, so the presence of a width cannot tell them apart -
        // only its value can, and ordering the width check either before or after the INT16 check
        // breaks one of the two cases. A width of exactly 1 is MySQL's tinyint(1) BOOLEAN
        // convention and must be preserved (the MySQL driver reports tinyint(1) as BIT), whereas
        // any other width (e.g. SQL Server's length 3) denotes a real 0-255 value that has to be
        // widened. A signed MySQL TINYINT is emitted as INT8 and never reaches this branch.
        if (schema.type() == Schema.Type.INT16 && columnSize != 1) {
            return "smallint";
        }
        // Keep an explicit display width as tinyint(n): a MySQL TINYINT(n), or the tinyint(1)
        // BOOLEAN preserved by the exception above.
        if (columnSize > 0) {
            return String.format("tinyint(%d)", columnSize);
        }
        return "tinyint";
    }
}
