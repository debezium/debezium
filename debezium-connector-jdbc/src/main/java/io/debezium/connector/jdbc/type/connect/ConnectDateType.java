/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import java.sql.Types;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@link Date} values.
 *
 * @author Chris Cranford
 */
public class ConnectDateType extends AbstractType {

    public static final ConnectDateType INSTANCE = new ConnectDateType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Date.LOGICAL_NAME };
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return dialect.getTypeName(Types.DATE);
    }

    @Override
    public String getDefaultValueBinding(DatabaseDialect dialect, Schema schema, Object value) {
        return dialect.getFormattedDate(((java.util.Date) value).toInstant().atZone(ZoneOffset.UTC));
        // final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
        // final String result = formatter.format(((java.util.Date) value).toInstant().atZone(ZoneOffset.UTC));
        // if (dialect instanceof OracleDatabaseDialect) {
        // return String.format("TO_DATE('%s', 'YYYY-MM-DD')", result);
        // }
        // return String.format("'%s'", result);
    }

}
