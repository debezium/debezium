/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import java.sql.Types;
import java.time.ZonedDateTime;

import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.ZonedTimeType;

/**
 * An implementation of {@link ZonedTimeType} for SQL Server.
 *
 * @author Chris Cranford
 */
class SqlServerZonedTimeType extends ZonedTimeType {

    public static final SqlServerZonedTimeType INSTANCE = new SqlServerZonedTimeType();

    protected void bindWithNoTimeZoneDetails(Query<?> query, int index, ZonedDateTime zonedDateTime) {
        query.setParameter(index, zonedDateTime.toLocalDateTime());
    }

    @Override
    protected int getJdbcType(DatabaseDialect dialect) {
        // SQL Server does not support time with time zone, but to align the behavior with other dialects,
        // we will directly map to a TIMESTAMP with TIME ZONE so that SQL Server is mapped to DATETIMEOFFSET.
        return Types.TIMESTAMP_WITH_TIMEZONE;
    }

}
