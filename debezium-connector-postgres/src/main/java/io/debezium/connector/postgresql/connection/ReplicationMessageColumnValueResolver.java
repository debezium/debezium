/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationMessage.ColumnValue;

/**
 * @author Chris Cranford
 */
public class ReplicationMessageColumnValueResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationMessageColumnValueResolver.class);

    /**
     * Resolve the value of a {@link ColumnValue}.
     *
     * @param columnName the column name
     * @param type the postgres type
     * @param fullType the full type-name for the column
     * @param value the column value
     * @param connection a postgres connection supplier
     * @param includeUnknownDatatypes true to include unknown data types, false otherwise
     * @param typeRegistry the postgres type registry
     * @return
     */
    public static Object resolveValue(String columnName, PostgresType type, String fullType, ColumnValue value, final PgConnectionSupplier connection,
                                      boolean includeUnknownDatatypes, TypeRegistry typeRegistry) {
        if (value.isNull()) {
            // nulls are null
            return null;
        }

        if (!type.isRootType()) {
            return resolveValue(columnName, type.getParentType(), fullType, value, connection, includeUnknownDatatypes, typeRegistry);
        }

        if (value.isArray(type)) {
            return value.asArray(columnName, type, fullType, connection);
        }

        if (type.isEnumType()) {
            return value.asString();
        }

        switch (type.getName()) {
            // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
            // plus aliases from the shorter names produced by older wal2json
            case "boolean":
            case "bool":
                return value.asBoolean();

            case "hstore":
                return value.asString();

            case "integer":
            case "int":
            case "int4":
            case "smallint":
            case "int2":
            case "smallserial":
            case "serial":
            case "serial2":
            case "serial4":
                return value.asInteger();

            case "bigint":
            case "bigserial":
            case "int8":
            case "oid":
                return value.asLong();

            case "real":
            case "float4":
                return value.asFloat();

            case "double precision":
            case "float8":
                return value.asDouble();

            case "numeric":
            case "decimal":
                return value.asDecimal();

            case "character":
            case "char":
            case "character varying":
            case "varchar":
            case "bpchar":
            case "text":
                return value.asString();

            case "date":
                return value.asLocalDate();

            case "timestamp with time zone":
            case "timestamptz":
                return value.asOffsetDateTimeAtUtc();

            case "timestamp":
            case "timestamp without time zone":
                return value.asInstant();

            case "time":
                return value.asTime();

            case "time without time zone":
                return value.asLocalTime();

            case "time with time zone":
            case "timetz":
                return value.asOffsetTimeUtc();

            case "bytea":
                return value.asByteArray();

            // these are all PG-specific types and we use the JDBC representations
            // note that, with the exception of point, no converters for these types are implemented yet,
            // i.e. those values won't actually be propagated to the outbound message until that's the case
            case "box":
                return value.asBox();
            case "circle":
                return value.asCircle();
            case "interval":
                return value.asInterval();
            case "line":
                return value.asLine();
            case "lseg":
                return value.asLseg();
            case "money":
                final Object v = value.asMoney();
                return (v instanceof PGmoney) ? ((PGmoney) v).val : v;
            case "path":
                return value.asPath();
            case "point":
                return value.asPoint();
            case "polygon":
                return value.asPolygon();

            // PostGIS types are HexEWKB strings
            // ValueConverter turns them into the correct types
            case "geometry":
            case "geography":
                return value.asString();

            case "citext":
            case "bit":
            case "bit varying":
            case "varbit":
            case "json":
            case "jsonb":
            case "xml":
            case "uuid":
            case "tsrange":
            case "tstzrange":
            case "daterange":
            case "inet":
            case "cidr":
            case "macaddr":
            case "macaddr8":
            case "int4range":
            case "numrange":
            case "int8range":
                return value.asString();

            // catch-all for other known/builtin PG types
            // TODO: improve with more specific/useful classes here?
            case "pg_lsn":
            case "tsquery":
            case "tsvector":
            case "txid_snapshot":
                // catch-all for unknown (extension module/custom) types
            default:
                break;
        }

        return value.asDefault(typeRegistry, type.getOid(), columnName, fullType, includeUnknownDatatypes, connection);
    }
}
