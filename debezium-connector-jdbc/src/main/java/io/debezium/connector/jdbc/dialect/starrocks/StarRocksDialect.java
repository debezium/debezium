/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.BOOLEAN;
import static org.hibernate.type.SqlTypes.CHAR;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.DOUBLE;
import static org.hibernate.type.SqlTypes.FLOAT;
import static org.hibernate.type.SqlTypes.NCHAR;
import static org.hibernate.type.SqlTypes.NCLOB;
import static org.hibernate.type.SqlTypes.NVARCHAR;
import static org.hibernate.type.SqlTypes.REAL;
import static org.hibernate.type.SqlTypes.TIME;
import static org.hibernate.type.SqlTypes.TIMESTAMP;
import static org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.TIME_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.VARBINARY;
import static org.hibernate.type.SqlTypes.VARCHAR;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.descriptor.sql.internal.CapacityDependentDdlType;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

/**
 * A Hibernate {@link org.hibernate.dialect.Dialect} for StarRocks.
 *
 * StarRocks implements the MySQL wire protocol, so this dialect extends {@link MySQLDialect}
 * (mirroring Hibernate's own {@code TiDBDialect} precedent) and overrides only the column
 * types where StarRocks diverges from MySQL. The version reported by a StarRocks frontend
 * reflects the StarRocks release and must not be interpreted as a MySQL server version, so
 * a fixed MySQL compatibility baseline is used instead.
 */
public class StarRocksDialect extends MySQLDialect {

    private static final DatabaseVersion MYSQL_COMPATIBILITY_VERSION = DatabaseVersion.make(8, 0, 33);

    private static final int MAX_VARCHAR_LENGTH = 1_048_576;

    public StarRocksDialect() {
        super(MYSQL_COMPATIBILITY_VERSION);
    }

    public StarRocksDialect(DialectResolutionInfo info) {
        this();
    }

    @Override
    protected String columnType(int sqlTypeCode) {
        return switch (sqlTypeCode) {
            // StarRocks has a native BOOLEAN type; MySQL maps this to "bit", which StarRocks lacks.
            case BOOLEAN -> "boolean";
            // StarRocks FLOAT and DOUBLE do not accept precision arguments or the
            // "double precision" spelling.
            case FLOAT, REAL -> "float";
            case DOUBLE -> "double";
            // StarRocks only offers DATETIME; there is no TIMESTAMP column type.
            case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> "datetime";
            // StarRocks has no TIME type; times are persisted as ISO-8601 formatted strings.
            case TIME, TIME_WITH_TIMEZONE -> "varchar(18)";
            // StarRocks does not support column-level character sets.
            case NCHAR -> columnType(CHAR);
            case NVARCHAR -> columnType(VARCHAR);
            // StarRocks has no TEXT/BLOB families; a maximum-length VARCHAR/VARBINARY is the
            // largest available representation (the STRING alias only covers 65533 bytes).
            case BLOB -> "varbinary(" + MAX_VARCHAR_LENGTH + ")";
            case CLOB, NCLOB -> "varchar(" + MAX_VARCHAR_LENGTH + ")";
            default -> super.columnType(sqlTypeCode);
        };
    }

    @Override
    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);
        final DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();

        // Replace MySQL's text/blob capacity ladders, which fall back to the tinytext/text/mediumtext
        // and blob families that StarRocks does not support.
        ddlTypeRegistry.addDescriptor(
                CapacityDependentDdlType.builder(VARCHAR, CapacityDependentDdlType.LobKind.BIGGEST_LOB,
                        columnType(CLOB), columnType(CHAR), castType(CHAR), this)
                        .withTypeCapacity(getMaxVarcharLength(), "varchar($l)")
                        .build());
        ddlTypeRegistry.addDescriptor(
                CapacityDependentDdlType.builder(NVARCHAR, CapacityDependentDdlType.LobKind.BIGGEST_LOB,
                        columnType(NCLOB), columnType(NCHAR), castType(NCHAR), this)
                        .withTypeCapacity(getMaxVarcharLength(), "varchar($l)")
                        .build());
        ddlTypeRegistry.addDescriptor(
                CapacityDependentDdlType.builder(VARBINARY, CapacityDependentDdlType.LobKind.BIGGEST_LOB,
                        columnType(BLOB), "binary", "binary", this)
                        .withTypeCapacity(getMaxVarbinaryLength(), "varbinary($l)")
                        .build());
    }

    @Override
    public int getMaxVarcharLength() {
        // StarRocks VARCHAR lengths are expressed in bytes, with an upper bound of 1048576.
        return MAX_VARCHAR_LENGTH;
    }

    @Override
    public int getMaxVarbinaryLength() {
        return MAX_VARCHAR_LENGTH;
    }

    @Override
    public int getDefaultDecimalPrecision() {
        // StarRocks DECIMAL supports a maximum precision of 38.
        return 38;
    }
}
