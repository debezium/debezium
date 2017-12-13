/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.sql.Types;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PgOid;

/**
 * Extracts type information from replication messages and associates them with each column.
 * The metadata are parsed lazily.
 *
 * @author Jiri Pechanec
 *
 */
public abstract class AbstractReplicationMessageColumn implements ReplicationMessage.Column {

    public static class TypeMetadataImpl implements ReplicationMessage.ColumnTypeMetadata {

        private static final Logger LOGGER = LoggerFactory.getLogger(TypeMetadataImpl.class);

        private static final int[] EMPTY_TYPE_MODIFIERS = {};
        private static final Pattern TYPE_PATTERN = Pattern.compile("^(?<full>(?<base>[^(\\[]+)(?:\\((?<mod>.+)\\))?(?<suffix>.*?))(?<array>\\[\\])?$");
        private static final Pattern TYPEMOD_PATTERN = Pattern.compile("\\s*,\\s*");
            // "text"; "character varying(255)"; "numeric(12,3)"; "geometry(MultiPolygon,4326)"; "timestamp (12) with time zone"; "int[]"

        /**
         * The basic name of the type without constraints
         */
        private final String baseType;

        /**
         * The full name of the type including the constraints
         */
        private final String fullType;

        /**
         * Length of the type, if present
         */
        private Integer length;

        /**
         * Scale of the type, if present
         */
        private Integer scale;

        /**
         * True if the type is array
         */
        private final boolean isArray;

        /**
         * The shortened name of the type how would be reported by JDBC
         */
        private final String normalizedTypeName;

        /**
         * True if the type has not <code>NOT NULL</code> constraint
         */
        private final boolean optional;

        public TypeMetadataImpl(String columnName, String typeWithModifiers, boolean optional) {
            this.optional = optional;
            Matcher m = TYPE_PATTERN.matcher(typeWithModifiers);
            if (!m.matches()) {
                LOGGER.error("Failed to parse columnType for {} '{}'", columnName, typeWithModifiers);
                throw new ConnectException(String.format("Failed to parse columnType '%s' for column %s", typeWithModifiers, columnName));
            }
            String fullType = m.group("full");
            String baseType = m.group("base").trim();
            if (!Objects.toString(m.group("suffix"), "").isEmpty()) {
                baseType = String.join(" ", baseType, m.group("suffix").trim());
            }
            int[] typeModifiers = EMPTY_TYPE_MODIFIERS;
            if (m.group("mod") != null) {
                final String[] typeModifiersStr = TYPEMOD_PATTERN.split(m.group("mod"));
                typeModifiers = new int[typeModifiersStr.length];
                for (int i = 0; i < typeModifiersStr.length; i++) {
                    try {
                        typeModifiers[i] = Integer.parseInt(typeModifiersStr[i]);
                    } catch (NumberFormatException e) {
                        throw new ConnectException(String.format("Failed to parse type modifier '%s' for column %s", typeModifiersStr[i], columnName));
                    }
                }
            }
            boolean isArray = m.group("array") != null;

            if (baseType.startsWith("_")) {
                // old-style type specifiers use an _ prefix for arrays
                // e.g. int4[] would be "_int4"
                baseType = baseType.substring(1);
                fullType = fullType.substring(1);
                isArray = true;
            }
            String normalizedTypeName = PgOid.normalizeTypeName(baseType);

            if (isArray) {
                normalizedTypeName = "_" + normalizedTypeName;
            }
            this.baseType = baseType;
            this.fullType = fullType;
            this.normalizedTypeName = normalizedTypeName;

            if (typeModifiers.length > 0) {
                this.length = typeModifiers[0];
            }
            if (typeModifiers.length > 1) {
                this.scale = typeModifiers[1];
            }

            this.isArray = isArray;
        }

        public String getBaseType() {
            return baseType;
        }

        public String getFullType() {
            return fullType;
        }

        @Override
        public OptionalInt getLength() {
            return length != null ? OptionalInt.of(length) : OptionalInt.empty();
        }

        @Override
        public OptionalInt getScale() {
            return scale != null ? OptionalInt.of(scale) : OptionalInt.empty();
        }

        @Override
        public boolean isArray() {
            return isArray;
        }

        @Override
        public String getName() {
            return normalizedTypeName;
        }

        public boolean isOptional() {
            return optional;
        }
    }

    private final String columnName;
    private final String typeWithModifiers;
    private final boolean optional;
    private TypeMetadataImpl typeMetadata;
    private final boolean hasMetadata;

    public AbstractReplicationMessageColumn(String columnName, String typeWithModifiers, boolean optional, boolean hasMetadata) {
        super();
        this.columnName = columnName;
        this.typeWithModifiers = typeWithModifiers;
        this.optional = optional;
        this.hasMetadata = hasMetadata;
    }

    private void initMetadata() {
        assert hasMetadata : "Metadata not available";
        typeMetadata = new TypeMetadataImpl(columnName, typeWithModifiers, optional);
    }

    /**
     * @return OID value of the type; if this is an array column, {@link Types#ARRAY}} will be returned.
     */
    @Override
    public int getOidType() {
        if (hasMetadata) {
            initMetadata();
            return typeMetadata.isArray() ? Types.ARRAY : doGetOidType();
        }
        return doGetOidType();
    }

    /**
     * @return OID type of elements for arrays
     */
    @Override
    public int getComponentOidType() {
        initMetadata();
        assert typeMetadata.isArray();
        return doGetOidType();
    }

    @Override
    public String getName() {
        return columnName;
    }

    /**
     * @return true if the column is optional
     */
    @Override
    public boolean isOptional() {
        return optional;
    }

    protected abstract int doGetOidType();

    @Override
    public TypeMetadataImpl getTypeMetadata() {
        initMetadata();
        return typeMetadata;
    }
}
