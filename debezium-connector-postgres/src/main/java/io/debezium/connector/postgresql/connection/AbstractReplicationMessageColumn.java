/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresType;

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

        private static final String[] NO_MODIFIERS = new String[0];

        public static final Pattern TYPE_PATTERN = Pattern
                .compile("^(?<schema>[^\\.\\(]+\\.)?(?<full>(?<base>[^(\\[]+)(?:\\((?<mod>.+)\\))?(?<suffix>.*?))(?<array>\\[\\])?$");
        private static final Pattern TYPEMOD_PATTERN = Pattern.compile("\\s*,\\s*");
        // "text"; "character varying(255)"; "numeric(12,3)"; "geometry(MultiPolygon,4326)"; "timestamp (12) with time zone"; "int[]"; "myschema.geometry"

        /**
         * Length of the type, if present
         */
        private Integer length;

        /**
         * Scale of the type, if present
         */
        private Integer scale;

        /**
         * True if the type has not <code>NOT NULL</code> constraint
         */
        private final boolean optional;

        public TypeMetadataImpl(String columnName, PostgresType type, String typeWithModifiers, boolean optional) {
            this.optional = optional;
            Matcher m = TYPE_PATTERN.matcher(typeWithModifiers);
            if (!m.matches()) {
                LOGGER.error("Failed to parse columnType for {} '{}'", columnName, typeWithModifiers);
                throw new ConnectException(String.format("Failed to parse columnType '%s' for column %s", typeWithModifiers, columnName));
            }

            String[] typeModifiers = m.group("mod") != null ? TYPEMOD_PATTERN.split(m.group("mod")) : NO_MODIFIERS;

            // TODO: make this more elegant/type-specific
            length = type.getDefaultLength();
            scale = type.getDefaultScale();
            if (typeModifiers.length > 0) {
                try {
                    final String typMod = typeModifiers[0];
                    this.length = type.length(Integer.parseInt(typMod));
                    this.scale = type.scale(Integer.parseInt(typMod));
                }
                catch (NumberFormatException e) {
                }
            }

            if (typeModifiers.length > 1) {
                try {
                    this.scale = Integer.parseInt(typeModifiers[1]);
                }
                catch (NumberFormatException e) {
                }
            }
        }

        @Override
        public int getLength() {
            return length;
        }

        @Override
        public int getScale() {
            return scale;
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
    private final PostgresType type;

    public AbstractReplicationMessageColumn(String columnName, PostgresType type, String typeWithModifiers, boolean optional, boolean hasMetadata) {
        super();
        this.columnName = columnName;
        this.type = type;
        this.typeWithModifiers = typeWithModifiers;
        this.optional = optional;
        this.hasMetadata = hasMetadata;
    }

    private void initMetadata() {
        assert hasMetadata : "Metadata not available";
        typeMetadata = new TypeMetadataImpl(columnName, type, typeWithModifiers, optional);
    }

    /**
     * @return the {@link PostgresType} containing both OID and JDBC id.
     */
    @Override
    public PostgresType getType() {
        return type;
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

    @Override
    public TypeMetadataImpl getTypeMetadata() {
        if (typeMetadata == null) {
            initMetadata();
        }
        return typeMetadata;
    }
}
