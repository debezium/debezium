/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Types;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * A {@link ColumnMapper} implementation that ensures that string values are masked.
 * Based on the constructor different masking methods could be used.
 *
 * @author Randall Hauch
 * @author Jan-Hendrik Dolling
 */
public class MaskStrings implements ColumnMapper {

    private final Function<Column, ValueConverter> converterFromColumn;

    /**
     * Create a {@link ColumnMapper} that masks string values with a predefined value.
     *
     * @param maskValue the value that should be used in place of the actual value; may not be null
     * @throws IllegalArgumentException if the {@param maskValue} is null
     */
    public MaskStrings(String maskValue) {
        Objects.requireNonNull(maskValue);
        this.converterFromColumn = ignored -> new MaskingValueConverter(maskValue);
    }

    /**
     * Create a {@link ColumnMapper} that masks string values by hashing the input value.
     * The hash is automatically shortened to the length of the column.
     *
     * @param salt the salt that is used within the hash function
     * @param hashAlgorithm the hash function that is used to mask the columns string values written in source records;
     *                      must be on of Java Cryptography Architecture Standard Algorithm {@link MessageDigest}.
     * @throws IllegalArgumentException if the {@param salt} or {@param hashAlgorithm} are null
     */
    public MaskStrings(byte[] salt, String hashAlgorithm, HashingByteArrayStrategy hashingByteArrayStrategy) {
        Objects.requireNonNull(salt);
        Objects.requireNonNull(hashAlgorithm);
        this.converterFromColumn = column -> {
            final HashValueConverter hashValueConverter = new HashValueConverter(salt, hashAlgorithm, hashingByteArrayStrategy);
            if (column.length() > 0) {
                return hashValueConverter.and(new TruncateStrings.TruncatingValueConverter(column.length()));
            }
            else {
                return hashValueConverter;
            }
        };
    }

    @Override
    public ValueConverter create(Column column) {
        switch (column.jdbcType()) {
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
                return converterFromColumn.apply(column);
            default:
                return ValueConverter.passthrough();
        }
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        schemaBuilder.parameter("masked", "true");
    }

    @Immutable
    protected static final class MaskingValueConverter implements ValueConverter {
        protected final String maskValue;

        public MaskingValueConverter(String maskValue) {
            this.maskValue = maskValue;
            assert this.maskValue != null;
        }

        @Override
        public Object convert(Object value) {
            return maskValue;
        }
    }

    @Immutable
    protected static final class HashValueConverter implements ValueConverter {

        private static final Logger LOGGER = LoggerFactory.getLogger(HashValueConverter.class);
        private final byte[] salt;
        private final MessageDigest hashAlgorithm;
        private final HashingByteArrayStrategy hashingByteArrayStrategy;

        public HashValueConverter(byte[] salt, String hashAlgorithm, HashingByteArrayStrategy hashingByteArrayStrategy) {
            this.salt = salt;
            this.hashingByteArrayStrategy = hashingByteArrayStrategy;
            try {
                this.hashAlgorithm = MessageDigest.getInstance(hashAlgorithm);
            }
            catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public Object convert(Object value) {
            if (value instanceof Serializable) {
                try {
                    return toHash((Serializable) value);
                }
                catch (IOException e) {
                    if (LOGGER.isErrorEnabled()) {
                        LOGGER.error("can't calculate hash", e);
                    }
                }
            }
            return null;
        }

        private String toHash(Serializable value) throws IOException {
            hashAlgorithm.reset();
            hashAlgorithm.update(salt);
            byte[] valueToByteArray = hashingByteArrayStrategy.toByteArray(value);
            return convertToHexadecimalFormat(hashAlgorithm.digest(valueToByteArray));
        }

        private String convertToHexadecimalFormat(byte[] bytes) {
            StringBuilder hashString = new StringBuilder();
            for (byte b : bytes) {
                hashString.append(String.format("%02x", b));
            }
            return hashString.toString();
        }
    }

    /**
     * V1 default and previous version. Because ObjectOutputStream is used, some characters are added before the actual value.
     * V2 should be used to fidelity for the value being hashed the same way in different places. The byte array also has only the actual value.
     *
     */
    public enum HashingByteArrayStrategy {
        V1 {
            @Override
            byte[] toByteArray(Serializable value) throws IOException {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(value);
                return bos.toByteArray();
            }
        },

        V2 {
            @Override
            byte[] toByteArray(Serializable value) {
                return value.toString().getBytes();
            }
        };

        abstract byte[] toByteArray(Serializable value) throws IOException;
    }
}
