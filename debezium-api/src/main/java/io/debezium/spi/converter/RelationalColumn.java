/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.converter;

import java.sql.Types;
import java.util.OptionalInt;

import io.debezium.common.annotation.Incubating;

/**
 * A definition of a converted relational column.
 *
 * @author Randall Hauch
 */
@Incubating
public interface RelationalColumn extends ConvertedField {

    /**
     * Get the {@link Types JDBC type} for this column
     *
     * @return the type constant
     */
    int jdbcType();

    /**
     * Get the database native type for this column
     *
     * @return a type constant for the specific database
     */
    int nativeType();

    /**
     * Get the database-specific name of the column's data type.
     *
     * @return the name of the type
     */
    String typeName();

    /**
     * Get the database-specific complete expression defining the column's data type, including dimensions, length, precision,
     * character sets, constraints, etc.
     *
     * @return the complete type expression
     */
    String typeExpression();

    /**
     * Get the maximum length of this column's values. For numeric columns, this represents the precision.
     *
     * @return the length of the column
     */
    OptionalInt length();

    /**
     * Get the scale of the column.
     *
     * @return the scale if it applies to this type
     */
    OptionalInt scale();

    /**
     * Determine whether this column is optional.
     *
     * @return {@code true} if it is optional, or {@code false} otherwise
     */
    boolean isOptional();

    /**
     * Get the default value of the column
     *
     * @return the default value
     */
    Object defaultValue();

    /**
     * Determine whether this column's has a default value
     *
     * @return {@code true} if the default value was provided, or {@code false} otherwise
     */
    boolean hasDefaultValue();

    /**
     * Get the character set associated with the column.
     *
     * @return the character set name
     */
    default String charsetName() {
        return null;
    }
}
