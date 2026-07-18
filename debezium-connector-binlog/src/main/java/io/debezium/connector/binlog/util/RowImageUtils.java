/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import io.debezium.relational.Column;

/**
 * Utilities for working with binlog row images that may omit columns, e.g. when the database
 * runs with {@code binlog_row_image=NOBLOB}.
 *
 * @author Bue-Von-Hun
 */
public final class RowImageUtils {

    private RowImageUtils() {
    }

    /**
     * Determines whether a column is a BLOB or TEXT family column. This covers all size variants
     * such as {@code TINYBLOB}, {@code MEDIUMBLOB}, {@code LONGBLOB}, {@code TINYTEXT},
     * {@code MEDIUMTEXT} and {@code LONGTEXT}, all of which are excluded from the binlog row image
     * when {@code binlog_row_image=NOBLOB}.
     *
     * @param column the column to check; may not be null
     * @return true if the column is a BLOB or TEXT family column
     */
    public static boolean isBlobOrTextColumn(Column column) {
        final String typeName = column.typeName();
        if (typeName == null) {
            return false;
        }
        final String upperCaseTypeName = typeName.toUpperCase();
        return upperCaseTypeName.contains("BLOB") || upperCaseTypeName.contains("TEXT");
    }
}
