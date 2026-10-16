/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

import io.debezium.connector.binlog.util.BinaryLiteralDefaultValueUtils.BinaryLiteral;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/**
 * The hexadecimal and bit-value literal defaults that a CREATE TABLE or ALTER TABLE statement assigns to columns. A string
 * column interprets these bytes in its character set, which is only known at the end of the statement: when a column does
 * not declare one, it uses the character set of its table, which a CREATE TABLE statement declares after the columns, and
 * which an ALTER TABLE statement can change anywhere in the statement. The defaults are therefore converted again at the
 * end of the statement.
 */
public class BinaryLiteralDefaults {

    private final Map<String, BinaryLiteral> literals = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Forgets the defaults of an earlier statement.
     */
    public void clear() {
        literals.clear();
    }

    /**
     * Records the literal default of a column.
     */
    public void put(String columnName, BinaryLiteral literal) {
        literals.put(columnName, literal);
    }

    /**
     * Forgets the literal default of a column, when another default of the column replaces it.
     */
    public void remove(String columnName) {
        literals.remove(columnName);
    }

    /**
     * Keeps the literal default of a column that the statement renames.
     */
    public void rename(String oldColumnName, String newColumnName) {
        final BinaryLiteral literal = literals.remove(oldColumnName);
        if (literal != null) {
            literals.put(newColumnName, literal);
        }
    }

    /**
     * Converts the literal default of a column of a CREATE TABLE statement, once the character set of its table is set.
     */
    public void convert(ColumnEditor column, String tableCharsetName, UnaryOperator<String> javaEncodings) {
        final BinaryLiteral literal = literals.get(column.name());
        if (literal != null) {
            column.defaultValueExpression(BinaryLiteralDefaultValueUtils.toDefaultValueExpression(column, literal, tableCharsetName, javaEncodings));
        }
    }

    /**
     * Converts the literal defaults of the columns of an ALTER TABLE statement, at the end of the statement.
     *
     * @param databaseCharsetName the character set of the database, used if the table has none; may be null
     */
    public void convert(TableEditor table, String databaseCharsetName, UnaryOperator<String> javaEncodings) {
        if (literals.isEmpty()) {
            return;
        }
        String tableCharsetName = table.create().defaultCharsetName();
        if (tableCharsetName == null) {
            tableCharsetName = databaseCharsetName;
        }
        for (Map.Entry<String, BinaryLiteral> entry : literals.entrySet()) {
            final Column column = table.columnWithName(entry.getKey());
            if (column != null) {
                final ColumnEditor columnEditor = column.edit();
                convert(columnEditor, tableCharsetName, javaEncodings);
                table.updateColumn(columnEditor.create());
            }
        }
        literals.clear();
    }
}
