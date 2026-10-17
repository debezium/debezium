/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

/**
 * The values of the {@code lower_case_table_names} system variable, which determines how the server stores and compares
 * database and table names.
 *
 * @see <a href="https://dev.mysql.com/doc/refman/8.4/en/identifier-case-sensitivity.html">Identifier Case Sensitivity</a>
 */
public enum LowerCaseTableNames {

    /**
     * {@code 0}: names are stored as given and compared case-sensitively.
     */
    CASE_SENSITIVE,

    /**
     * {@code 1}: names are stored in lowercase and compared case-insensitively.
     */
    STORED_IN_LOWERCASE,

    /**
     * {@code 2}: names are stored as given and compared case-insensitively.
     */
    CASE_INSENSITIVE;

    /**
     * Parses the value of the {@code lower_case_table_names} system variable. Any value other than {@code 0} or {@code 1},
     * including a missing value, compares names case-insensitively.
     *
     * @param value the value of the system variable; may be null
     * @return the setting; never null
     */
    public static LowerCaseTableNames parse(String value) {
        if ("0".equals(value)) {
            return CASE_SENSITIVE;
        }
        if ("1".equals(value)) {
            return STORED_IN_LOWERCASE;
        }
        return CASE_INSENSITIVE;
    }

    /**
     * @return whether the server compares database and table names case-insensitively
     */
    public boolean isCaseInsensitive() {
        return this != CASE_SENSITIVE;
    }
}
