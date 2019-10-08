/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;

import io.debezium.relational.SystemVariables;

/**
 * Custom class for MySQL {@link SystemVariables}, which defines MySQL scopes and constants of used variable names.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class MySqlSystemVariables extends SystemVariables {

    public enum MySqlScope implements Scope {

        GLOBAL(2),
        SESSION(1),
        LOCAL(1);

        private int priority;

        MySqlScope(int priority) {
            this.priority = priority;
        }

        @Override
        public int priority() {
            return priority;
        }
    }

    /**
     * The system variable name for the name of the character set that the server uses by default.
     * See http://dev.mysql.com/doc/refman/5.7/en/server-options.html#option_mysqld_character-set-server
     */
    public static final String CHARSET_NAME_SERVER = "character_set_server";

    /**
     * The system variable name fo the name for the character set that the current database uses.
     */
    public static final String CHARSET_NAME_DATABASE = "character_set_database";
    public static final String CHARSET_NAME_CLIENT = "character_set_client";
    public static final String CHARSET_NAME_RESULT = "character_set_results";
    public static final String CHARSET_NAME_CONNECTION = "character_set_connection";

    /**
     * The system variable name to see if the MySQL tables are stored and looked-up in case sensitive way.
     * See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_lower_case_table_names
     */
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

    public MySqlSystemVariables() {
        super(Arrays.asList(MySqlScope.SESSION, MySqlScope.GLOBAL));
    }

    @Override
    protected ConcurrentMap<String, String> forScope(Scope scope) {
        // local and session scope are the same in MySQL
        if (scope == MySqlScope.LOCAL) {
            scope = MySqlScope.SESSION;
        }
        return super.forScope(scope);
    }
}
