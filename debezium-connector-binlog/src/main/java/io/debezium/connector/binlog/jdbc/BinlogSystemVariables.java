/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;

import io.debezium.relational.SystemVariables;

/**
 * Binlog-specific connector implementation of {@link SystemVariables}, which defines scopes and constants
 * of used system variable names.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>
 * @author Chris Cranford
 */
public class BinlogSystemVariables extends SystemVariables {

    /**
     * Various defined binlog scopes.
     */
    public enum BinlogScope implements Scope {
        /**
         * Specifies the global scope.
         */
        GLOBAL(2),
        /**
         * Specifies the session scope.
         */
        SESSION(1),
        /**
         * Specifies the local scope.
         */
        LOCAL(1);

        private int priority;

        BinlogScope(int priority) {
            this.priority = priority;
        }

        @Override
        public int priority() {
            return priority;
        }
    }

    /**
     * The system variable name for the name of the character set that the server uses by default.
     *
     * http://dev.mysql.com/doc/refman/8.2/en/server-options.html#option_mysqld_character-set-server
     * https://mariadb.com/kb/en/server-system-variables/#character_set_server
     */
    public static final String CHARSET_NAME_SERVER = "character_set_server";

    /**
     * The system variable name of the character set used by the database.
     *
     * https://dev.mysql.com/doc/refman/8.3/en/server-system-variables.html#sysvar_character_set_database
     * https://mariadb.com/kb/en/server-system-variables/#character_set_database
     */
    public static final String CHARSET_NAME_DATABASE = "character_set_database";

    /**
     * The system variable name of the character set used for queries from the client.
     *
     * https://dev.mysql.com/doc/refman/8.3/en/server-system-variables.html#sysvar_character_set_client
     * https://mariadb.com/kb/en/server-system-variables/#character_set_client
     */
    public static final String CHARSET_NAME_CLIENT = "character_set_client";

    /**
     * The system variable name of the character set used for results and error messages sent to the client.
     *
     * https://dev.mysql.com/doc/refman/8.3/en/server-system-variables.html#sysvar_character_set_results
     * https://mariadb.com/kb/en/server-system-variables/#character_set_results
     */
    public static final String CHARSET_NAME_RESULT = "character_set_results";

    /**
     * The system variable name of the character set used for number to string conversion and literals.
     *
     * https://dev.mysql.com/doc/refman/8.3/en/server-system-variables.html#sysvar_character_set_connection
     * See https://mariadb.com/kb/en/server-system-variables/#character_set_connection
     */
    public static final String CHARSET_NAME_CONNECTION = "character_set_connection";

    /**
     * The system variable name to see if the database tables are stored in a case-sensitive way.
     *
     * https://dev.mysql.com/doc/refman/8.2/en/server-system-variables.html#sysvar_lower_case_table_names
     * https://mariadb.com/kb/en/server-system-variables/#lower_case_table_names
     */
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

    public BinlogSystemVariables() {
        super(Arrays.asList(BinlogScope.SESSION, BinlogScope.GLOBAL));
    }

    @Override
    protected ConcurrentMap<String, String> forScope(Scope scope) {
        // LOCAL and SESSION are the same scope
        if (scope == BinlogScope.LOCAL) {
            scope = BinlogScope.SESSION;
        }
        return super.forScope(scope);
    }
}
