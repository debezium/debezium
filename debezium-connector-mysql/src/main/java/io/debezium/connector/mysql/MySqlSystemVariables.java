/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Encapsulates a set of the MySQL system variables.
 * 
 * @author Randall Hauch
 */
public class MySqlSystemVariables {

    public static enum Scope {
        GLOBAL, SESSION, LOCAL;
    }
    
    /**
     * The system variable name for the name of the character set that the server uses by default.
     * See http://dev.mysql.com/doc/refman/5.7/en/server-options.html#option_mysqld_character-set-server
     */
    public static final String CHARSET_NAME_SERVER = "character_set_server";

    /**
     * The system variable name to see if the MySQL tables are stored and looked-up in case sensitive way.
     * See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_lower_case_table_names
     */
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";

    private final ConcurrentMap<String, String> global = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> session = new ConcurrentHashMap<>();

    /**
     * Create an instance.
     */
    public MySqlSystemVariables() {
    }

    /**
     * Set the variable with the specified scope.
     * 
     * @param scope the variable scope; may be null if the session scope is to be used
     * @param name the name of the variable; may not be null
     * @param value the variable value; may be null if the value for the named variable is to be removed
     * @return this object for method chaining purposes; never null
     */
    public MySqlSystemVariables setVariable(Scope scope, String name, String value) {
        name = variableName(name);
        if (value != null) {
            forScope(scope).put(name, value);
        } else {
            forScope(scope).remove(name);
        }
        return this;
    }

    /**
     * Get the variable with the specified name and scope.
     * 
     * @param name the name of the variable; may not be null
     * @param scope the variable scope; may not be null
     * @return the variable value; may be null if the variable is not currently set
     */
    public String getVariable(String name, Scope scope) {
        name = variableName(name);
        return forScope(scope).get(name);
    }

    /**
     * Get the variable with the specified name, first checking the {@link Scope#SESSION session} (or {@link Scope#LOCAL local})
     * variables and then the {@link Scope#GLOBAL global} variables.
     * 
     * @param name the name of the variable; may not be null
     * @return the variable value; may be null if the variable is not currently set
     */
    public String getVariable(String name) {
        name = variableName(name);
        String value = session.get(name);
        if (value == null) {
            value = global.get(name);
        }
        return value;
    }

    private String variableName(String name) {
        return name.toLowerCase();
    }

    private ConcurrentMap<String, String> forScope(Scope scope) {
        if (scope != null) {
            switch (scope) {
                case GLOBAL:
                    return global;
                case SESSION:
                case LOCAL:
                    return session;
            }
        }
        return session;
    }

}
