/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational.ddl;

import io.debezium.relational.Tables;
import io.debezium.text.ParsingException;

/**
 * A parser interface for DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public interface DdlParser {

    /**
     * Examine the supplied string containing DDL statements, and apply those statements to the specified
     * database table definitions.
     *
     * @param ddlContent     the stream of tokens containing the DDL statements; may not be null
     * @param databaseTables the database's table definitions, which should be used by this method to create, change, or remove
     *                       tables as defined in the DDL content; may not be null
     * @throws ParsingException if there is a problem parsing the supplied content
     */
    void parse(String ddlContent, Tables databaseTables);

    void setCurrentDatabase(String databaseName);

    /**
     * Set the name of the current schema.
     *
     * @param schemaName the name of the current schema; may be null
     */
    void setCurrentSchema(String schemaName);

    /**
     * Add a listener. This method should not be called more than once with the same listener object, since the result will be
     * that object will be called multiple times for each event.
     *
     * @param listener the listener; if null nothing is done
     */
    void addListener(DdlParserListener listener);

    /**
     * Remove an existing listener.
     *
     * @param listener the listener; if null nothing is done
     * @return {@code true} if the listener was removed, or {@code false} otherwise
     */
    boolean removeListener(DdlParserListener listener);

    /**
     * Remove all existing listeners.
     */
    void removeListeners();

    /**
     * The token used to terminate a DDL statement.
     *
     * @return the terminating token; never null
     */
    String terminator();
}
