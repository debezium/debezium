/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr;

import io.debezium.relational.TableId;
import io.debezium.text.ParsingException;

/**
 * Raised by the DDL parser when a captured table is created with {@code CREATE TABLE ... AS SELECT}.
 * <p>
 * The statement does not name the columns of the table it creates, so the parser cannot build the
 * relational model from it. The caller resolves the table's definition from the database as of the
 * statement's commit and parses that instead, or treats the statement as unparsable when it cannot.
 *
 * @author Chris Cranford
 */
public class CreateTableAsSelectException extends ParsingException {

    private final TableId tableId;

    public CreateTableAsSelectException(TableId tableId) {
        super(null, "Table " + tableId + " is created with CREATE TABLE AS SELECT, which does not describe its columns");
        this.tableId = tableId;
    }

    /**
     * @return the table the statement creates, never {@code null}
     */
    public TableId getTableId() {
        return tableId;
    }
}
