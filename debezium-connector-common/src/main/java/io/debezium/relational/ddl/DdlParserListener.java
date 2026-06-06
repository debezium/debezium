/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.Optional;

import io.debezium.annotation.Immutable;
import io.debezium.relational.TableId;

/**
 * An interface that can listen to various actions of a {@link DdlParser}. Every kind of {@link Event} has a {@link EventType
 * type} that makes it easier to implement a {@link DdlParserListener} using a {@code switch} statement. However, each kind of
 * {@link Event} also may have additional data associated with it.
 * <p>
 * Clearly not all DDL statements processed by a {@link DdlParser parser} will result in an {@link Event event}.
 *
 * @author Randall Hauch
 */
@FunctionalInterface
public interface DdlParserListener {

    /**
     * Handle a DDL event.
     *
     * @param event the DDL event; never null
     */
    void handle(Event event);

    /**
     * The type of concrete {@link Event}s.
     */
    enum EventType {
        CREATE_TABLE,
        ALTER_TABLE,
        DROP_TABLE,
        TRUNCATE_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        CREATE_DATABASE,
        ALTER_DATABASE,
        DROP_DATABASE,
        USE_DATABASE,
        SET_VARIABLE,
    }

    /**
     * The base class for all concrete events.
     */
    @Immutable
    abstract class Event {
        private final String statement;
        private final EventType type;

        public Event(EventType type, String ddlStatement) {
            this.type = type;
            this.statement = ddlStatement;
        }

        /**
         * Get the {@link EventType type} of event. This is useful when switching on the kind of event.
         * @return the type of event; never null
         */
        public EventType type() {
            return type;
        }

        /**
         * Get the DDL statement associated with this event.
         * @return the DDL statement; never null
         */
        public String statement() {
            return statement;
        }
    }

    /**
     * The base class for all table-related events.
     */
    @Immutable
    abstract class TableEvent extends Event {
        private final TableId tableId;
        private final boolean isView;

        public TableEvent(EventType type, TableId tableId, String ddlStatement, boolean isView) {
            super(type, ddlStatement);
            this.tableId = tableId;
            this.isView = isView;
        }

        /**
         * Get the identifier of the primary table affected by this event.
         * @return the table identifier; never null
         */
        public TableId tableId() {
            return tableId;
        }

        /**
         * Determine whether the target of the event is a view rather than a table.
         * @return {@code true} if the target is a view, or {@code false} if the target is a table
         */
        public boolean isView() {
            return isView;
        }

        @Override
        public String toString() {
            return tableId() + " => " + statement();
        }
    }

    /**
     * An event describing the creation (or replacement) of a table.
     */
    @Immutable
    class TableCreatedEvent extends TableEvent {
        public TableCreatedEvent(TableId tableId, String ddlStatement, boolean isView) {
            super(EventType.CREATE_TABLE, tableId, ddlStatement, isView);
        }
    }

    /**
     * An event describing the altering of a table.
     */
    @Immutable
    class TableAlteredEvent extends TableEvent {
        private final TableId previousTableId;

        public TableAlteredEvent(TableId tableId, TableId previousTableId, String ddlStatement, boolean isView) {
            super(EventType.ALTER_TABLE, tableId, ddlStatement, isView);
            this.previousTableId = previousTableId;
        }

        /**
         * If the table was renamed, then get the old identifier of the table before it was renamed.
         * @return the table's previous identifier; may be null if the alter did not affect the table's identifier
         */
        public TableId previousTableId() {
            return previousTableId;
        }

        @Override
        public String toString() {
            if (previousTableId != null) {
                return tableId() + " (was " + previousTableId() + ") => " + statement();
            }
            return tableId() + " => " + statement();
        }
    }

    /**
     * An event describing the dropping of a table.
     */
    @Immutable
    class TableDroppedEvent extends TableEvent {
        public TableDroppedEvent(TableId tableId, String ddlStatement, boolean isView) {
            super(EventType.DROP_TABLE, tableId, ddlStatement, isView);
        }
    }

    /**
     * An event describing the truncating of a table.
     */
    @Immutable
    class TableTruncatedEvent extends TableEvent {
        public TableTruncatedEvent(TableId tableId, String ddlStatement, boolean isView) {
            super(EventType.TRUNCATE_TABLE, tableId, ddlStatement, isView);
        }
    }

    /**
     * The abstract base class for all index-related events.
     */
    @Immutable
    abstract class TableIndexEvent extends Event {
        private final TableId tableId;
        private final String indexName;

        public TableIndexEvent(EventType type, String indexName, TableId tableId, String ddlStatement) {
            super(type, ddlStatement);
            this.tableId = tableId;
            this.indexName = indexName;
        }

        /**
         * Get the identifier of the table to which the index applies.
         * @return the table identifier; may be null if the index is not scoped to a single table
         */
        public TableId tableId() {
            return tableId;
        }

        /**
         * Get the name of the index affected by this event.
         * @return the index name; never null
         */
        public String indexName() {
            return indexName;
        }

        @Override
        public String toString() {
            if (tableId == null) {
                return indexName() + " => " + statement();
            }
            return indexName() + " on " + tableId() + " => " + statement();
        }
    }

    /**
     * An event describing the creation of an index on a table.
     */
    @Immutable
    class TableIndexCreatedEvent extends TableIndexEvent {
        public TableIndexCreatedEvent(String indexName, TableId tableId, String ddlStatement) {
            super(EventType.CREATE_INDEX, indexName, tableId, ddlStatement);
        }
    }

    /**
     * An event describing the dropping of an index on a table.
     */
    @Immutable
    class TableIndexDroppedEvent extends TableIndexEvent {
        public TableIndexDroppedEvent(String indexName, TableId tableId, String ddlStatement) {
            super(EventType.DROP_INDEX, indexName, tableId, ddlStatement);
        }
    }

    /**
     * The base class for all table-related events.
     */
    @Immutable
    abstract class DatabaseEvent extends Event {
        private final String databaseName;

        public DatabaseEvent(EventType type, String databaseName, String ddlStatement) {
            super(type, ddlStatement);
            this.databaseName = databaseName;
        }

        /**
         * Get the database name affected by this event.
         * @return the database name; never null
         */
        public String databaseName() {
            return databaseName;
        }

        @Override
        public String toString() {
            return databaseName() + " => " + statement();
        }
    }

    /**
     * An event describing the creation of a database.
     */
    @Immutable
    class DatabaseCreatedEvent extends DatabaseEvent {
        public DatabaseCreatedEvent(String databaseName, String ddlStatement) {
            super(EventType.CREATE_DATABASE, databaseName, ddlStatement);
        }
    }

    /**
     * An event describing the altering of a database.
     */
    @Immutable
    class DatabaseAlteredEvent extends DatabaseEvent {
        private final String previousDatabaseName;

        public DatabaseAlteredEvent(String databaseName, String previousDatabaseName, String ddlStatement) {
            super(EventType.ALTER_DATABASE, databaseName, ddlStatement);
            this.previousDatabaseName = previousDatabaseName;
        }

        /**
         * If the table was renamed, then get the old identifier of the table before it was renamed.
         * @return the table's previous identifier; may be null if the alter did not affect the table's identifier
         */
        public String previousDatabaseName() {
            return previousDatabaseName;
        }

        @Override
        public String toString() {
            if (previousDatabaseName != null) {
                return databaseName() + " (was " + previousDatabaseName() + ") => " + statement();
            }
            return databaseName() + " => " + statement();
        }
    }

    /**
     * An event describing the dropping of a database.
     */
    @Immutable
    class DatabaseDroppedEvent extends DatabaseEvent {
        public DatabaseDroppedEvent(String databaseName, String ddlStatement) {
            super(EventType.DROP_DATABASE, databaseName, ddlStatement);
        }
    }

    /**
     * An event describing the switching of a database.
     */
    @Immutable
    class DatabaseSwitchedEvent extends DatabaseEvent {
        public DatabaseSwitchedEvent(String databaseName, String ddlStatement) {
            super(EventType.USE_DATABASE, databaseName, ddlStatement);
        }
    }

    /**
     * An event describing the setting of a variable.
     */
    @Immutable
    class SetVariableEvent extends Event {

        private final String variableName;
        private final String value;
        private final String databaseName;
        private final int order;

        public SetVariableEvent(String variableName, String value, String currentDatabaseName, int order, String ddlStatement) {
            super(EventType.SET_VARIABLE, ddlStatement);
            this.variableName = variableName;
            this.value = value;
            this.databaseName = currentDatabaseName;
            this.order = order;
        }

        /**
         * Get the name of the variable that was set.
         * @return the variable name; never null
         */
        public String variableName() {
            return variableName;
        }

        /**
         * Get the value of the variable that was set.
         * @return the variable value; may be null
         */
        public String variableValue() {
            return value;
        }

        /**
         * In case of multiple vars set in the same SET statement the order of the variable in the statement.
         * @return the variable order
         */
        public int order() {
            return order;
        }

        public Optional<String> databaseName() {
            return Optional.ofNullable(databaseName);
        }

        @Override
        public String toString() {
            return statement();
        }
    }
}
