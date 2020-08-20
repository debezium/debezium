/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.TableId;

/**
 * A {@link DdlParserListener} that accumulates changes, allowing them to be consumed in the same order by database.
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public class DdlChanges implements DdlParserListener {

    private final String terminator;
    private final List<Event> events = new ArrayList<>();
    private final Set<String> databaseNames = new HashSet<>();

    /**
     * Create a new changes object with ';' as the terminator token.
     */
    public DdlChanges() {
        this(null);
    }

    /**
     * Create a new changes object with the designated terminator token.
     *
     * @param terminator the token used to terminate each statement; may be null
     */
    public DdlChanges(String terminator) {
        this.terminator = terminator != null ? terminator : ";";
    }

    /**
     * Clear all accumulated changes.
     *
     * @return this object for method chaining; never null
     */
    public DdlChanges reset() {
        events.clear();
        databaseNames.clear();
        return this;
    }

    @Override
    public void handle(Event event) {
        events.add(event);
        databaseNames.add(getDatabase(event));
    }

    /**
     * Consume the events in the same order they were {@link #handle(io.debezium.relational.ddl.DdlParserListener.Event) recorded},
     * but grouped by database name. Multiple sequential statements that were applied to the same database are grouped together.
     * @param consumer the consumer
     */
    public void groupStatementStringsByDatabase(DatabaseStatementStringConsumer consumer) {
        groupEventsByDatabase((DatabaseEventConsumer) (dbName, eventList) -> {
            final StringBuilder statements = new StringBuilder();
            final Set<TableId> tables = new HashSet<>();
            eventList.forEach(event -> {
                statements.append(event.statement());
                statements.append(terminator);
                addTable(tables, event);
            });
            consumer.consume(dbName, tables, statements.toString());
        });
    }

    private void addTable(final Set<TableId> tables, Event event) {
        if (event instanceof TableEvent) {
            tables.add(((TableEvent) event).tableId());
        }
    }

    /**
     * Consume the events in the same order they were {@link #handle(io.debezium.relational.ddl.DdlParserListener.Event) recorded},
     * but grouped by database name. Multiple sequential statements that were applied to the same database are grouped together.
     * @param consumer the consumer
     */
    public void groupStatementsByDatabase(DatabaseStatementConsumer consumer) {
        groupEventsByDatabase((DatabaseEventConsumer) (dbName, eventList) -> {
            List<String> statements = new ArrayList<>();
            final Set<TableId> tables = new HashSet<>();
            eventList.forEach(event -> {
                statements.add(event.statement());
                addTable(tables, event);
            });
            consumer.consume(dbName, tables, statements);
        });
    }

    /**
     * Consume the events in the same order they were {@link #handle(io.debezium.relational.ddl.DdlParserListener.Event) recorded},
     * but grouped by database name. Multiple sequential statements that were applied to the same database are grouped together.
     * @param consumer the consumer
     */
    public void groupEventsByDatabase(DatabaseEventConsumer consumer) {
        if (isEmpty()) {
            return;
        }
        if (databaseNames.size() <= 1) {
            consumer.consume(databaseNames.iterator().next(), events);
            return;
        }
        List<Event> dbEvents = new ArrayList<>();
        String currentDatabase = null;
        for (Event event : events) {
            String dbName = getDatabase(event);
            if (currentDatabase == null || dbName.equals(currentDatabase)) {
                currentDatabase = dbName;
                // Accumulate the statement ...
                dbEvents.add(event);
            }
            else {
                // Submit the statements ...
                consumer.consume(currentDatabase, dbEvents);
            }
        }
    }

    protected String getDatabase(Event event) {
        switch (event.type()) {
            case CREATE_TABLE:
            case ALTER_TABLE:
            case DROP_TABLE:
            case TRUNCATE_TABLE:
                TableEvent tableEvent = (TableEvent) event;
                return tableEvent.tableId().catalog();
            case CREATE_INDEX:
            case DROP_INDEX:
                TableIndexEvent tableIndexEvent = (TableIndexEvent) event;
                return tableIndexEvent.tableId().catalog();
            case CREATE_DATABASE:
            case ALTER_DATABASE:
            case DROP_DATABASE:
            case USE_DATABASE:
                DatabaseEvent dbEvent = (DatabaseEvent) event;
                return dbEvent.databaseName();
            case SET_VARIABLE:
                SetVariableEvent varEvent = (SetVariableEvent) event;
                return varEvent.databaseName().orElse("");
        }
        assert false : "Should never happen";
        return null;
    }

    public boolean isEmpty() {
        return events.isEmpty();
    }

    public boolean applyToMoreDatabasesThan(String name) {
        return databaseNames.contains(name) ? databaseNames.size() > 1 : databaseNames.size() > 0;
    }

    @Override
    public String toString() {
        return events.toString();
    }

    public static interface DatabaseEventConsumer {
        void consume(String databaseName, List<Event> events);
    }

    public static interface DatabaseStatementConsumer {
        void consume(String databaseName, Set<TableId> tableList, List<String> ddlStatements);
    }

    public static interface DatabaseStatementStringConsumer {
        void consume(String databaseName, Set<TableId> tableList, String ddlStatements);
    }

    /**
     * @return true if any event stored is one of
     * <ul>
     * <li>database-wide events and affects included/excluded database</li>
     * <li>table related events and the table is included</li>
     * <li>events that set a variable and either affects included database or is a system-wide variable</li>
     * <ul>
     */
    public boolean anyMatch(Predicate<String> databaseFilter, Predicate<TableId> tableFilter) {
        return events.stream().anyMatch(event -> (event instanceof DatabaseEvent) && databaseFilter.test(((DatabaseEvent) event).databaseName())
                || (event instanceof TableEvent) && tableFilter.test(((TableEvent) event).tableId())
                || (event instanceof SetVariableEvent) && (!((SetVariableEvent) event).databaseName().isPresent()
                        || databaseFilter.test(((SetVariableEvent) event).databaseName().get())));
    }
}
