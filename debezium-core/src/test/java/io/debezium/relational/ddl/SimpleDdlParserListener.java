/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.fest.assertions.StringAssert;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DdlParser.Action;
import io.debezium.relational.ddl.DdlParser.Listener;

/**
 * @author Randall Hauch
 *
 */
public class SimpleDdlParserListener implements Listener {

    public static final class Event {
        public final TableId tableId;
        public final String indexName;
        public final Action action;
        public final String ddlStatement;

        public Event(TableId tableId, Action action, String ddlStatement) {
            this(null, tableId, action, ddlStatement);
        }

        public Event(String indexName, TableId tableId, Action action, String ddlStatement) {
            this.indexName = indexName;
            this.tableId = tableId;
            this.action = action;
            this.ddlStatement = ddlStatement;
        }

        public StringAssert assertDdlStatement() {
            return assertThat(ddlStatement);
        }

        public Event assertTableId(TableId id) {
            assertThat(tableId).isEqualTo(id);
            return this;
        }

        public Event assertCreateTable(TableId id) {
            assertThat(tableId).isEqualTo(id);
            assertThat(action).isEqualTo(Action.CREATE);
            return this;
        }

        public Event assertDropTable(TableId id) {
            assertThat(tableId).isEqualTo(id);
            assertThat(action).isEqualTo(Action.DROP);
            return this;
        }

        public Event assertAlterTable(TableId id) {
            assertThat(tableId).isEqualTo(id);
            assertThat(action).isEqualTo(Action.ALTER);
            return this;
        }

        public Event assertCreateIndex(String indexName, TableId id) {
            assertThat(indexName).isEqualTo(indexName);
            assertThat(tableId).isEqualTo(id);
            assertThat(action).isEqualTo(Action.CREATE);
            return this;
        }

        public Event assertDropIndex(String indexName, TableId id) {
            assertThat(indexName).isEqualTo(indexName);
            assertThat(tableId).isEqualTo(id);
            assertThat(action).isEqualTo(Action.DROP);
            return this;
        }
    }

    private final AtomicLong counter = new AtomicLong();
    private final List<Event> events = new ArrayList<>();

    public SimpleDdlParserListener() {
    }

    @Override
    public void handleTableEvent(TableId tableId, Action action, String ddlStatement) {
        events.add(new Event(tableId, action, ddlStatement));
        counter.incrementAndGet();
    }

    @Override
    public void handleIndexEvent(String indexName, TableId tableId, Action action, String ddlStatement) {
        events.add(new Event(indexName, tableId, action, ddlStatement));
        counter.incrementAndGet();
    }
    
    /**
     * Get the total number of events that have been handled by this listener.
     * @return the total number of events
     */
    public int total() {
        return counter.intValue();
    }
    
    /**
     * Get the number of events currently held by this listener that have yet to be {@link #next() processed}.
     * @return the number of remaining events
     */
    public int remaining() {
        return events.size();
    }
    
    /**
     * Get the next event seen by this listener.
     * @return the next event, or null if there is no event
     */
    public Event next() {
        return events.isEmpty() ? null : events.remove(0);
    }
}
