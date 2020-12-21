/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.debezium.relational.TableId;

/**
 * @author Randall Hauch
 *
 */
public class SimpleDdlParserListener extends DdlChanges implements DdlParserListener {

    public static final class EventAssert {

        private final Event actual;

        public EventAssert(Event actual) {
            this.actual = actual;
        }

        public EventAssert ddlMatches(String expected) {
            assertThat(actual.statement()).isEqualTo(expected);
            return this;
        }

        public EventAssert ddlStartsWith(String expected) {
            assertThat(actual.statement()).startsWith(expected);
            return this;
        }

        public EventAssert ddlContains(String expected) {
            assertThat(actual.statement()).contains(expected);
            return this;
        }

        protected TableEvent tableEvent() {
            assertThat(actual).isInstanceOf(TableEvent.class);
            return (TableEvent) actual;
        }

        protected TableAlteredEvent alterTableEvent() {
            assertThat(actual).isInstanceOf(TableAlteredEvent.class);
            return (TableAlteredEvent) actual;
        }

        public EventAssert tableNameIs(String expected) {
            assertThat(tableEvent().tableId().table()).isEqualTo(expected);
            return this;
        }

        public EventAssert tableIs(TableId expected) {
            assertThat(tableEvent().tableId()).isEqualTo(expected);
            return this;
        }

        public EventAssert ofType(EventType expected) {
            assertThat(actual.type()).isEqualTo(expected);
            return this;
        }

        public EventAssert createTableNamed(String tableName) {
            return createTable().tableNameIs(tableName).isNotView();
        }

        public EventAssert alterTableNamed(String tableName) {
            return alterTable().tableNameIs(tableName).isNotView();
        }

        public EventAssert truncateTableNamed(String tableName) {
            return truncateTable().tableNameIs(tableName).isNotView();
        }

        public EventAssert renamedFrom(String oldName) {
            TableId previousTableId = alterTableEvent().previousTableId();
            if (oldName == null) {
                assertThat(previousTableId).isNull();
            }
            else {
                assertThat(previousTableId.table()).isEqualTo(oldName);
            }
            return this;
        }

        public EventAssert dropTableNamed(String tableName) {
            return dropTable().tableNameIs(tableName).isNotView();
        }

        public EventAssert createViewNamed(String viewName) {
            return createTable().tableNameIs(viewName).isView();
        }

        public EventAssert alterViewNamed(String viewName) {
            return alterTable().tableNameIs(viewName).isView();
        }

        public EventAssert dropViewNamed(String viewName) {
            return dropTable().tableNameIs(viewName).isView();
        }

        public EventAssert isView() {
            assertThat(tableEvent().isView()).isTrue();
            return this;
        }

        public EventAssert isNotView() {
            assertThat(tableEvent().isView()).isFalse();
            return this;
        }

        public EventAssert createTable() {
            ofType(EventType.CREATE_TABLE);
            return this;
        }

        public EventAssert alterTable() {
            ofType(EventType.ALTER_TABLE);
            return this;
        }

        public EventAssert dropTable() {
            ofType(EventType.DROP_TABLE);
            return this;
        }

        public EventAssert createIndex() {
            ofType(EventType.CREATE_INDEX);
            return this;
        }

        public EventAssert dropIndex() {
            ofType(EventType.DROP_INDEX);
            return this;
        }

        public EventAssert truncateTable() {
            ofType(EventType.TRUNCATE_TABLE);
            return this;
        }
    }

    private final AtomicLong counter = new AtomicLong();
    private final List<Event> events = new ArrayList<>();

    public SimpleDdlParserListener() {
    }

    @Override
    public void handle(Event event) {
        events.add(event);
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
     * Get the number of events currently held by this listener that have yet to be {@link #assertNext() checked}.
     * @return the number of remaining events
     */
    public int remaining() {
        return events.size();
    }

    /**
     * Assert that there is no next event.
     */
    public void assertNoMoreEvents() {
        assertThat(events.isEmpty()).isTrue();
    }

    /**
     * Perform assertions on the next event seen by this listener.
     * @return the next event, or null if there is no event
     */
    public EventAssert assertNext() {
        assertThat(events.isEmpty()).isFalse();
        return new EventAssert(events.remove(0));
    }

    /**
     * Perform an operation on each of the events.
     * @param eventConsumer the event consumer function; may not be null
     */
    public void forEach(Consumer<Event> eventConsumer) {
        events.forEach(eventConsumer);
    }
}
