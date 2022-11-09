/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

/**
 * An abstract representation of one or more changes to the structure to the tables of a relational database. Used
 * within a schema history as an alternative to storing and re-parsing DB-specific DDL statements.
 *
 * @author Gunnar Morling
 *
 */
public class TableChanges implements Iterable<TableChange> {

    private final List<TableChange> changes;

    public TableChanges() {
        this.changes = new ArrayList<>();
    }

    public TableChanges create(Table table) {
        changes.add(new TableChange(TableChangeType.CREATE, table));
        return this;
    }

    public TableChanges alter(Table table) {
        changes.add(new TableChange(TableChangeType.ALTER, table));
        return this;
    }

    public TableChanges alter(TableChange change) {
        if (change.getPreviousId() == null) {
            return alter(change.getTable());
        }
        return rename(change.getTable(), change.getPreviousId());
    }

    public TableChanges rename(Table table, TableId previousId) {
        changes.add(new TableChange(TableChangeType.ALTER, table, previousId));
        return this;
    }

    public TableChanges drop(Table table) {
        changes.add(new TableChange(TableChangeType.DROP, table));
        return this;
    }

    @Override
    public Iterator<TableChange> iterator() {
        return changes.iterator();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + changes.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TableChanges other = (TableChanges) obj;

        return changes.equals(other.changes);
    }

    @Override
    public String toString() {
        return "TableChanges [changes=" + changes + "]";
    }

    public static class TableChange {

        private final TableChangeType type;
        private final TableId previousId;
        private final TableId id;
        private final Table table;

        public TableChange(TableChangeType type, Table table) {
            this(type, table, null);
        }

        public TableChange(TableChangeType type, Table table, TableId previousId) {
            this.type = type;
            this.table = table;
            this.id = table.id();
            this.previousId = previousId;
        }

        public TableChangeType getType() {
            return type;
        }

        public TableId getId() {
            return id;
        }

        public TableId getPreviousId() {
            return previousId;
        }

        public Table getTable() {
            return table;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + id.hashCode();
            result = prime * result + ((previousId == null) ? 0 : previousId.hashCode());
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + type.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TableChange other = (TableChange) obj;
            if (!id.equals(other.id)) {
                return false;
            }
            if (previousId == null) {
                if (other.previousId != null) {
                    return false;
                }
            }
            else if (!previousId.equals(other.previousId)) {
                return false;
            }
            if (table == null) {
                if (other.table != null) {
                    return false;
                }
            }
            else if (!table.equals(other.table)) {
                return false;
            }
            if (type != other.type) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "TableChange [type=" + type + ", id=" + id + ", previousId=" + previousId + ", table=" + table + "]";
        }

    }

    /**
     * The interface that defines conversion of {@code TableChanges} into a serialized format for
     * persistent storage or delivering as a message.
     *
     * @author Jiri Pechanec
     *
     * @param <T> target type
     */
    public interface TableChangesSerializer<T> {

        T serialize(TableChanges tableChanges);

        /**
         * @param useCatalogBeforeSchema true if the parsed string contains only 2 items and the first should be used as
         * the catalog and the second as the table name, or false if the first should be used as the schema and the
         * second as the table name
         */
        TableChanges deserialize(T data, boolean useCatalogBeforeSchema);
    }

    public enum TableChangeType {
        CREATE,
        ALTER,
        DROP;
    }
}
