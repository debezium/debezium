/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope.Operation;
import io.debezium.relational.TableId;

/**
 * A test utility for accumulating the {@link SourceRecord}s that represent change events on rows. This store applies the
 * changes and maintains the current state of the rows.
 *
 * @author Randall Hauch
 */
public class KeyValueStore {

    protected static Function<String, TableId> prefixedWith(String prefix) {
        int index = prefix.length();
        return topicName -> {
            if (topicName.startsWith(prefix) && topicName.length() > index) {
                return TableId.parse(topicName.substring(index));
            }
            return null;
        };
    }

    protected static Function<String, TableId> fromRegex(String regex, int groupNumber) {
        Pattern pattern = Pattern.compile(regex);
        return topicName -> {
            Matcher m = pattern.matcher(topicName);
            String fullyQualified = m.group(groupNumber);
            return fullyQualified != null ? TableId.parse(fullyQualified) : null;
        };
    }

    /**
     * Create a KeyValueStore that uses the supplied regular expression and group number to extract the {@link TableId} from
     * the topic name.
     *
     * @param regex the regular expression that identifies the table ID within the topic name; may not be null
     * @param groupNumber the group number in the regex for the table ID string
     * @return the key value store
     */
    public static KeyValueStore createForTopicsMatching(String regex, int groupNumber) {
        return new KeyValueStore(fromRegex(regex, groupNumber));
    }

    /**
     * Create a KeyValueStore that removes from the topic names the supplied prefix to obtain the {@link TableId}.
     *
     * @param prefix the prefix after which all of the topic name forms the table ID; may not be null
     * @return the key value store
     */
    public static KeyValueStore createForTopicsBeginningWith(String prefix) {
        return new KeyValueStore(prefixedWith(prefix));
    }

    private final List<SourceRecord> sourceRecords = new ArrayList<>();
    private final Map<TableId, Collection> collectionsByTableId = new HashMap<>();
    private final Function<String, TableId> tableIdFromTopic;

    protected KeyValueStore(Function<String, TableId> tableIdFromTopic) {
        this.tableIdFromTopic = tableIdFromTopic;
    }

    public void add(SourceRecord record) {
        TableId tableId = tableIdFromTopic.apply(record.topic());
        if (tableId != null) {
            this.sourceRecords.add(record);
            getOrCreate(tableId).add(record);
        }
    }

    public List<SourceRecord> sourceRecords() {
        return sourceRecords;
    }

    public Collection collection(String fullyQualifiedName) {
        return collection(TableId.parse(fullyQualifiedName));
    }

    public Collection collection(String catalog, String tableName) {
        return collection(new TableId(catalog, null, tableName));
    }

    public Collection collection(TableId tableId) {
        return collectionsByTableId.get(tableId);
    }

    public Set<TableId> collections() {
        return Collections.unmodifiableSet(collectionsByTableId.keySet());
    }

    public Set<String> databases() {
        return collectionsByTableId.keySet().stream().map(TableId::catalog).collect(Collectors.toSet());
    }

    public int collectionCount() {
        return collectionsByTableId.size();
    }

    public Collection getOrCreate(TableId tableId) {
        return collectionsByTableId.computeIfAbsent(tableId, Collection::new);
    }

    public static class Collection {
        private final TableId id;
        private Schema keySchema = null;
        private Schema valueSchema = null;
        private final List<Schema> keySchemas = new ArrayList<>();
        private final List<Schema> valueSchemas = new ArrayList<>();
        private final Map<Struct, SourceRecord> valuesByKey = new HashMap<>();
        private final SourceRecordStats stats = new SourceRecordStats();

        protected Collection(TableId id) {
            this.id = id;
        }

        public TableId tableId() {
            return id;
        }

        /**
         * Get the number of changes to the key schema for events in this collection.
         *
         * @return the count; never negative
         */
        public long numberOfKeySchemaChanges() {
            return keySchemas.size();
        }

        /**
         * Get the number of changes to the key schema for events in this collection.
         *
         * @return the count; never negative
         */
        public long numberOfValueSchemaChanges() {
            return valueSchemas.size();
        }

        /**
         * Get the number of {@link Operation#CREATE CREATE} records {@link #add(SourceRecord) added} to this collection.
         *
         * @return the count; never negative
         */
        public long numberOfCreates() {
            return stats.numberOfCreates();
        }

        /**
         * Get the number of {@link Operation#DELETE DELETE} records {@link #add(SourceRecord) added} to this collection.
         *
         * @return the count; never negative
         */
        public long numberOfDeletes() {
            return stats.numberOfDeletes();
        }

        /**
         * Get the number of {@link Operation#READ READ} records {@link #add(SourceRecord) added} to this collection.
         *
         * @return the count; never negative
         */
        public long numberOfReads() {
            return stats.numberOfReads();
        }

        /**
         * Get the number of {@link Operation#UPDATE UPDATE} records {@link #add(SourceRecord) added} to this collection.
         *
         * @return the count; never negative
         */
        public long numberOfUpdates() {
            return stats.numberOfUpdates();
        }

        /**
         * Get the number of tombstone records that were {@link #add(SourceRecord) added} to this collection.
         *
         * @return the count; never negative
         */
        public long numberOfTombstones() {
            return stats.numberOfTombstones();
        }

        public int size() {
            return valuesByKey.size();
        }

        public int keySchemaChanges() {
            return keySchemas.size();
        }

        public int valueSchemaChanges() {
            return valueSchemas.size();
        }

        public Struct value(Struct key) {
            SourceRecord record = valuesByKey.get(key);
            return record != null ? valueFor(record) : null;
        }

        public void forEach(Consumer<SourceRecord> consumer) {
            valuesByKey.values().forEach(consumer);
        }

        protected void add(SourceRecord record) {
            if (record != null) {
                Struct key = keyFor(record);
                if (key == null) {
                    // no PK information
                    return;
                }
                Struct value = valueFor(record);
                if (value != null) {
                    Operation op = Envelope.operationFor(record);
                    if (op != null) {
                        switch (op) {
                            case READ:
                            case CREATE:
                            case UPDATE:
                                // Replace the existing value ...
                                valuesByKey.put((Struct) record.key(), record);
                                break;
                            case DELETE:
                                valuesByKey.remove(key);
                                break;
                        }
                    }
                }
                else {
                    // This is a tombstone ...
                    valuesByKey.remove(key);
                }
                // Add the schemas if they are different ...
                if (!record.keySchema().equals(keySchema)) {
                    keySchemas.add(record.keySchema());
                    keySchema = record.keySchema();
                }
                if (!record.valueSchema().equals(valueSchema)) {
                    valueSchemas.add(record.valueSchema());
                    valueSchema = record.valueSchema();
                }
                stats.accept(record);
            }
        }
    }

    protected static Struct keyFor(SourceRecord record) {
        return (Struct) record.key();
    }

    protected static Struct valueFor(SourceRecord record) {
        Struct envelope = (Struct) record.value();
        Field afterField = envelope.schema().field(Envelope.FieldName.AFTER);
        if (afterField != null) {
            return envelope.getStruct(afterField.name());
        }
        return null;
    }

    protected static Struct sourceFor(SourceRecord record) {
        Struct envelope = (Struct) record.value();
        Field field = envelope.schema().field(Envelope.FieldName.SOURCE);
        if (field != null) {
            return envelope.getStruct(field.name());
        }
        return null;
    }
}
