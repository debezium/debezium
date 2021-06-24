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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.history.HistoryRecord;

/**
 * A test utility for accumulating the {@link SourceRecord}s on the schema change topic.
 * @author Randall Hauch
 */
public class SchemaChangeHistory {

    private final String topicName;
    private final List<SourceRecord> sourceRecords = new ArrayList<>();
    private final Map<String, List<SourceRecord>> sourceRecordsByDbName = new HashMap<>();

    public SchemaChangeHistory(String topic) {
        this.topicName = topic;
    }

    public boolean add(SourceRecord record) {
        if (topicName.equals(record.topic())) {
            this.sourceRecords.add(record);
            String dbName = getAffectedDatabase(record);
            sourceRecordsByDbName.computeIfAbsent(dbName, db -> new ArrayList<>()).add(record);
            return true;
        }
        return false;
    }

    public int recordCount() {
        return sourceRecords.size();
    }

    public void forEach(Consumer<SourceRecord> consumer) {
        sourceRecords.forEach(consumer);
    }

    public int databaseCount() {
        return sourceRecordsByDbName.size();
    }

    public Set<String> databases() {
        return Collections.unmodifiableSet(sourceRecordsByDbName.keySet());
    }

    public List<SourceRecord> ddlRecordsForDatabase(String dbName) {
        return sourceRecordsByDbName.get(dbName);
    }

    protected String getAffectedDatabase(SourceRecord record) {
        Struct envelope = (Struct) record.value();
        Field dbField = envelope.schema().field(HistoryRecord.Fields.DATABASE_NAME);
        if (dbField != null) {
            return envelope.getString(dbField.name());
        }
        return null;
    }
}
