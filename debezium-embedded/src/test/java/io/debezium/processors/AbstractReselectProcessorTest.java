/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

import ch.qos.logback.classic.Level;

/**
 * @author Chris Cranford
 */
public abstract class AbstractReselectProcessorTest<T extends SourceConnector> extends AbstractConnectorTest {

    protected abstract Class<T> getConnectorClass();

    protected abstract JdbcConnection databaseConnection();

    protected abstract Configuration.Builder getConfigurationBuilder();

    protected abstract String topicName();

    protected abstract String tableName();

    protected abstract String reselectColumnsList();

    protected abstract void createTable() throws Exception;

    protected abstract void dropTable() throws Exception;

    protected abstract String getInsertWithValue();

    protected abstract String getInsertWithNullValue();

    protected abstract void waitForStreamingStarted() throws InterruptedException;

    @Before
    @SuppressWarnings("resource")
    public void beforeEach() throws Exception {
        createTable();
        databaseConnection().setAutoCommit(false);
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();
        assertNoRecordsToConsume();
        dropTable();
    }

    @Test
    @FixFor("DBZ-4321")
    @SuppressWarnings("resource")
    public void testNoColumnsReselectedWhenNotNullSnapshot() throws Exception {
        LogInterceptor interceptor = new LogInterceptor(ReselectColumnsPostProcessor.class);
        interceptor.setLoggerLevel(ReselectColumnsPostProcessor.class, Level.DEBUG);

        databaseConnection().execute(getInsertWithValue());

        Configuration config = getConfigurationBuilder()
                .with("snapshot.mode", "initial")
                .with("reselect.columns.list", reselectColumnsList()).build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        final SourceRecords sourceRecords = consumeRecordsByTopicReselectWhenNotNullSnapshot();
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName());

        // Check read
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidRead(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("one");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);

        assertThat(interceptor.containsMessage("No columns require re-selection.")).isTrue();
    }

    @Test
    @FixFor("DBZ-4321")
    @SuppressWarnings("resource")
    public void testNoColumnsReselectedWhenNotNullStreaming() throws Exception {
        LogInterceptor interceptor = new LogInterceptor(ReselectColumnsPostProcessor.class);
        interceptor.setLoggerLevel(ReselectColumnsPostProcessor.class, Level.DEBUG);

        Configuration config = getConfigurationBuilder().with("reselect.columns.list", reselectColumnsList()).build();
        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        databaseConnection().execute(getInsertWithValue());
        databaseConnection().execute(String.format("UPDATE %s SET data = 'two' where id = 1", tableName()));
        databaseConnection().execute(String.format("DELETE FROM %s WHERE id = 1", tableName()));

        final SourceRecords sourceRecords = consumeRecordsByTopicReselectWhenNotNullStreaming();
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName());

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("one");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("two");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);

        // Check delete
        record = tableRecords.get(2);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidDelete(record, fieldName("id"), 1);
        assertThat(after).isNull();

        // Check tombstone
        record = tableRecords.get(3);
        VerifyRecord.isValidTombstone(record, fieldName("id"), 1);
        assertThat(record.value()).isNull();

        assertThat(interceptor.containsMessage("No columns require re-selection.")).isTrue();
    }

    @Test
    @FixFor("DBZ-4321")
    @SuppressWarnings("resource")
    public void testColumnsReselectedWhenValueIsNullSnapshot() throws Exception {
        databaseConnection().execute(getInsertWithNullValue());
        databaseConnection().execute(String.format("UPDATE %s SET data = 'two' where id = 1", tableName()));

        Configuration config = getConfigurationBuilder()
                .with("snapshot.mode", "initial")
                .with("reselect.columns.list", reselectColumnsList())
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        final SourceRecords sourceRecords = consumeRecordsByTopicReselectWhenNullSnapshot();
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName());

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidRead(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("two");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);
    }

    @Test
    @FixFor("DBZ-4321")
    @SuppressWarnings("resource")
    public void testColumnsReselectedWhenValueIsNullStreaming() throws Exception {
        Configuration config = getConfigurationBuilder().with("reselect.columns.list", reselectColumnsList()).build();
        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForStreamingStarted();

        databaseConnection().executeWithoutCommitting(getInsertWithNullValue());
        databaseConnection().executeWithoutCommitting(String.format("UPDATE %s SET data = 'two' where id = 1", tableName()));
        databaseConnection().commit();

        final SourceRecords sourceRecords = consumeRecordsByTopicReselectWhenNullStreaming();
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic(topicName());

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("two");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, fieldName("id"), 1);
        assertThat(after.get(fieldName("id"))).isEqualTo(1);
        assertThat(after.get(fieldName("data"))).isEqualTo("two");
        assertThat(after.get(fieldName("data2"))).isEqualTo(1);
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullSnapshot() throws InterruptedException {
        return consumeRecordsByTopic(1);
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNotNullStreaming() throws InterruptedException {
        return consumeRecordsByTopic(4);
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNullSnapshot() throws InterruptedException {
        return consumeRecordsByTopic(1);
    }

    protected SourceRecords consumeRecordsByTopicReselectWhenNullStreaming() throws InterruptedException {
        return consumeRecordsByTopic(2);
    }

    protected String fieldName(String fieldName) {
        return fieldName;
    }

}
