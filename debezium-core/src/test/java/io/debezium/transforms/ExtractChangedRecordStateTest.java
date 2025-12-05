/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for the {@link ExtractChangedRecordState} single message transformation.
 *
 * @author Chris Cranford
 */
public class ExtractChangedRecordStateTest extends AbstractExtractStateTest {

    @Test
    @FixFor("DBZ-8721")
    public void testUpdateWithDefaultValues() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecordWithOptionalNull();
            // Update the name from null to the default value "default_str" instead of "updatedRecord"
            ((Struct) updatedRecord.value()).getStruct("after").put("name", "default_str");

            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            assertThat(changedHeaderValues).containsExactly("name");
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testAddUpdatedFieldToHeaders() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            final Header unchangedHeader = getSourceRecordHeader(transformRecord, "Unchanged");
            final List<String> unchangedHeaderValues = (ArrayList<String>) unchangedHeader.value();

            assertThat(transformRecord.headers().size()).isEqualTo(2);
            assertThat(changedHeaderValues.size()).isEqualTo(1);
            assertThat(changedHeaderValues.get(0)).isEqualTo("name");
            assertThat(changedHeader.schema().name()).isEqualTo("Changed");
            assertThat(unchangedHeaderValues.size()).isEqualTo(1);
            assertThat(unchangedHeaderValues.get(0)).isEqualTo("id");
        }

        // Not set unchanged header
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            final Header unchangedHeader = getSourceRecordHeader(transformRecord, "Unchanged");

            assertThat(transformRecord.headers().size()).isEqualTo(1);
            assertThat(changedHeaderValues.contains("name")).isTrue();
            assertThat(unchangedHeader).isNull();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testAddCreatedFieldToHeaders() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            assertThat(transformRecord.headers().size()).isEqualTo(2);
            assertThat((List<?>) getSourceRecordHeader(transformRecord, "Changed").value()).isEmpty();
            assertThat((List<?>) getSourceRecordHeader(transformRecord, "Unchanged").value()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithCreate() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithUpdate() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithDelete() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord transformRecord = transform.apply(deleteRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    private Header getSourceRecordHeader(SourceRecord record, String headerKey) {
        Iterator<Header> operationHeader = record.headers().allWithName(headerKey);
        if (!operationHeader.hasNext()) {
            return null;
        }
        return operationHeader.next();
    }
}
