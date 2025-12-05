/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.transforms.ExtractSchemaToNewRecord.SOURCE_SCHEMA_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.DebeziumException;

public class ExtractSchemaToNewRecordTest extends AbstractExtractStateTest {

    @Test
    public void testHandleCreatedRecord() {
        SourceRecord createdRecord = createCreateRecord();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            SourceRecord transformRecord = transform.apply(createdRecord);
            assertMetadataBlock(transformRecord);
        }
    }

    @Test
    public void testHandleUpdateRecord() {
        SourceRecord updatedRecord = createUpdateRecord();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            SourceRecord transformRecord = transform.apply(updatedRecord);
            assertMetadataBlock(transformRecord);
        }
    }

    @Test
    public void testHandleDeletedRecord() {
        SourceRecord deletedRecord = createDeleteRecord();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            SourceRecord transformRecord = transform.apply(deletedRecord);
            assertMetadataBlock(transformRecord);
        }
    }

    @Test
    public void testHandleTombstoneRecord() {
        SourceRecord tombstoneRecord = createTombstoneRecord();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            SourceRecord transformRecord = transform.apply(tombstoneRecord);
            assertThat(transformRecord).isEqualTo(tombstoneRecord);
        }
    }

    @Test
    public void testHandleTruncatedRecord() {
        SourceRecord truncateRecord = createTruncateRecord();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            SourceRecord transformRecord = transform.apply(truncateRecord);
            assertThat(transformRecord).isEqualTo(truncateRecord);
        }
    }

    @Test
    public void testHandleUpdateRecordWithoutSchemaParameters() {
        SourceRecord truncateRecord = createUpdateRecordWithKey();
        try (ExtractSchemaToNewRecord<SourceRecord> transform = new ExtractSchemaToNewRecord<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            transform.apply(truncateRecord);
        }
        catch (DebeziumException ex) {
            assertThat(ex.getMessage())
                    .isEqualTo("Ensure that enable configurations \"column.propagate.source.type\" or \"datatype.propagate.source.type\" and the value is set to \".*\"");
        }
    }

    private void assertMetadataBlock(SourceRecord transformRecord) {
        Struct metadata = (Struct) ((Struct) transformRecord.value()).get(SOURCE_SCHEMA_KEY);
        Struct table = (Struct) metadata.get("table");
        List<Struct> columns = (List<Struct>) table.get("columns");
        Struct id = columns.get(0);
        Struct name = columns.get(1);

        assertThat(metadata.get("id")).isEqualTo("test_table");
        assertThat(id.get("name")).isEqualTo("id");
        assertThat(id.get("typeName")).isEqualTo("int");
        assertThat(name.get("name")).isEqualTo("name");
        assertThat(name.get("typeName")).isEqualTo("varchar");
        assertThat(name.get("length")).isEqualTo(255);
    }
}
