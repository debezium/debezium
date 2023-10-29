/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

public class NewExtractNewRecordStateTest extends AbstractExtractStateTest {

    @Test
    public void testTombstoneDroppedByDefault() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneDroppedConfigured() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            // props.put(DROP_TOMBSTONES, "true");
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneForwardConfigured() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            // props.put(DROP_TOMBSTONES, "false");
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            // assertThat(transform.apply(tombstone)).isEqualTo(tombstone);
            // convert delete event to tombstone record and skip the following tombstone if existed
            assertThat(transform.apply(tombstone)).isNull();
        }
    }

    @Test
    public void testTombstoneRewriteWithTombstone() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite-with-tombstone");
            transform.configure(props);

            final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
            assertThat(transform.apply(tombstone)).isEqualTo(tombstone);
        }
    }

    @Test
    public void testTruncateDroppedByDefault() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord truncate = createTruncateRecord();
            assertThat(transform.apply(truncate)).isNull();
        }
    }

    @Test
    public void testDeleteDroppedByDefault() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteDrop() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord)).isNull();
        }
    }

    @Test
    public void testHandleDeleteNone() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord tombstone = transform.apply(deleteRecord);
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test
    public void testHandleDeleteRewrite() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
        }
    }

    @Test
    public void testHandleCreateRewrite() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("false");
            assertThat(unwrapped.headers()).hasSize(1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    public void testUnwrapCreateRecord() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);
            assertThat(((Struct) unwrapped.value()).getString("name")).isEqualTo("myRecord");
        }
    }

    @Test
    @FixFor("DBZ-5166")
    public void testUnwrapCreateRecordWithOptionalDefaultValue() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecordWithOptionalNull();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);
            assertThat(((Struct) unwrapped.value()).getString("name")).isEqualTo(null);
        }
    }

    @Test
    public void testIgnoreUnknownRecord() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
        }
    }

    @Test
    @FixFor("DBZ-971")
    public void testUnwrapPropagatesRecordHeaders() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            createRecord.headers().addString("application/debezium-test-header", "shouldPropagatePreviousRecordHeaders");

            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).getInt8("id")).isEqualTo((byte) 1);

            assertThat(unwrapped.headers()).hasSize(1);
            Iterator<Header> headers = unwrapped.headers().allWithName("application/debezium-test-header");
            assertThat(headers.hasNext()).isTrue();
            assertThat(headers.next().value().toString()).isEqualTo("shouldPropagatePreviousRecordHeaders");
        }
    }

    @Test
    @FixFor({ "DBZ-1452", "DBZ-6901" })
    public void testAddField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_FIELDS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());

            SourceRecord createRecordAddingColumn = createCreateRecordAddingColumn("started_at", 1694587158123L);
            SourceRecord unwrappedAddingColumn = transform.apply(createRecordAddingColumn);
            assertThat(((Struct) unwrappedAddingColumn.value()).get("started_at")).isEqualTo(1694587158123L);

            createRecordAddingColumn = createCreateRecordAddingColumn("started_at", 1694587158789L);
            unwrappedAddingColumn = transform.apply(createRecordAddingColumn);
            assertThat(((Struct) unwrappedAddingColumn.value()).get("started_at")).isEqualTo(1694587158789L);
        }
    }

    @Test
    @FixFor("DBZ-2984")
    public void testAddTimestamp() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props1 = new HashMap<>();
            props1.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props1.put(ADD_FIELDS, "ts_ms");
            transform.configure(props1);

            final SourceRecord createRecord1 = createCreateRecord();
            final SourceRecord unwrapped1 = transform.apply(createRecord1);
            assertThat(((Struct) unwrapped1.value()).get("__ts_ms")).isNotNull();

            final Map<String, String> props2 = new HashMap<>();
            props2.put(ADD_FIELDS, "source.ts_ms");
            transform.configure(props2);

            final SourceRecord createRecord2 = createCreateRecord();
            final SourceRecord unwrapped2 = transform.apply(createRecord2);
            assertThat(((Struct) unwrapped2.value()).get("__source_ts_ms")).isNotNull();
        }
    }

    @Test
    @FixFor({ "DBZ-1452", "DBZ-2504" })
    public void testAddFields() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_FIELDS, "op , lsn,id");
            props.put(ADD_FIELDS_PREFIX, "prefix.");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get("prefix.op")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get("prefix.lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("prefix.id")).isEqualTo("571");
        }
    }

    @Test
    @FixFor({ "DBZ-2606" })
    public void testNewFieldAndHeaderMapping() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            String fieldPrefix = "";
            String headerPrefix = "prefix.";
            props.put(ADD_FIELDS, "op:OP, lsn:LSN, id:ID, source.lsn:source_lsn, transaction.total_order:TOTAL_ORDER");
            props.put(ADD_FIELDS_PREFIX, fieldPrefix);
            props.put(ADD_HEADERS, "op, source.lsn:source_lsn, transaction.id:TXN_ID, transaction.total_order:TOTAL_ORDER");
            props.put(ADD_HEADERS_PREFIX, headerPrefix);
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "OP")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "LSN")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "ID")).isEqualTo("571");
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "source_lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "TOTAL_ORDER")).isEqualTo(42L);

            assertThat(unwrapped.headers()).hasSize(4);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, headerPrefix + "op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.UPDATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, headerPrefix + "source_lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, headerPrefix + "TXN_ID");
            assertThat(headerValue).isEqualTo(String.valueOf(571L));
            headerValue = getSourceRecordHeaderByKey(unwrapped, headerPrefix + "TOTAL_ORDER");
            assertThat(headerValue).isEqualTo(String.valueOf(42L));
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsForMissingOptionalField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_FIELDS, "op,lsn,id");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.CREATE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("__id")).isEqualTo(null);
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsSpecifyStruct() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_FIELDS, "op,source.lsn,transaction.id,transaction.total_order");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get("__source_lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get("__transaction_id")).isEqualTo("571");
            assertThat(((Struct) unwrapped.value()).get("__transaction_total_order")).isEqualTo(42L);
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeader() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeaders() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_HEADERS, "op , lsn,id");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.headers()).hasSize(3);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.UPDATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__id");
            assertThat(headerValue).isEqualTo(String.valueOf(571L));
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddHeadersForMissingOptionalField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_HEADERS, "op,lsn,id");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assertThat(unwrapped.headers()).hasSize(3);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.CREATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__id");
            assertThat(headerValue).isNull();
        }
    }

    @Test
    @FixFor({ "DBZ-1452", "DBZ-2504" })
    public void testAddHeadersSpecifyStruct() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_HEADERS, "op,source.lsn,transaction.id,transaction.total_order");
            props.put(ADD_HEADERS_PREFIX, "prefix.");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.headers()).hasSize(4);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.UPDATE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.source_lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.transaction_id");
            assertThat(headerValue).isEqualTo(String.valueOf(571L));
            headerValue = getSourceRecordHeaderByKey(unwrapped, "prefix.transaction_total_order");
            assertThat(headerValue).isEqualTo(String.valueOf(42L));
        }
    }

    @Test
    public void testAddTopicRoutingField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ROUTE_BY_FIELD, "name");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrappedCreate = transform.apply(createRecord);
            assertThat(unwrappedCreate.topic()).isEqualTo("myRecord");
        }
    }

    @Test
    public void testUpdateTopicRoutingField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ROUTE_BY_FIELD, "name");
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecord();
            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(unwrapped.topic()).isEqualTo("updatedRecord");
        }
    }

    @Test
    public void testDeleteTopicRoutingField() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ROUTE_BY_FIELD, "name");

            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            assertThat(transform.apply(deleteRecord).topic()).isEqualTo("myRecord");
        }
    }

    @Test
    @FixFor("DBZ-1876")
    public void testAddHeadersHandleDeleteRewriteAndTombstone() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite-with-tombstone");
            props.put(ADD_HEADERS, "op,source.lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            String headerValue = getSourceRecordHeaderByKey(unwrapped, "__op");
            assertThat(headerValue).isEqualTo(Envelope.Operation.DELETE.code());
            headerValue = getSourceRecordHeaderByKey(unwrapped, "__source_lsn");
            assertThat(headerValue).isEqualTo(String.valueOf(1234));

            final SourceRecord tombstone = transform.apply(createTombstoneRecord());
            assertThat(getSourceRecordHeaderByKey(tombstone, "__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldHandleDeleteRewrite() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsHandleDeleteRewrite() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op,lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1876")
    public void testAddFieldsHandleDeleteRewriteAndTombstone() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite-with-tombstone");
            props.put(ADD_FIELDS, "op,lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__lsn")).isEqualTo(1234);

            final SourceRecord tombstone = transform.apply(createTombstoneRecord());
            assertThat(tombstone.value()).isNull();
        }
    }

    @Test
    @FixFor("DBZ-1452")
    public void testAddFieldsSpecifyStructHandleDeleteRewrite() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "rewrite");
            props.put(ADD_FIELDS, "op,source.lsn");
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord unwrapped = transform.apply(deleteRecord);
            assertThat(((Struct) unwrapped.value()).getString("__deleted")).isEqualTo("true");
            assertThat(((Struct) unwrapped.value()).get("__op")).isEqualTo(Envelope.Operation.DELETE.code());
            assertThat(((Struct) unwrapped.value()).get("__source_lsn")).isEqualTo(1234);
        }
    }

    @Test
    @FixFor("DBZ-1517")
    public void testSchemaChangeEventWithOperationHeader() {
        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            props.put(ADD_HEADERS, "op");
            transform.configure(props);

            final SourceRecord unknownRecord = createUnknownRecord();
            assertThat(transform.apply(unknownRecord)).isEqualTo(unknownRecord);

            final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
            assertThat(transform.apply(unnamedSchemaRecord)).isEqualTo(unnamedSchemaRecord);
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void dropFieldsFromValueWithSchemaCompatibility() {
        final List<String> dropFields = List.of("id", "name");
        final String dropHeaderName = "drop-fields";

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            props.put(DROP_FIELDS_HEADER_NAME, "drop-fields");
            transform.configure(props);

            // Create has no key, only a value
            // "id" is retained because its non-optional in the value.
            // "name" should be dropped because its optional
            SourceRecord before = addDropFieldsHeader(createCreateRecordWithOptionalNull(), dropHeaderName, dropFields);
            SourceRecord after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Create has a key with one optional field name
            // "id" should be retained in the key & value because drop key is not enabled & is non-optional.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createCreateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Update has no key, only a value
            // "id" is retained because its non-optional in the value.
            // "name" should be dropped because its optional
            before = addDropFieldsHeader(createUpdateRecordWithOptionalNull(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Update has a key with one optional field name
            // "id" should be retained in the key & value because drop key is not enabled & is non-optional.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createUpdateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Delete
            before = addDropFieldsHeader(createDeleteRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default

            // Tombstones
            before = addDropFieldsHeader(createTombstoneRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void dropFieldsFromValueWithoutSchemaCompatibility() {
        final List<String> dropFields = List.of("id", "name");
        final String dropHeaderName = "drop-fields";

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            props.put(DROP_FIELDS_HEADER_NAME, "drop-fields");
            props.put(DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE, "false");
            transform.configure(props);

            // Create has no key, only a value
            // "id" is not optional, but it's dropped from the value because schema compatibility is disabled
            // "name" should be dropped because its optional
            SourceRecord before = addDropFieldsHeader(createCreateRecordWithOptionalNull(), dropHeaderName, dropFields);
            SourceRecord after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Create has a key with one optional field name
            // "id" is retained in the key, but dropped in the value due to disabling schema compatibility.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createCreateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Update has no key, only a value
            // "id" is not optional, but it's dropped from the value because schema compatibility is disabled
            // "name" should be dropped because its optional
            before = addDropFieldsHeader(createUpdateRecordWithOptionalNull(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Update has a key with one optional field name
            // "id" is retained in the key, but dropped in the value due to disabling schema compatibility.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createUpdateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Delete
            before = addDropFieldsHeader(createDeleteRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default

            // Tombstones
            before = addDropFieldsHeader(createTombstoneRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void dropFieldsFromValueAndKeyWithSchemaCompatibility() {
        final List<String> dropFields = List.of("id", "name");
        final String dropHeaderName = "drop-fields";

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            props.put(DROP_FIELDS_HEADER_NAME, "drop-fields");
            props.put(DROP_FIELDS_FROM_KEY, "true");
            transform.configure(props);

            // Create has no key, only a value
            // "id" is retained because its non-optional in the value.
            // "name" should be dropped because its optional
            SourceRecord before = addDropFieldsHeader(createCreateRecordWithOptionalNull(), dropHeaderName, dropFields);
            SourceRecord after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Create has a key with one optional field name
            // "id" should be retained in the key & value because drop key is not enabled & is non-optional.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createCreateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Update has no key, only a value
            // "id" is retained because its non-optional in the value.
            // "name" should be dropped because its optional
            before = addDropFieldsHeader(createUpdateRecordWithOptionalNull(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Update has a key with one optional field name
            // "id" should be retained in the key & value because drop key is not enabled & is non-optional.
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createUpdateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNotNull();
            assertThat(((Struct) after.key()).get("id")).isEqualTo((byte) 1);
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNotNull();
            assertThat(((Struct) after.value()).get("id")).isEqualTo((byte) 1);

            // Delete
            before = addDropFieldsHeader(createDeleteRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default

            // Tombstones
            before = addDropFieldsHeader(createTombstoneRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void dropFieldsFromValueAndKeyWithoutSchemaCompatibility() {
        final List<String> dropFields = List.of("id", "name");
        final String dropHeaderName = "drop-fields";

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "drop");
            props.put(DROP_FIELDS_HEADER_NAME, "drop-fields");
            props.put(DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE, "false");
            props.put(DROP_FIELDS_FROM_KEY, "true");
            transform.configure(props);

            // Create has no key, only a value
            // "id" is not optional, but it's dropped from the key and value because schema compatibility is disabled
            // "name" should be dropped because its optional
            SourceRecord before = addDropFieldsHeader(createCreateRecordWithOptionalNull(), dropHeaderName, dropFields);
            SourceRecord after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Create has a key with one optional field name
            // "id" is not optional, but it's dropped from the key and value because schema compatibility is disabled
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createCreateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNull();
            assertThat(after.key()).isEqualTo(new Struct(after.keySchema()));
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Update has no key, only a value
            // "id" is not optional, but it's dropped from the value because schema compatibility is disabled
            // "name" should be dropped because its optional
            before = addDropFieldsHeader(createUpdateRecordWithOptionalNull(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.key()).isNull(); // no key was specified in original event
            assertThat(after.keySchema()).isNull();
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Update has a key with one optional field name
            // "id" is not optional, but it's dropped from the key and value because schema compatibility is disabled
            // "name" should be dropped in the value because it's optional and not a key column.
            before = addDropFieldsHeader(createUpdateRecordWithKey(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after.keySchema().field("id")).isNull();
            assertThat(after.key()).isEqualTo(new Struct(after.keySchema()));
            assertThat(after.valueSchema().field("name")).isNull();
            assertThat(after.valueSchema().field("id")).isNull();
            assertThat(after.value()).isNotNull();
            assertThat(((Struct) after.value())).isEqualTo(new Struct(after.valueSchema()));

            // Delete
            before = addDropFieldsHeader(createDeleteRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default

            // Tombstones
            before = addDropFieldsHeader(createTombstoneRecord(), dropHeaderName, dropFields);
            after = transform.apply(before);
            assertThat(after).isNull(); // drop tombstones are enabled by default
        }
    }

    @Test
    @FixFor({ "DBZ-6486" })
    public void testNewNotDefaultField() {

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            String fieldPrefix = "";
            props.put(ADD_FIELDS, "notExisting,op:OP, lsn:LSN, id:ID, source.lsn:source_lsn, transaction.total_order:TOTAL_ORDER, changes:META_SRC_CHANGED");
            props.put(ADD_FIELDS_PREFIX, fieldPrefix);
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecordWithChangedFields();

            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "OP")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "LSN")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "ID")).isEqualTo("571");
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "source_lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "TOTAL_ORDER")).isEqualTo(42L);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "META_SRC_CHANGED")).isEqualTo(List.of("name"));
            assertThat((unwrapped.valueSchema().field("notExisting"))).isNull();
        }
    }

    @Test
    @FixFor({ "DBZ-6486" })
    public void testFieldNotExists() {

        try (ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_TOMBSTONE_DELETES, "tombstone");
            String fieldPrefix = "";
            props.put(ADD_FIELDS, "op:OP, lsn:LSN, id:ID, source.lsn:source_lsn, transaction.total_order:TOTAL_ORDER, notExist:META_SRC_CHANGED");
            props.put(ADD_FIELDS_PREFIX, fieldPrefix);
            transform.configure(props);

            final SourceRecord updateRecord = createUpdateRecordWithChangedFields();

            final SourceRecord unwrapped = transform.apply(updateRecord);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "OP")).isEqualTo(Envelope.Operation.UPDATE.code());
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "LSN")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "ID")).isEqualTo("571");
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "source_lsn")).isEqualTo(1234);
            assertThat(((Struct) unwrapped.value()).get(fieldPrefix + "TOTAL_ORDER")).isEqualTo(42L);
        }
    }
}
