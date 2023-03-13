/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.time.Instant;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;

public class SMTTestFramework {

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("server-1.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .build();

    public static SourceRecord buildProductsSourceRecord(String connector, String db, String schema, String tableName, Struct row, Envelope.Operation operation) {

        String dataCollectionFieldName = "mongodb".equals(connector) ? "collection" : "table";

        SchemaBuilder sourceSchemaBuilder = SchemaBuilder.struct()
                .name("source")
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field(dataCollectionFieldName, Schema.STRING_SCHEMA);

        if (schema != null) {
            sourceSchemaBuilder.field("schema", Schema.STRING_SCHEMA);
        }

        Schema sourceSchema = sourceSchemaBuilder.build();

        Envelope createEnvelope = Envelope.defineSchema()
                .withName("server-1.inventory.product.Envelope")
                .withRecord(VALUE_SCHEMA)
                .withSource(sourceSchema)
                .build();

        Struct source = new Struct(sourceSchema);
        source.put("connector", connector);
        source.put("db", db);
        source.put(dataCollectionFieldName, tableName);
        if (schema != null) {
            source.put("schema", schema);
        }

        Struct payload = createEnvelope.create(row, source, Instant.now());

        switch (operation) {
            case CREATE:
            case UPDATE:
            case READ:
                payload = createEnvelope.create(row, source, Instant.now());
                break;
            case DELETE:
                payload = createEnvelope.delete(row, source, Instant.now());
                break;
            case TRUNCATE:
                payload = createEnvelope.truncate(source, Instant.now());
                break;
        }

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "prefix.inventory.products",
                createEnvelope.schema(), payload);
    }
}
