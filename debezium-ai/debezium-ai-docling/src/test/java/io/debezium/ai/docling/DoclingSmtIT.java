/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.docling;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.data.Envelope;
import io.debezium.junit.SkipLongRunning;
import io.debezium.junit.SkipTestRule;

import ai.docling.testcontainers.serve.DoclingServeContainer;
import ai.docling.testcontainers.serve.config.DoclingServeContainerConfig;

/**
 * Integrations tests for {@link FieldToDocling} SMT.
 *
 * @author vjuranek
 */
@SkipLongRunning("Downloading Docling container takes too long")
public class DoclingSmtIT {

    @Rule
    public final TestRule skipLongRunning = new SkipTestRule();

    private static final String DOCLING_IMAGE_NAME = "quay.io/docling-project/docling-serve:v1.9.0";

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("mysql.inventory.products.Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("price", Schema.FLOAT32_SCHEMA)
            .field("product", Schema.STRING_SCHEMA)
            .field("manual", Schema.STRING_SCHEMA)
            .build();

    public static final Struct ROW = new Struct(VALUE_SCHEMA)
            .put("id", 101L)
            .put("price", 20.0F)
            .put("product", "a product")
            .put("manual", "= Manual\nThis is a manual how to use this product.");

    public static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("mysql.inventory.products.Envelope")
            .withRecord(VALUE_SCHEMA)
            .withSource(Schema.STRING_SCHEMA)
            .build();

    public static final Struct PAYLOAD = ENVELOPE.create(ROW, null, Instant.now());
    public static final SourceRecord SOURCE_RECORD = new SourceRecord(new HashMap<>(), new HashMap<>(), "topic", ENVELOPE.schema(), PAYLOAD);

    private final FieldToDocling<SourceRecord> doclingSmt = new FieldToDocling<>();

    private final DoclingServeContainer doclingContainer = new DoclingServeContainer(
            DoclingServeContainerConfig.builder()
                    .image(DOCLING_IMAGE_NAME)
                    .enableUi(false)
                    .build());

    @Before
    public void startDoclingServe() {
        doclingContainer.start();
    }

    @After
    public void stopDoclingServe() {
        doclingContainer.stop();
    }

    @Test
    public void testAsciidocToMarkdown() throws InterruptedException, IOException {
        assertDoclingSmtForConfig(Map.of(
                "field.source", "after.manual",
                "field.docling", "after.docling",
                "serve.url", String.format("http://%S:%d", doclingContainer.getHost(), doclingContainer.getPort()),
                "input.source", "text",
                "input.format", "asciidoc",
                "output.format", "markdown"));
    }

    private void assertDoclingSmtForConfig(Map<String, ?> config) throws InterruptedException, IOException {
        doclingSmt.configure(config);
        SourceRecord transformedRecord = doclingSmt.apply(SOURCE_RECORD);

        Struct payloadStruct = (Struct) transformedRecord.value();
        assertThat(payloadStruct.getStruct("after").getString("manual")).contains("= Manual\nThis is a manual how to use this product.");
        String doclingContent = payloadStruct.getStruct("after").getString("docling");
        assertThat(doclingContent).isEqualTo("# Manual\n\nThis is a manual how to use this product.");
    }
}
