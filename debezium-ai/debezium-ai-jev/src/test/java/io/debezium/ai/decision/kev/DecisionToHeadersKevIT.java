/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.kev;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.debezium.ai.decision.DecisionToHeaders;
import io.debezium.ai.decision.ProductRecords;

/**
 * Integration test of the {@link DecisionToHeaders} transformation with the real local Kev API. Skipped unless the
 * {@code KEV_BASE_URL} environment variable is set. The tests use synthetic data only.
 *
 * @author Jiri Pechanec
 */
@EnabledIfEnvironmentVariable(named = "KEV_BASE_URL", matches = ".+")
public class DecisionToHeadersKevIT {

    private static final String PREFIX = "__debezium.ai.decision.";

    @Test
    public void shouldWriteDecisionHeadersForAnEnvelopeRecord() {
        final Map<String, String> config = new HashMap<>();
        config.put("decision.provider", "kev");
        config.put("kev.base.url", System.getenv("KEV_BASE_URL"));
        config.put("kev.request.timeout.ms", "60000");
        final var key = System.getenv("KEV_API_KEY");
        if (key != null) {
            config.put("kev.api.key", key);
        }
        config.putAll(Map.ofEntries(
                Map.entry("state.fields", "after.name,after.description"),
                Map.entry("questions", "category,severity,emergency"),
                Map.entry("questions.category.type", "choice"),
                Map.entry("questions.category.instructions", "Which team should handle this product?"),
                Map.entry("questions.category.criteria.MARKETING", "Promotion and positioning"),
                Map.entry("questions.category.criteria.SAFETY", "Hazards and recalls"),
                Map.entry("questions.category.criteria.SHIPPING", "Logistics and delivery"),
                Map.entry("questions.severity.type", "score"),
                Map.entry("questions.severity.instructions", "How severe is the product defect?"),
                Map.entry("questions.severity.criteria.0", "Cosmetic"),
                Map.entry("questions.severity.criteria.1", "Dangerous"),
                Map.entry("questions.emergency.type", "yesno"),
                Map.entry("questions.emergency.instructions", "Does the product require immediate intervention?")));

        try (var smt = new DecisionToHeaders<SourceRecord>()) {
            smt.configure(config);

            final var result = smt.apply(ProductRecords.create("Space heater", "Overheats and has caused fires, customers must stop using it"));

            final var headers = result.headers();
            assertThat(headers.lastWithName(PREFIX + "status").value()).isEqualTo("SUCCESS");
            assertThat((String) headers.lastWithName(PREFIX + "model").value()).isNotBlank();
            assertThat(headers.lastWithName(PREFIX + "category").schema()).isSameAs(Schema.STRING_SCHEMA);
            assertThat(headers.lastWithName(PREFIX + "category").value()).isIn("MARKETING", "SAFETY", "SHIPPING");
            assertThat(headers.lastWithName(PREFIX + "category.confidence").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
            assertThat(headers.lastWithName(PREFIX + "category.probabilities").schema()).isSameAs(Schema.STRING_SCHEMA);
            assertThat(headers.lastWithName(PREFIX + "severity").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
            assertThat((Double) headers.lastWithName(PREFIX + "severity").value()).isBetween(0.0, 1.0);
            assertThat(headers.lastWithName(PREFIX + "severity.confidence").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
            assertThat(headers.lastWithName(PREFIX + "severity.probabilities").schema()).isSameAs(Schema.STRING_SCHEMA);
            assertThat(headers.lastWithName(PREFIX + "emergency").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
            assertThat((Double) headers.lastWithName(PREFIX + "emergency").value()).isBetween(0.0, 1.0);
        }
    }
}
