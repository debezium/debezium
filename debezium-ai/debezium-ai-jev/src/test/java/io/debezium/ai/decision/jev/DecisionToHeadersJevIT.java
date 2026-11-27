/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.jev;

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
 * Integration test of the {@link DecisionToHeaders} transformation with the real Jev (TypeSafe) API. Skipped unless the
 * {@code TYPESAFE_API_KEY} environment variable is set. The tests use synthetic data only.
 *
 * @author Jiri Pechanec
 */
@EnabledIfEnvironmentVariable(named = "TYPESAFE_API_KEY", matches = ".+")
public class DecisionToHeadersJevIT {

    private static final String PREFIX = "__debezium.ai.decision.";

    @Test
    public void shouldWriteDecisionHeadersForAnEnvelopeRecord() {
        final var config = new HashMap<String, String>(JevDecisionServiceIT.jevConfig(System.getenv("TYPESAFE_API_KEY")));
        config.putAll(Map.of(
                "state.fields", "after.name,after.description",
                "questions", "category,emergency",
                "questions.category.type", "choice",
                "questions.category.instructions", "Which team should handle this product?",
                "questions.category.criteria.MARKETING", "Promotion and positioning",
                "questions.category.criteria.SAFETY", "Hazards and recalls",
                "questions.category.criteria.SHIPPING", "Logistics and delivery",
                "questions.emergency.type", "yesno",
                "questions.emergency.instructions", "Does the product require immediate intervention?"));

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
            assertThat(headers.lastWithName(PREFIX + "emergency").schema()).isSameAs(Schema.FLOAT64_SCHEMA);
            assertThat((Double) headers.lastWithName(PREFIX + "emergency").value()).isBetween(0.0, 1.0);
        }
    }
}
