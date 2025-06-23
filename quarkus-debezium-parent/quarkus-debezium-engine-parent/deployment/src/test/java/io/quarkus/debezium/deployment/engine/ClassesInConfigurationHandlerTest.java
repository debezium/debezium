/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ClassesInConfigurationHandlerTest {

    private static final Map<String, String> COMPLETE_CONFIGURATION = Map.of(
            "transforms.t0.add.fields", "op,table",
            "transforms.t0.add.headers", "db,table",
            "transforms.t0.negate", "false",
            "transforms.t0.predicate", "p2",
            "transforms.t0.type", "io.debezium.transforms.ExtractNewRecordState",
            "transforms", "t0",
            "predicates.p2.pattern", "inventory.inventory.products",
            "predicates.p2.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
            "predicates", "p2",
            "enabled", "false");

    private static final Map<String, String> CONFIGURATION_WITH_MULTIPLE_TRANSFORMATION = Map.of(
            "transforms.t0.add.fields", "op,table",
            "transforms.t0.add.headers", "db,table",
            "transforms.t0.negate", "false",
            "transforms.t0.predicate", "p2",
            "transforms.t0.type", "io.debezium.transforms.ExtractNewRecordState",
            "transforms", "t0,t1",
            "transforms.t1.add.fields", "op,table",
            "transforms.t1.add.headers", "db,table",
            "transforms.t1.type", "io.debezium.transforms.ExtractNewRecordState");

    private static final Map<String, String> CONFIGURATION_WITHOUT_TRANSFORMATION = Map.of(
            "predicates.p2.pattern", "inventory.inventory.products",
            "predicates.p2.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
            "predicates", "p2",
            "enabled", "false");

    private static final Map<String, String> CONFIGURATION_WITH_POST_PROCESSING = Map.of(
            "post.processors", "reselector",
            "reselector.type", "io.debezium.processors.reselect.ReselectColumnsPostProcessor",
            "reselector.reselect.unavailable.values", "true",
            "reselector.reselect.null.values", "true",
            "reselector.reselect.use.event.key", "false",
            "reselector.reselect.error.handling.mode", "WARN");

    @Test
    @DisplayName("should return empty when there are no transforms")
    void shouldBeEmptyWithoutTransforms() {
        ClassesInConfigurationHandler underTest = ClassesInConfigurationHandler.TRANSFORM;

        assertThat(underTest.extract(CONFIGURATION_WITHOUT_TRANSFORMATION)).isEmpty();
    }

    @Test
    @DisplayName("should return one class when there is one transforms")
    void shouldBeOneElementWhenThereIsOneTransforms() {
        ClassesInConfigurationHandler underTest = ClassesInConfigurationHandler.TRANSFORM;

        assertThat(underTest.extract(COMPLETE_CONFIGURATION)).containsExactly("io.debezium.transforms.ExtractNewRecordState");
    }

    @Test
    @DisplayName("should return multiple classes when there is more than one transform")
    void shouldBeMultipleElementWhenThereIsMoreThanOneTransforms() {
        ClassesInConfigurationHandler underTest = ClassesInConfigurationHandler.TRANSFORM;

        assertThat(underTest.extract(CONFIGURATION_WITH_MULTIPLE_TRANSFORMATION)).containsExactly(
                "io.debezium.transforms.ExtractNewRecordState",
                "io.debezium.transforms.ExtractNewRecordState");
    }

    @Test
    @DisplayName("should return multiple classes when there is more than one transform")
    void shouldBeMultipleElementWhenThereIsMoreThanOnePostProcessor() {
        ClassesInConfigurationHandler underTest = ClassesInConfigurationHandler.POST_PROCESSOR;

        assertThat(underTest.extract(CONFIGURATION_WITH_POST_PROCESSING)).containsExactly(
                "io.debezium.processors.reselect.ReselectColumnsPostProcessor");
    }

    @Test
    @DisplayName("should return one class when there is one predicate")
    void shouldBeOneElementWhenThereIsOnePredicate() {
        ClassesInConfigurationHandler underTest = ClassesInConfigurationHandler.PREDICATE;

        assertThat(underTest.extract(COMPLETE_CONFIGURATION)).containsExactly("org.apache.kafka.connect.transforms.predicates.TopicNameMatches");
    }
}