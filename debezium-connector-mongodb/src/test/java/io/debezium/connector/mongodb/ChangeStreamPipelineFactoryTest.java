/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.mongodb.Filters.FilterConfig;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CursorPipelineOrder;
import io.debezium.data.Envelope;

@RunWith(MockitoJUnitRunner.class)
public class ChangeStreamPipelineFactoryTest {

    private static final List<String> SIZE_PIPELINE = List.of(
            "" +
                    "{\n" +
                    "   \"$match\": {\n" +
                    "       \"$and\": [ {\n" +
                    "           \"$expr\": {\n \"$lte\": [{\"$bsonSize\": \"$fullDocument\"}, 42]}\n" +
                    "       }, {\n" +
                    "           \"$expr\": { \"$lte\": [{\"$bsonSize\": \"$fullDocumentBeforeChange\"}, 42]}}" +
                    "   ] }\n" +
                    "}");

    private static final List<String> INTERNAL_PIPELINE = List.of(
            "" +
                    "{\n" +
                    "  \"$replaceRoot\" : {\n" +
                    "    \"newRoot\" : {\n" +
                    "      \"event\" : \"$$ROOT\",\n" +
                    "      \"namespace\" : {\n" +
                    "        \"$concat\" : [ \"$ns.db\", \".\", \"$ns.coll\" ]\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}",
            "" +
                    "{\n" +
                    "  \"$match\" : {\n" +
                    "    \"$and\" : [ {\n" +
                    "      \"namespace\" : {\n" +
                    "        \"$regularExpression\" : {\n" +
                    "          \"pattern\" : \"dbit.*\",\n" +
                    "          \"options\" : \"i\"\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }, {\n" +
                    "      \"event.operationType\" : {\n" +
                    "        \"$in\" : [ \"insert\", \"update\", \"replace\", \"delete\" ]\n" +
                    "      }\n" +
                    "    } ]\n" +
                    "  }\n" +
                    "}",
            "" +
                    "{\n" +
                    "  \"$replaceRoot\" : {\n" +
                    "    \"newRoot\" : \"$event\"\n" +
                    "  }\n" +
                    "}");

    private static final List<String> USER_PIPELINE = List.of(
            "{\n" +
                    "  \"$match\" : {\n" +
                    "    \"$and\" : [ {\n" +
                    "      \"operationType\" : \"insert\"\n" +
                    "    }, {\n" +
                    "      \"fullDocument.eventId\" : 1404\n" +
                    "    } ]\n" +
                    "  }\n" +
                    "}");

    @InjectMocks
    private ChangeStreamPipelineFactory sut;

    @Mock
    private MongoDbConnectorConfig connectorConfig;
    @Mock
    private FilterConfig filterConfig;

    @Test
    public void testCreateWithInternalFirstAndOversizeHandlingFail() {
        testCreate(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE, USER_PIPELINE));
    }

    @Test
    public void testCreateWithUserFirstAndOversizeHandlingFail() {
        testCreate(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
    }

    @Test
    public void testCreateWithInternalFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE, USER_PIPELINE));
    }

    @Test
    public void testCreateWithUserFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
    }

    @SafeVarargs
    private List<String> mergeStages(List<String>... stages) {
        return Stream.of(stages)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private String asJsonArray(List<String> stages) {
        return stages.stream().collect(Collectors.joining(",", "[", "]"));
    }

    public void testCreate(CursorPipelineOrder pipelineOrder, List<String> expectedStageJsons) {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(pipelineOrder);

        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE)); // The default
        given(filterConfig.getCollectionIncludeList())
                .willReturn("dbit.*");
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline(asJsonArray(USER_PIPELINE)));

        // When:
        var pipeline = sut.create();

        // Then:
        assertPipelineStagesEquals(pipeline.getStages(), expectedStageJsons);
    }

    public void testCreateWithSkipOversized(CursorPipelineOrder pipelineOrder, List<String> expectedStageJsons) {
        // Given:
        given(connectorConfig.getOversizeHandlingMode())
                .willReturn(MongoDbConnectorConfig.OversizeHandlingMode.SKIP);
        given(connectorConfig.getOversizeSkipThreshold())
                .willReturn(42);

        testCreate(pipelineOrder, mergeStages(SIZE_PIPELINE, expectedStageJsons));
    }

    private static void assertPipelineStagesEquals(List<? extends Bson> stages, List<String> expectedStageJsons) {
        assertThat(stages)
                .hasSameSizeAs(expectedStageJsons);

        for (int i = 0; i < stages.size(); i++) {
            var expectedStageJson = expectedStageJsons.get(i);
            assertThat(stages)
                    .element(i)
                    .satisfies((stage) -> assertJsonEquals(stage.toBsonDocument().toJson(), expectedStageJson));
        }
    }

    private static void assertJsonEquals(String actual, String expected) {
        try {
            var mapper = new ObjectMapper();
            var actualNode = mapper.readTree(actual);
            var expectedNode = mapper.readTree(expected);
            assertThat(actualNode).isEqualTo(expectedNode);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}