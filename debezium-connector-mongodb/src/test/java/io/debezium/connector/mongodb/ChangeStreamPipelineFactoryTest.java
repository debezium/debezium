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
import java.util.Optional;
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

    private static final List<String> INTERNAL_PIPELINE_LITERALS = List.of(
            "" +
                    "{\n" +
                    "  \"$match\": {\n" +
                    "    \"$and\": [{\n" +
                    "        \"$or\": [\n" +
                    "          { \"ns\": { \"$in\": [ { \"db\": \"dbit\", \"coll\": \"col1\" }, { \"db\": \"dbit\", \"coll\": \"col2\" } ] } },\n" +
                    "          { \"ns\": { \"db\": \"signal\", \"coll\": \"col\" } }\n" +
                    "        ]\n" +
                    "      }, {\n" +
                    "        \"operationType\": {\n" +
                    "          \"$in\": [\"insert\", \"update\", \"replace\", \"delete\" ]\n" +
                    "        }\n" +
                    "      }]\n" +
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
        testCreateLiterals(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE_LITERALS, USER_PIPELINE));
    }

    @Test
    public void testCreateWithUserFirstAndOversizeHandlingFail() {
        testCreate(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
        testCreateLiterals(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE_LITERALS));
    }

    @Test
    public void testCreateWithInternalFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE, USER_PIPELINE));
        testCreateLiteralsWithSkipOversized(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE_LITERALS, USER_PIPELINE));
    }

    @Test
    public void testCreateWithUserFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
        testCreateLiteralsWithSkipOversized(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE_LITERALS));
    }

    @Test
    public void testCreateWithUserOnly() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.USER_ONLY);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE)); // The default
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of("dbit.*"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline(asJsonArray(USER_PIPELINE)));

        // When:
        var pipeline = sut.create();

        // Then:
        assertPipelineStagesEquals(pipeline.getStages(), USER_PIPELINE);
    }

    @Test
    public void testCollectionIncludeListWithSpacesAfterCommas() {

        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of("db1.col1, db2.col2 , db3.col3"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        // Verify the regex pattern in the pipeline doesn't have spaces after pipes
        var pattern = extractNamespaceRegexPattern(pipeline);
        assertThat(pattern).isEqualTo("db1.col1|db2.col2|db3.col3");
        assertThat(pattern).doesNotContain("| ");
    }

    @Test
    public void testCollectionIncludeListWithMultipleSpaces() {
        // Test case with multiple spaces and mixed formatting
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of("db1.col1, db2.col2 , db3.col3"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pattern = extractNamespaceRegexPattern(pipeline);
        // Verify all spaces are trimmed and no spaces after pipes
        assertThat(pattern).isEqualTo("db1.col1|db2.col2|db3.col3");
        assertThat(pattern).doesNotContain("| ");
        assertThat(pattern).doesNotContain(" |");
    }

    @Test
    public void testDatabaseIncludeListWithSpacesAfterCommas() {
        // Test case for database include list with spaces after commas
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.getDbIncludeList())
                .willReturn(Optional.of("db1, db2, db3"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pattern = extractDatabaseRegexPattern(pipeline);
        // Verify no space after pipe character
        assertThat(pattern).isEqualTo("db1|db2|db3");
        assertThat(pattern).doesNotContain("| ");
    }

    @Test
    public void testCollectionExcludeListWithSpacesAfterCommas() {
        // Test case for collection exclude list with spaces after commas
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.getCollectionExcludeList())
                .willReturn(Optional.of("db1.col1, db2.col2"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pattern = extractNamespaceRegexPattern(pipeline);
        // Verify negative lookahead pattern doesn't have spaces after pipes
        assertThat(pattern).startsWith("(?!");
        assertThat(pattern).endsWith(")");
        var innerPattern = pattern.substring(3, pattern.length() - 1);
        assertThat(innerPattern).isEqualTo("db1.col1|db2.col2");
        assertThat(innerPattern).doesNotContain("| ");
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
                .willReturn(Optional.of("dbit.*"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline(asJsonArray(USER_PIPELINE)));

        // When:
        var pipeline = sut.create();

        // Then:
        assertPipelineStagesEquals(pipeline.getStages(), expectedStageJsons);
    }

    public void testCreateLiterals(CursorPipelineOrder pipelineOrder, List<String> expectedStageJsons) {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(pipelineOrder);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE)); // The default
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(true);
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of("dbit.col1,dbit.col2"));
        given(filterConfig.getSignalDataCollection())
                .willReturn(Optional.of("signal.col"));
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

    public void testCreateLiteralsWithSkipOversized(CursorPipelineOrder pipelineOrder, List<String> expectedStageJsons) {
        // Given:
        given(connectorConfig.getOversizeHandlingMode())
                .willReturn(MongoDbConnectorConfig.OversizeHandlingMode.SKIP);
        given(connectorConfig.getOversizeSkipThreshold())
                .willReturn(42);

        testCreateLiterals(pipelineOrder, mergeStages(SIZE_PIPELINE, expectedStageJsons));
    }

    private static void assertPipelineStagesEquals(List<? extends Bson> stages, List<String> expectedStageJsons) {
        assertThat(stages).hasSameSizeAs(expectedStageJsons);

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

    /**
     * Extracts the namespace regex pattern from the change stream pipeline.
     * Handles different pipeline structures (with or without $or wrapper).
     */
    private static String extractNamespaceRegexPattern(ChangeStreamPipeline pipeline) {
        var stages = pipeline.getStages();
        var matchStage = stages.stream()
                .filter(stage -> stage.toBsonDocument().containsKey("$match"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No $match stage found in pipeline"));

        var matchDoc = matchStage.toBsonDocument().getDocument("$match");
        var andArray = matchDoc.getArray("$and");
        
        // Recursively search for namespace regex pattern
        for (var andElement : andArray) {
            var pattern = findNamespacePatternInDocument(andElement.asDocument());
            if (pattern != null) {
                return pattern;
            }
        }
        
        throw new AssertionError("No namespace regex pattern found in pipeline");
    }

    /**
     * Recursively searches for namespace regex pattern in a BSON document.
     */
    private static String findNamespacePatternInDocument(org.bson.BsonDocument doc) {
        // Check if namespace is directly in this document
        if (doc.containsKey("namespace")) {
            var namespaceValue = doc.get("namespace");
            if (namespaceValue.isDocument()) {
                var namespaceDoc = namespaceValue.asDocument();
                if (namespaceDoc.containsKey("$regularExpression")) {
                    return namespaceDoc.getDocument("$regularExpression")
                            .getString("pattern").getValue();
                }
            }
        }
        
        // Check if it's wrapped in $or
        if (doc.containsKey("$or")) {
            var orArray = doc.getArray("$or");
            for (var orElement : orArray) {
                var pattern = findNamespacePatternInDocument(orElement.asDocument());
                if (pattern != null) {
                    return pattern;
                }
            }
        }
        
        // Check if it's wrapped in $and (nested)
        if (doc.containsKey("$and")) {
            var andArray = doc.getArray("$and");
            for (var andElement : andArray) {
                var pattern = findNamespacePatternInDocument(andElement.asDocument());
                if (pattern != null) {
                    return pattern;
                }
            }
        }
        
        return null;
    }

    /**
     * Extracts the database regex pattern from the change stream pipeline.
     * Handles different pipeline structures (with or without $or wrapper).
     */
    private static String extractDatabaseRegexPattern(ChangeStreamPipeline pipeline) {
        var stages = pipeline.getStages();
        var matchStage = stages.stream()
                .filter(stage -> stage.toBsonDocument().containsKey("$match"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No $match stage found in pipeline"));

        var matchDoc = matchStage.toBsonDocument().getDocument("$match");
        var andArray = matchDoc.getArray("$and");
        
        // Recursively search for database regex pattern
        for (var andElement : andArray) {
            var pattern = findDatabasePatternInDocument(andElement.asDocument());
            if (pattern != null) {
                return pattern;
            }
        }
        
        throw new AssertionError("No database regex pattern found in pipeline");
    }

    /**
     * Recursively searches for database regex pattern in a BSON document.
     */
    private static String findDatabasePatternInDocument(org.bson.BsonDocument doc) {
        // Check if event.ns.db is directly in this document
        if (doc.containsKey("event.ns.db")) {
            var dbValue = doc.get("event.ns.db");
            if (dbValue.isDocument()) {
                var dbDoc = dbValue.asDocument();
                if (dbDoc.containsKey("$regularExpression")) {
                    return dbDoc.getDocument("$regularExpression")
                            .getString("pattern").getValue();
                }
            }
        }
        
        // Check if it's wrapped in $or
        if (doc.containsKey("$or")) {
            var orArray = doc.getArray("$or");
            for (var orElement : orArray) {
                var pattern = findDatabasePatternInDocument(orElement.asDocument());
                if (pattern != null) {
                    return pattern;
                }
            }
        }
        
        // Check if it's wrapped in $and (nested)
        if (doc.containsKey("$and")) {
            var andArray = doc.getArray("$and");
            for (var andElement : andArray) {
                var pattern = findDatabasePatternInDocument(andElement.asDocument());
                if (pattern != null) {
                    return pattern;
                }
            }
        }
        
        return null;
    }
}