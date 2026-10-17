/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.Filters.FilterConfig;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CursorPipelineOrder;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.PostImageMode;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.PreImageMode;
import io.debezium.data.Envelope;

@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    void setUp() {
        lenient().when(connectorConfig.getCaptureModePreImage()).thenReturn(PreImageMode.OFF);
        lenient().when(connectorConfig.getCaptureModePostImage()).thenReturn(PostImageMode.LOOKUP);
    }

    @ParameterizedTest
    @CsvSource({
            "required, lookup, ",
            "off, post_image_required, ",
            ", , post_image_required"
    })
    void testCreateWithRequiredImagesMatchesRegexesOnOriginalEvent(String preImage, String postImage, String legacyPostImage) {
        var builder = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.col1,dbit.col2")
                .with(MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION, "signal.col");
        if (preImage != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, preImage);
        }
        if (postImage != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_MODE_POST_IMAGE, postImage);
        }
        if (legacyPostImage != null) {
            builder.with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_update_full")
                    .with(MongoDbConnectorConfig.CAPTURE_MODE_FULL_UPDATE_TYPE, legacyPostImage);
        }

        var pipeline = create(builder.build());

        assertPipelineStagesEquals(pipeline.getStages(), List.of(
                "" +
                        "{\n" +
                        "  \"$match\": {\n" +
                        "    \"$and\": [{\n" +
                        "        \"$or\": [\n" +
                        "          { \"$expr\": { \"$regexMatch\": {\n" +
                        "              \"input\": { \"$concat\": [ \"$ns.db\", \".\", \"$ns.coll\" ] },\n" +
                        "              \"regex\": \"^(?:dbit.col1|dbit.col2)$\",\n" +
                        "              \"options\": \"i\" } } },\n" +
                        "          { \"ns\": { \"db\": \"signal\", \"coll\": \"col\" } }\n" +
                        "        ]\n" +
                        "      }, {\n" +
                        "        \"operationType\": {\n" +
                        "          \"$in\": [\"insert\", \"update\", \"replace\", \"delete\" ]\n" +
                        "        }\n" +
                        "      }]\n" +
                        "  }\n" +
                        "}"));
    }

    @Test
    void testCreateWithRequiredImagesAnchorsExcludeListsAndDatabaseFilters() {
        var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, "db1, db2")
                .with(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST, "db1.a\\,b, db1.c")
                .with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, "required")
                .build();

        var pipeline = create(config);

        assertPipelineStagesEquals(pipeline.getStages(), List.of(
                "" +
                        "{\n" +
                        "  \"$match\": {\n" +
                        "    \"$and\": [{\n" +
                        "        \"$and\": [\n" +
                        "          { \"ns.db\": { \"$regularExpression\": { \"pattern\": \"^(?:db1|db2)$\", \"options\": \"i\" } } },\n" +
                        "          { \"$expr\": { \"$regexMatch\": {\n" +
                        "              \"input\": { \"$concat\": [ \"$ns.db\", \".\", \"$ns.coll\" ] },\n" +
                        "              \"regex\": \"^(?!(?:db1.a,b|db1.c)$)\",\n" +
                        "              \"options\": \"i\" } } }\n" +
                        "        ]\n" +
                        "      }, {\n" +
                        "        \"operationType\": {\n" +
                        "          \"$in\": [\"insert\", \"update\", \"replace\", \"delete\" ]\n" +
                        "        }\n" +
                        "      }]\n" +
                        "  }\n" +
                        "}"));
    }

    @Test
    void testCreateWithAvailableImagesKeepsRegexPipeline() {
        var config = TestHelper.getConfiguration().edit()
                .with(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST, "dbit.*")
                .with(MongoDbConnectorConfig.CAPTURE_MODE_PRE_IMAGE, "when_available")
                .with(MongoDbConnectorConfig.CAPTURE_MODE_POST_IMAGE, "post_image")
                .build();

        var pipeline = create(config);

        assertPipelineStagesEquals(pipeline.getStages(), INTERNAL_PIPELINE);
    }

    @Test
    void testCreateWithInternalFirstAndOversizeHandlingFail() {
        testCreate(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE, USER_PIPELINE));
        testCreateLiterals(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE_LITERALS, USER_PIPELINE));
    }

    @Test
    void testCreateWithUserFirstAndOversizeHandlingFail() {
        testCreate(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
        testCreateLiterals(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE_LITERALS));
    }

    @Test
    void testCreateWithInternalFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE, USER_PIPELINE));
        testCreateLiteralsWithSkipOversized(CursorPipelineOrder.INTERNAL_FIRST, mergeStages(INTERNAL_PIPELINE_LITERALS, USER_PIPELINE));
    }

    @Test
    void testCreateWithUserFirstAndOversizeHandlingSkip() {
        testCreateWithSkipOversized(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE));
        testCreateLiteralsWithSkipOversized(CursorPipelineOrder.USER_FIRST, mergeStages(USER_PIPELINE, INTERNAL_PIPELINE_LITERALS));
    }

    @Test
    void testCreateWithUserOnly() {
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
    void testCollectionIncludeListTrimsWhitespace() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(false);
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of("db.col1, db.col2"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pipelineJson = pipelineToString(pipeline);
        assertThat(pipelineJson).contains("\"pattern\"");
        assertThat(pipelineJson).contains("db.col1|db.col2");
    }

    @Test
    void testCollectionIncludeListTrimsAllWhitespace() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(false);
        given(filterConfig.getCollectionIncludeList())
                .willReturn(Optional.of(" db1.col1 ,   db2.col2  ,  db3.col3 "));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pipelineJson = pipelineToString(pipeline);
        assertThat(pipelineJson).contains("\"pattern\"");
        assertThat(pipelineJson).contains("db1.col1|db2.col2|db3.col3");
    }

    @Test
    void testCollectionExcludeListTrimsWhitespace() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(false);
        given(filterConfig.getCollectionExcludeList())
                .willReturn(Optional.of("db1.col1, db2.col2"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pipelineJson = pipelineToString(pipeline);
        assertThat(pipelineJson).contains("\"pattern\"");
        assertThat(pipelineJson).contains("(?!db1.col1|db2.col2)");
    }

    @Test
    void testDatabaseIncludeListTrimsWhitespace() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(false);
        given(filterConfig.getDbIncludeList())
                .willReturn(Optional.of("db1, db2, db3"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pipelineJson = pipelineToString(pipeline);
        assertThat(pipelineJson).contains("\"pattern\"");
        assertThat(pipelineJson).contains("db1|db2|db3");
    }

    @Test
    void testDatabaseExcludeListTrimsWhitespace() {
        // Given:
        given(connectorConfig.getCursorPipelineOrder())
                .willReturn(CursorPipelineOrder.INTERNAL_FIRST);
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE));
        given(filterConfig.isLiteralsMatchMode())
                .willReturn(false);
        given(filterConfig.getDbExcludeList())
                .willReturn(Optional.of("db1, db2 , db3"));
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[]"));

        // When:
        var pipeline = sut.create();

        // Then:
        var pipelineJson = pipelineToString(pipeline);
        assertThat(pipelineJson).contains("\"pattern\"");
        assertThat(pipelineJson).contains("(?!db1|db2|db3)");
    }

    private static ChangeStreamPipeline create(Configuration config) {
        return new ChangeStreamPipelineFactory(new MongoDbConnectorConfig(config), new FilterConfig(config)).create();
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

    private static String pipelineToString(ChangeStreamPipeline pipeline) {
        return pipeline.getStages().stream()
                .map(stage -> stage.toBsonDocument().toJson())
                .collect(Collectors.joining());
    }
}