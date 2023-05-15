/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.mongodb.Filters.FilterConfig;
import io.debezium.data.Envelope;

@RunWith(MockitoJUnitRunner.class)
public class ChangeStreamPipelineFactoryTest {

    @InjectMocks
    private ChangeStreamPipelineFactory sut;

    @Mock
    private MongoDbConnectorConfig connectorConfig;
    @Mock
    private FilterConfig filterConfig;

    @Test
    public void testCreate() {
        // Given:
        given(connectorConfig.getSkippedOperations())
                .willReturn(EnumSet.of(Envelope.Operation.TRUNCATE)); // The default
        given(filterConfig.getCollectionIncludeList())
                .willReturn("dbit.*");
        given(filterConfig.getUserPipeline())
                .willReturn(new ChangeStreamPipeline("[{\"$match\": { \"$and\": [{\"operationType\": \"insert\"}, {\"fullDocument.eventId\": 1404 }] } }]"));

        // When:
        var pipeline = sut.create();

        // Then:
        assertPipelineStagesEquals(pipeline.getStages(),
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
                        "}",
                "" +
                        "{\n" +
                        "  \"$match\" : {\n" +
                        "    \"$and\" : [ {\n" +
                        "      \"operationType\" : \"insert\"\n" +
                        "    }, {\n" +
                        "      \"fullDocument.eventId\" : 1404\n" +
                        "    } ]\n" +
                        "  }\n" +
                        "}");
    }

    private static void assertPipelineStagesEquals(List<? extends Bson> stages, String... expectedStageJsons) {
        assertThat(stages)
                .hasSameSizeAs(expectedStageJsons);

        for (int i = 0; i < stages.size(); i++) {
            var expectedStageJson = expectedStageJsons[i];
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
