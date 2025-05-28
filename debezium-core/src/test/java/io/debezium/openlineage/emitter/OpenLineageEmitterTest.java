/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.openlineage.DebeziumOpenLineageConfiguration;
import io.debezium.openlineage.OpenLineageContext;
import io.debezium.openlineage.OpenLineageJobIdentifier;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.openlineage.dataset.DefaultInputDatasetNamespaceResolver;
import io.debezium.openlineage.dataset.DefaultOutputDatasetNamespaceResolver;
import io.openlineage.client.OpenLineage;

@RunWith(MockitoJUnitRunner.class)
public class OpenLineageEmitterTest {

    @Mock
    private OpenLineageEventEmitter eventEmitter;

    private OpenLineageEmitter emitter;

    @Captor
    ArgumentCaptor<OpenLineage.RunEvent> eventCaptor;

    @Before
    public void setUp() {

        Map<String, String> configMap = new HashMap<>();
        configMap.put("database.hostname", "localhost");
        configMap.put("database.port", "3306");

        Configuration config = Configuration.from(configMap);

        emitter = new OpenLineageEmitter(
                "mysql",
                config,
                new OpenLineageContext(
                        new OpenLineage(URI.create("http://producer.io")),
                        new DebeziumOpenLineageConfiguration(
                                true,
                                new DebeziumOpenLineageConfiguration.Config("opnelineage.yml"),
                                new DebeziumOpenLineageConfiguration.Job(
                                        "namespace",
                                        "description",
                                        Map.of("tag1", "tagValue"),
                                        Map.of("owner1", "ownervalue"))),
                        new OpenLineageJobIdentifier("namespace", "test-connector")),
                eventEmitter,
                new DefaultInputDatasetNamespaceResolver(),
                new DefaultOutputDatasetNamespaceResolver());
    }

    @Test
    public void testEmitInitialState() {
        emitter.emit(BaseSourceTask.State.INITIAL);

        verify(eventEmitter).emit(eventCaptor.capture());
        OpenLineage.RunEvent event = eventCaptor.getValue();

        assertEquals(OpenLineage.RunEvent.EventType.START, event.getEventType());
        assertNotNull(event.getRun());
        assertEquals("test-connector", event.getJob().getName());
    }

    @Test
    public void testEmitWithError() {
        Throwable error = new RuntimeException("Test failure");

        emitter.emit(BaseSourceTask.State.RESTARTING, error);

        verify(eventEmitter).emit(eventCaptor.capture());
        OpenLineage.RunEvent event = eventCaptor.getValue();

        assertEquals(OpenLineage.RunEvent.EventType.FAIL, event.getEventType());
        Assertions.assertThat(event.getRun().getFacets().getErrorMessage().getMessage()).isEqualTo("Test failure");
    }

    @Test
    public void testEmitWithInputAndOutputDatasets() {
        DatasetMetadata.FieldDefinition field = new DatasetMetadata.FieldDefinition("id", "int", "Identifier", Collections.emptyList());
        DatasetMetadata inputDataset = new DatasetMetadata("input_table", DatasetMetadata.DatasetType.INPUT, List.of(field));
        DatasetMetadata outputDataset = new DatasetMetadata("output_table", DatasetMetadata.DatasetType.OUTPUT, List.of(field));

        emitter.emit(BaseSourceTask.State.RUNNING, List.of(inputDataset, outputDataset));

        verify(eventEmitter).emit(eventCaptor.capture());
        OpenLineage.RunEvent event = eventCaptor.getValue();

        assertEquals(OpenLineage.RunEvent.EventType.RUNNING, event.getEventType());
        assertEquals(1, event.getInputs().size());
        assertEquals("input_table", event.getInputs().get(0).getName());
        assertEquals("mysql://localhost:3306", event.getInputs().get(0).getNamespace());

        assertEquals(1, event.getOutputs().size());
        assertEquals("output_table", event.getOutputs().get(0).getName());
        assertEquals("kafka://unknown:unknown", event.getOutputs().get(0).getNamespace());
    }

    @Test
    public void testEmitComplete() {
        emitter.emit(BaseSourceTask.State.STOPPED);

        verify(eventEmitter).emit(eventCaptor.capture());
        OpenLineage.RunEvent event = eventCaptor.getValue();

        assertEquals(OpenLineage.RunEvent.EventType.COMPLETE, event.getEventType());
    }

}
