/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * Verifies that the open/close watermark rows written to the signal table carry the id of the
 * signal that requested the snapshot and the data collection the chunk belongs to.
 */
@ExtendWith(MockitoExtension.class)
public class SignalBasedIncrementalSnapshotChangeEventSourceTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JdbcConnection jdbcConnection;
    private List<Map<Integer, String>> signalInserts;
    private SignalBasedIncrementalSnapshotChangeEventSource<TestPartition, TableId> source;
    private SignalBasedIncrementalSnapshotContext<TableId> context;

    interface TestPartition extends io.debezium.pipeline.spi.Partition {
    }

    @BeforeEach
    public void setUp() throws Exception {
        jdbcConnection = mock(JdbcConnection.class);
        signalInserts = new ArrayList<>();
        when(jdbcConnection.quotedTableIdString(any())).thenReturn("debezium.signal");
        when(jdbcConnection.prepareUpdate(anyString(), any(JdbcConnection.StatementPreparer.class))).thenAnswer(invocation -> {
            final Map<Integer, String> row = new HashMap<>();
            final PreparedStatement statement = mock(PreparedStatement.class);
            doAnswer(setter -> row.put(setter.getArgument(0), setter.getArgument(1)))
                    .when(statement).setString(anyInt(), any());
            invocation.getArgument(1, JdbcConnection.StatementPreparer.class).accept(statement);
            signalInserts.add(row);
            return jdbcConnection;
        });

        source = new SignalBasedIncrementalSnapshotChangeEventSource<>(config(), jdbcConnection, null, null, null, null, null, null);
        context = new SignalBasedIncrementalSnapshotContext<>();
        source.context = context;
    }

    private RelationalDatabaseConnectorConfig config() {
        final Configuration configuration = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION, "debezium.signal")
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core")
                .build();
        return new RelationalDatabaseConnectorConfig(configuration, null, null, 0, ColumnFilterMode.CATALOG, true) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }

    @Test
    public void shouldAttributeWatermarksToSignalAndDataCollection() throws Exception {
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a"), List.of(), "");
        context.addDataCollectionNamesToSnapshot("signal-2", List.of("public.b"), List.of(), "");

        context.startNewChunk();
        source.emitWindowOpen(null, null);

        assertThat(signalInserts).hasSize(1);
        assertThat(signalInserts.get(0).get(1)).isEqualTo(context.currentChunkId() + "-open");
        assertThat(signalInserts.get(0).get(2)).isEqualTo(OpenIncrementalSnapshotWindow.NAME);
        JsonNode metadata = mapper.readTree(signalInserts.get(0).get(3));
        assertThat(metadata.get("correlationId").asText()).isEqualTo("signal-1");
        assertThat(metadata.get("dataCollectionId").asText()).isEqualTo("public.a");

        // The queue advances to the next data collection before the close of the current data
        // collection's last chunk is emitted; the close must keep the attribution captured at open.
        final String chunkId = context.currentChunkId();
        context.nextDataCollection();
        source.emitWindowClose(null, null);

        assertThat(signalInserts).hasSize(2);
        assertThat(signalInserts.get(1).get(1)).isEqualTo(chunkId + "-close");
        assertThat(signalInserts.get(1).get(2)).isEqualTo(CloseIncrementalSnapshotWindow.NAME);
        metadata = mapper.readTree(signalInserts.get(1).get(3));
        assertThat(metadata.get("correlationId").asText()).isEqualTo("signal-1");
        assertThat(metadata.get("dataCollectionId").asText()).isEqualTo("public.a");
        assertThat(metadata.get("openWindowTimestamp").asText()).isEqualTo(mapper.readTree(signalInserts.get(0).get(3)).get("openWindowTimestamp").asText());

        // The next chunk belongs to the next data collection, requested by the other signal.
        context.startNewChunk();
        source.emitWindowOpen(null, null);

        assertThat(signalInserts).hasSize(3);
        metadata = mapper.readTree(signalInserts.get(2).get(3));
        assertThat(metadata.get("correlationId").asText()).isEqualTo("signal-2");
        assertThat(metadata.get("dataCollectionId").asText()).isEqualTo("public.b");
    }

    @Test
    public void shouldEmitWatermarksWithoutAttributionWhenCorrelationIdIsUnknown() throws Exception {
        // Snapshots resumed from offsets written by older versions have no per-collection correlation id.
        context.addDataCollectionNamesToSnapshot(null, List.of("public.a"), List.of(), "");

        context.startNewChunk();
        source.emitWindowOpen(null, null);

        final JsonNode metadata = mapper.readTree(signalInserts.get(0).get(3));
        assertThat(metadata.has("correlationId")).isFalse();
        assertThat(metadata.get("dataCollectionId").asText()).isEqualTo("public.a");
    }
}
