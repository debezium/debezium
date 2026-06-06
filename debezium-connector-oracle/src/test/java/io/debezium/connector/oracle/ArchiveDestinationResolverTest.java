/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.connector.oracle.OracleConnectorConfig.ARCHIVE_DESTINATION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Unit tests for the {@link ArchiveDestinationNameResolver}.
 *
 * @author Chris Cranford
 */
public class ArchiveDestinationResolverTest {

    @FixFor("DBZ-9041")
    @Test
    void shouldLogUsingArchiveDestinationName() throws Exception {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.isArchiveLogDestinationValid(eq("LOG_ARCHIVE_DEST_1"))).thenReturn(true);

        final Configuration config = Configuration.create().with(ARCHIVE_DESTINATION_NAME, "LOG_ARCHIVE_DEST_1").build();
        final LogInterceptor logInterceptor = new LogInterceptor(ArchiveDestinationNameResolver.class);

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        connectorConfig.getArchiveDestinationNameResolver().validate(connection);

        assertThat(logInterceptor.containsMessage("Using archive destination name: LOG_ARCHIVE_DEST_1")).isTrue();

        final List<String> names = connectorConfig.getArchiveDestinationNameResolver().getDestinationNames(connection);
        assertThat(names).containsExactly("LOG_ARCHIVE_DEST_1");
    }

    @FixFor("DBZ-9041")
    @Test
    void shouldLogNoValidDestinationDetected() {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);

        final Configuration config = Configuration.create().with(ARCHIVE_DESTINATION_NAME, "D1,D2").build();
        final LogInterceptor logInterceptor = new LogInterceptor(ArchiveDestinationNameResolver.class);

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        assertThatThrownBy(() -> connectorConfig.getArchiveDestinationNameResolver().validate(connection))
                .hasMessage("None of the supplied archive destinations are local and valid: [D1, D2]");

        assertThat(logInterceptor.containsMessage("Failed to locate a valid archive destination.")).isTrue();
    }

    @FixFor("DBZ-9041")
    @Test
    void shouldResolveDestinationAsNullWhenNoDestinationSpecified() {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(Configuration.empty());
        assertThatThrownBy(() -> {
            connectorConfig.getArchiveDestinationNameResolver().getDestinationNames(connection);
        }).hasMessage("Failed to locate a local and valid archive destination in Oracle.");
    }

    @FixFor("DBZ-9041")
    @Test
    void shouldResolveDestinationNameFromListOfValidAndInvalidOptions() throws Exception {
        final OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.isArchiveLogDestinationValid(eq("LOG_ARCHIVE_DEST_1"))).thenReturn(true);

        final Configuration config = Configuration.create().with(ARCHIVE_DESTINATION_NAME, "D1,LOG_ARCHIVE_DEST_1").build();
        final LogInterceptor logInterceptor = new LogInterceptor(ArchiveDestinationNameResolver.class);

        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        connectorConfig.getArchiveDestinationNameResolver().validate(connection);

        assertThat(logInterceptor.containsMessage("Using archive destination name: LOG_ARCHIVE_DEST_1")).isTrue();

        final List<String> names = connectorConfig.getArchiveDestinationNameResolver().getDestinationNames(connection);
        assertThat(names).containsExactly("LOG_ARCHIVE_DEST_1");
    }
}
