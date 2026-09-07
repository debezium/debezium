/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogMetadataBasedSchemaIT;

/**
 * MySQL integration test for the binlog-metadata-based schema mode (debezium/dbz#978). Adds coverage for
 * MySQL spatial types, which are not available in the same form on MariaDB.
 */
public class MySqlBinlogMetadataSchemaIT extends BinlogMetadataBasedSchemaIT<MySqlConnector> implements MySqlCommon {

    @Test
    public void shouldReconstructSpatialTypesFromBinlogMetadata() throws Exception {
        final Configuration config = metadataModeConfig().build();
        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());

        executeStatements(DATABASE.getDatabaseName(),
                "INSERT INTO spatial_types (c_point, c_geometry) VALUES ("
                        + "ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(8.25 3.22)', 4326))");

        final List<SourceRecord> records = consumeTable("spatial_types", 1);
        assertThat(records).hasSize(1);
        assertThat(operationOf(records.get(0))).isEqualTo("c");

        final Struct after = afterOf(records.get(0));
        assertThat(after.schema().field("c_point")).as("c_point reconstructed").isNotNull();
        assertThat(after.schema().field("c_geometry")).as("c_geometry reconstructed").isNotNull();
        // The GEOMETRY_TYPE metadata carries the spatial subtype, so a POINT column surfaces with the
        // Point logical type including its x/y fields; plain GEOMETRY surfaces as a geometry struct.
        assertThat(after.getStruct("c_point").getFloat64("x")).isEqualTo(1.0);
        assertThat(after.getStruct("c_point").getFloat64("y")).isEqualTo(1.0);
        assertThat(after.getStruct("c_geometry")).isNotNull();

        stopConnector();
    }
}
