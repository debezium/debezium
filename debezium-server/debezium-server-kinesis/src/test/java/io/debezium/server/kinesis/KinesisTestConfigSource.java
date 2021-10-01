/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class KinesisTestConfigSource extends TestConfigSource {

    public static final String KINESIS_REGION = "eu-central-1";

    public KinesisTestConfigSource() {
        Map<String, String> kinesisTest = new HashMap<>();

        kinesisTest.put("debezium.sink.type", "kinesis");
        kinesisTest.put("debezium.sink.kinesis.region", KINESIS_REGION);
        kinesisTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kinesisTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        kinesisTest.put("debezium.source.offset.flush.interval.ms", "0");
        kinesisTest.put("debezium.source.database.server.name", "testc");
        kinesisTest.put("debezium.source.schema.include.list", "inventory");
        kinesisTest.put("debezium.source.table.include.list", "inventory.customers");

        config = kinesisTest;
    }

    @Override
    public int getOrdinal() {
        // Configuration property precedence is based on ordinal values and since we override the
        // properties in TestConfigSource, we should give this a higher priority.
        return super.getOrdinal() + 1;
    }
}
