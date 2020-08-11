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
import io.debezium.server.TestDatabase;

public class KinesisTestConfigSource extends TestConfigSource {

    public static final String KINESIS_REGION = "eu-central-1";

    final Map<String, String> kinesisTest = new HashMap<>();

    public KinesisTestConfigSource() {
        kinesisTest.put("debezium.sink.type", "kinesis");
        kinesisTest.put("debezium.sink.kinesis.region", KINESIS_REGION);
        kinesisTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        kinesisTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        kinesisTest.put("debezium.source.offset.flush.interval.ms", "0");
        kinesisTest.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        kinesisTest.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        kinesisTest.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        kinesisTest.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        kinesisTest.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        kinesisTest.put("debezium.source.database.server.name", "testc");
        kinesisTest.put("debezium.source.schema.include.list", "inventory");
        kinesisTest.put("debezium.source.table.include.list", "inventory.customers");

        config = kinesisTest;
    }
}
