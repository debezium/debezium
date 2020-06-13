/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;
import io.debezium.server.TestDatabase;

public class S3TestConfigSource extends TestConfigSource {

    public static final String S3_REGION = "eu-central-1";
    public static final String S3_BUCKET = "test-bucket";

    final Map<String, String> s3Test = new HashMap<>();

    public S3TestConfigSource() {
        s3Test.put("debezium.sink.type", "s3");
        s3Test.put("debezium.sink.s3.region", S3_REGION);
        s3Test.put("debezium.sink.s3.endpointoverride", "http://localhost:9001");
        s3Test.put("debezium.sink.s3.bucket.name", S3_BUCKET);
        s3Test.put("debezium.sink.s3.objectkey.prefix", "debezium-server-");
        s3Test.put("debezium.sink.s3.credentials.profile", "default");
        s3Test.put("debezium.sink.s3.credentials.useinstancecred", "false");
        s3Test.put("debezium.sink.s3.s3batch.maxeventsperbatch", "2");
        //
        // s3Test.put("debezium.format.value.schemas.enable", "true");
        // s3Test.put("debezium.format.value.converter", "io.debezium.converters.CloudEventsConverter");
        s3Test.put("value.converter", "io.debezium.converters.CloudEventsConverter");
        // s3Test.put("debezium.format.value.converter.data.serializer.type" , "json");
        s3Test.put("value.converter.data.serializer.type", "json");

        // s3Test.put("debezium.format.key", "avro");
        // s3Test.put("debezium.format.key.converter", "io.confluent.connect.avro.AvroConverter");
        /*
         * s3Test.put("debezium.format.key", "avro");
         * s3Test.put("debezium.format.key.converter", "io.confluent.connect.avro.AvroConverter");
         * s3Test.put("debezium.format.value", "avro");
         * s3Test.put("debezium.format.value.converter", "io.confluent.connect.avro.AvroConverter");
         * s3Test.put("debezium.format.key.schema.registry.url", "http://localhost:8081");
         * s3Test.put("debezium.format.value.schema.registry.url", "http://localhost:8081");
         */
        s3Test.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        s3Test.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        s3Test.put("debezium.source.offset.flush.interval.ms", "0");
        s3Test.put("debezium.source.database.hostname", TestDatabase.POSTGRES_HOST);
        s3Test.put("debezium.source.database.port", Integer.toString(TestDatabase.POSTGRES_PORT));
        s3Test.put("debezium.source.database.user", TestDatabase.POSTGRES_USER);
        s3Test.put("debezium.source.database.password", TestDatabase.POSTGRES_PASSWORD);
        s3Test.put("debezium.source.database.dbname", TestDatabase.POSTGRES_DBNAME);
        s3Test.put("debezium.source.database.server.name", "testc");
        s3Test.put("debezium.source.schema.whitelist", "inventory");
        s3Test.put("debezium.source.table.whitelist", "inventory.customers");

        config = s3Test;
    }
}
