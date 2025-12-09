/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.history;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;

import software.amazon.awssdk.regions.Region;

public class S3SchemaHistoryTest {

    @Test
    public void testValidConfig() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
    }

    @Test
    public void testDefaultAutheticationProviderConfig() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
    }

    @Test
    public void testMissingBucket() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        assertThrows(DebeziumException.class, () -> history.configure(config, null, SchemaHistoryListener.NOOP, true));
    }

    @Test
    public void testMissingObjectName() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        assertThrows(DebeziumException.class, () -> history.configure(config, null, SchemaHistoryListener.NOOP, true));
    }

    @Test
    public void testMissingRegion() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        assertThrows(DebeziumException.class, () -> history.configure(config, null, SchemaHistoryListener.NOOP, true));
    }

    @Test
    public void testInvalidRegion() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, "incorrect")
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "http://localhost:3421")
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
    }

    @Test
    public void testMissingEndpoint() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
    }

    @Test
    public void testInvalidEndpoint() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "aa")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "bb")
                .with(S3SchemaHistory.BUCKET_CONFIG, "bucket")
                .with(S3SchemaHistory.OBJECT_NAME, "debezium-history")
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, "broken url")
                .build();
        assertThrows(IllegalArgumentException.class, () -> history.configure(config, null, SchemaHistoryListener.NOOP, true));
    }

}
