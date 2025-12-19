/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.util.Map;

import kafka.server.KafkaConfig;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * A utility class to format Kafka storage directories.
 * Required from Kafka 4 onwards.
 *
 * @author Jiri Pechanec
 */
public class KafkaStorageFormatter {

    private static final String DEFAULT_CLUSTER_ID = "ldlg5yhkQeWtnUrZrC6edg";

    private final KafkaConfig config;
    private final Namespace namespace;

    public KafkaStorageFormatter(KafkaConfig config, boolean standalone, String clusterId) {
        this.config = config;
        this.namespace = new Namespace(Map.of(
                "standalone", standalone,
                "cluster_id", clusterId));
    }

    public KafkaStorageFormatter(KafkaConfig config) {
        this(config, true, DEFAULT_CLUSTER_ID);
    }

    public void format() {
        kafka.tools.StorageTool$.MODULE$.runFormatCommand(namespace, config, System.out);
    }
}