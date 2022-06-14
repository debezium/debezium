/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.kafka.history;

import java.util.Objects;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public class KafkaStorageConfiguration {
    public static int validateServerNameIsDifferentFromHistoryTopicName(Configuration config, Field field, Field.ValidationOutput problems) {
        String serverName = config.getString(field);
        String historyTopicName = config.getString(KafkaDatabaseHistory.TOPIC);

        if (Objects.equals(serverName, historyTopicName)) {
            problems.accept(field, serverName, "Must not have the same value as " + KafkaDatabaseHistory.TOPIC.name());
            return 1;
        }

        return 0;
    }
}
