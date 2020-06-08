/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.debezium.server.s3.objectkeymapper;

import io.debezium.engine.format.Json;
import org.eclipse.microprofile.config.ConfigProvider;

import java.time.LocalDateTime;

public class DefaultObjectKeyMapper implements ObjectKeyMapper {
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @Override
    public String map(String destination, LocalDateTime batchTime, String recordId) {
        String fname = batchTime.toString() + "-" + recordId + "." + valueFormat;
        return objectKeyPrefix + destination + "/" + fname;
    }

    @Override
    public String map(String destination, LocalDateTime batchTime, int batchId) {
        String fname = batchTime.toString() + "-" + batchId + "." + valueFormat;
        return objectKeyPrefix + destination + "/" + fname;
    }
}
