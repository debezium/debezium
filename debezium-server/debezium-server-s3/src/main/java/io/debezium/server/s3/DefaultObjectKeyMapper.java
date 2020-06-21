/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import java.time.LocalDateTime;

import org.eclipse.microprofile.config.ConfigProvider;

import io.debezium.engine.format.Json;

public class DefaultObjectKeyMapper implements ObjectKeyMapper {
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @Override
    public String map(String destination, LocalDateTime batchTime, String recordId) {
        String fname = batchTime.toString() + "-" + recordId + "." + valueFormat;
        return objectKeyPrefix + destination + "/" + fname;
    }

    @Override
    public String map(String destination, LocalDateTime batchTime, Integer batchId) {
        String fname = batchTime.toString() + "-" + batchId + "." + valueFormat;
        return objectKeyPrefix + destination + "/" + fname;
    }
}
