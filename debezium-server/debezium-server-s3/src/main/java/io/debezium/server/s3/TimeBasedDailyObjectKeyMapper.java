/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;

public class TimeBasedDailyObjectKeyMapper implements ObjectKeyMapper {
    final String objectKeyPrefix = ConfigProvider.getConfig().getValue("debezium.sink.s3.objectkey.prefix", String.class);
    final String valueFormat = ConfigProvider.getConfig().getOptionalValue("debezium.format.value", String.class).orElse(Json.class.getSimpleName().toLowerCase());

    @Override
    public String map(String destination, LocalDateTime batchTime, String recordId) {
        Objects.requireNonNull(destination, "destination Cannot be Null");
        Objects.requireNonNull(batchTime, "batchTime Cannot be Null");
        Objects.requireNonNull(recordId, "recordId Cannot be Null");
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + recordId + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }

    @Override
    public String map(String destination, LocalDateTime batchTime, Integer batchId) {
        Objects.requireNonNull(destination, "destination Cannot be Null");
        Objects.requireNonNull(batchTime, "batchTime Cannot be Null");
        Objects.requireNonNull(batchId, "batchId Cannot be Null");
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + "-" + batchId + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }
}
