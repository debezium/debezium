/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.debezium.server.s3.objectkeymapper;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeBasedDailyObjectKeyMapper extends DefaultObjectKeyMapper {

    @Override
    public String map(String destination, LocalDateTime batchTime, String recordId) {
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + recordId + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }

    public String map(String destination, LocalDateTime batchTime, int batchId) {
        String fname = batchTime.toEpochSecond(ZoneOffset.UTC) + "-" + batchId + "." + valueFormat;
        String partiton = "year=" + batchTime.getYear() + "/month=" + StringUtils.leftPad(batchTime.getMonthValue() + "", 2, '0') + "/day="
                + StringUtils.leftPad(batchTime.getDayOfMonth() + "", 2, '0');
        return objectKeyPrefix + destination + "/" + partiton + "/" + fname;
    }
}
