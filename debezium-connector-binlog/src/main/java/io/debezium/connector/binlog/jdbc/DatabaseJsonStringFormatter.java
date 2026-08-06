/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import java.util.Base64;
import java.util.Locale;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonStringFormatter;

/**
 * A {@link JsonStringFormatter} that renders {@code JSON} values the way the database server does when the
 * column is read via JDBC: a space after each key colon and entry comma, six fractional digits on temporal
 * scalars, and a {@code base64:typeNN:} prefix on opaque scalars.
 */
public class DatabaseJsonStringFormatter extends JsonStringFormatter {

    @Override
    public void name(String name) {
        super.name(name);
        // The buffer is only reachable through appendString, and a space needs no escaping
        appendString(" ");
    }

    @Override
    public void nextEntry() {
        super.nextEntry();
        appendString(" ");
    }

    @Override
    public void valueDatetime(int year, int month, int day, int hour, int min, int sec, int microSeconds) {
        // The base class omits a zero fractional part and trims trailing zeros; the server does neither
        value(String.format(Locale.ROOT, "%s%04d-%02d-%02d %02d:%02d:%02d.%06d",
                year < 0 ? "-" : "", Math.abs(year), month, day, hour, min, sec, microSeconds));
    }

    @Override
    public void valueTime(int hour, int min, int sec, int microSeconds) {
        // A negative TIME carries its sign on the hour component
        value(String.format(Locale.ROOT, "%s%02d:%02d:%02d.%06d",
                hour < 0 ? "-" : "", Math.abs(hour), min, sec, microSeconds));
    }

    @Override
    public void valueOpaque(ColumnType type, byte[] value) {
        value("base64:type" + type.getCode() + ":" + Base64.getEncoder().encodeToString(value));
    }
}
