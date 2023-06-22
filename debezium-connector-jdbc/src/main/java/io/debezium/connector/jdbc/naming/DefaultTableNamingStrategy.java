/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default implementation of the {@link TableNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}.
 *
 * @author Chris Cranford
 */
public class DefaultTableNamingStrategy implements TableNamingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTableNamingStrategy.class);

    private final Pattern sourcePattern = Pattern.compile("\\$\\{(source\\.)(.*?)}");

    @Override
    public String resolveTableName(JdbcSinkConnectorConfig config, SinkRecord record) {
        // Default behavior is to replace dots with underscores
        final String topicName = record.topic().replace(".", "_");
        String table = config.getTableNameFormat().replace("${topic}", topicName);

        table = resolveTableNameBySource(config, record, table);
        return table;
    }

    private String resolveTableNameBySource(JdbcSinkConnectorConfig config, SinkRecord record, String tableFormat) {
        String table = tableFormat;
        if (table.contains("${source.")) {
            try {
                Struct source = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE);
                Matcher matcher = sourcePattern.matcher(table);
                while (matcher.find()) {
                    String target = matcher.group();
                    table = table.replace(target, source.getString(matcher.group(2)));
                }
            } catch (DataException e) {
                LOGGER.error("Failed to resolve table name with format '{}', check source field in topic '{}'", config.getTableNameFormat(), record.topic());
                throw e;
            }
        }
        return table;
    }
}
