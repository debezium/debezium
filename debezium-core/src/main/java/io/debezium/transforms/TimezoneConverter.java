/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.re2j.Pattern;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

/**
 *
 * @author Anisha Mohanty
 *
 */

public class TimezoneConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimezoneConverter.class);

    private static final Field CONVERTED_TIMEZONE = Field.create("converted.timezone")
            .withDisplayName("Converted Timezone")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("A string that represents the timezone to which the time-based fields should be converted."
                    + "The format can be geographic (e.g. America/Los_Angeles), or it can be a UTC offset in the format of +/-hh:mm, (e.g. +08:00).");

    private static final Field INCLUDE_LIST = Field.create("include.list")
            .withDisplayName("Include List")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of rules that specify what events should be evaluated for timezone conversion, using one of the following formats: "
                    + "`source:<tablename>`:: Matches only Debezium change events with a source information block with the specified table name. All time-based fields will be converted. "
                    + "`source:<tablename>:<fieldname>`:: Matches only Debezium change events with a source information with the specified table name. Only the specified field name will be converted. "
                    + "`topic:<topicname>`:: Matches the specified topic name, converting all time-based fields. "
                    + "`topic:<topicname>:<fieldname>`:: Matches the specified topic name, converting only the specified field name. "
                    + "`<matchname>:<fieldname>`:: Uses a heuristic matching algorithm to matches the source information block table name if the source information block exists, otherwise matches against the topic name. The conversion is applied only to to the specified field name. ");

    private static final Field EXCLUDE_LIST = Field.create("exclude.list")
            .withDisplayName("Exclude List")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of rules that specify what events should be excluded from timezone conversion, using one of the following formats: "
                    + "`source:<tablename>`:: Matches only Debezium change events with a source information block with the specified table name. All time-based fields will be excluded. "
                    + "`source:<tablename>:<fieldnames>`:: Matches only Debezium change events with a source information with the specified table name. Only the specified field name will be excluded. "
                    + "`topic:<topicname>`:: Matches the specified topic name, excluding all time-based fields. "
                    + "`topic:<topicname>:<fieldnames>`:: Matches the specified topic name, excluding only the specified field name. "
                    + "`<matchname>:<fieldnames>`:: Uses a heuristic matching algorithm to matches the source information block table name if the source information block exists, otherwise matches against the topic name. The conversion is applied only to to the specified field name. ");

    private SmtManager<R> smtManager;
    private String convertedTimezone;
    private List<String> includeList;
    private List<String> excludeList;
    private static final String SOURCE = Envelope.FieldName.SOURCE;
    private static final String TOPIC = "topic";
    private static final String FIELD_SOURCE_PREFIX = Envelope.FieldName.SOURCE + ".";
    private static final String FIELD_BEFORE_PREFIX = Envelope.FieldName.BEFORE + ".";
    private static final String FIELD_AFTER_PREFIX = Envelope.FieldName.AFTER + ".";
    private static final Pattern TIMEZONE_OFFSET_PATTERN = Pattern.compile("^[+-]\\d{2}:\\d{2}(:\\d{2})?$");
    private static final Pattern LIST_PATTERN = Pattern.compile("^\\[(source|topic|[\".\\w\\s_]+):([\".\\w\\s_]+(?::[\".\\w\\s_]+)?(?:,|]$))+$");
    private final Map<String, Set<String>> topicFieldsMap = new HashMap<>();
    private final Map<String, Set<String>> tableFieldsMap = new HashMap<>();
    private final Map<String, Set<String>> noPrefixFieldsMap = new HashMap<>();
    private static final List<String> SUPPORTED_TIMESTAMP_LOGICAL_NAMES = List.of(
            MicroTimestamp.SCHEMA_NAME,
            NanoTimestamp.SCHEMA_NAME,
            Timestamp.SCHEMA_NAME,
            ZonedTimestamp.SCHEMA_NAME,
            ZonedTime.SCHEMA_NAME,
            org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME);
    private static final List<String> UNSUPPORTED_LOGICAL_NAMES = List.of(
            io.debezium.time.Date.SCHEMA_NAME,
            io.debezium.time.MicroTime.SCHEMA_NAME,
            io.debezium.time.NanoTime.SCHEMA_NAME,
            io.debezium.time.Time.SCHEMA_NAME,
            org.apache.kafka.connect.data.Date.LOGICAL_NAME,
            org.apache.kafka.connect.data.Time.LOGICAL_NAME);

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, CONVERTED_TIMEZONE, INCLUDE_LIST, EXCLUDE_LIST);
        return config;
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        Struct value = (Struct) record.value();
        String table = getTableFromSource(value);
        String topic = record.topic();

        if (includeList.isEmpty() && excludeList.isEmpty()) {
            handleAllRecords(value, table, topic);
        }
        else if (!includeList.isEmpty()) {
            handleInclude(value, table, topic);
        }
        else {
            handleExclude(value, table, topic);
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers());
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(CONVERTED_TIMEZONE, INCLUDE_LIST, EXCLUDE_LIST));

        convertedTimezone = config.getString(CONVERTED_TIMEZONE);
        includeList = config.getList(INCLUDE_LIST);
        excludeList = config.getList(EXCLUDE_LIST);

        validateConfiguration();

        if (!excludeList.isEmpty()) {
            collectTablesAndTopics(excludeList);
        }
        else if (!includeList.isEmpty()) {
            collectTablesAndTopics(includeList);
        }
    }

    private void collectTablesAndTopics(List<String> list) {
        String commonPrefix = null;
        for (String item : list) {
            FieldItem parseItem = parseItem(item);
            String prefix = parseItem.prefix;
            String matchName = parseItem.getMatchName();
            String field = parseItem.getFieldName();

            if (prefix != null) {
                commonPrefix = prefix;
            }

            if (Objects.equals(commonPrefix, TOPIC)) {
                if (!topicFieldsMap.containsKey(matchName)) {
                    topicFieldsMap.put(matchName, new HashSet<>());
                }
                if (field != null) {
                    topicFieldsMap.get(matchName).add(field);
                }
            }
            else if (Objects.equals(commonPrefix, SOURCE)) {
                if (!tableFieldsMap.containsKey(matchName)) {
                    tableFieldsMap.put(matchName, new HashSet<>());
                }
                if (field != null) {
                    tableFieldsMap.get(matchName).add(field);
                }
            }
            else {
                if (!noPrefixFieldsMap.containsKey(matchName)) {
                    noPrefixFieldsMap.put(matchName, new HashSet<>());
                }
                if (field != null) {
                    noPrefixFieldsMap.get(matchName).add(field);
                }
            }
        }
    }

    private void validateConfiguration() {
        if (!includeList.isEmpty()) {
            if (!LIST_PATTERN.matcher(includeList.toString()).matches()) {
                throw new DebeziumException(
                        "Invalid include list format. Please specify a list of rules in the format of \"source:<tablename>:<fieldnames>\", \"topic:<topicname>:<fieldnames>\", \"<matchname>:<fieldnames>\"");
            }
        }
        else if (!excludeList.isEmpty()) {
            if (!LIST_PATTERN.matcher(excludeList.toString()).matches()) {
                throw new DebeziumException(
                        "Invalid exclude list format. Please specify a list of rules in the format of \"source:<tablename>:<fieldnames>\", \"topic:<topicname>:<fieldnames>\", \"<matchname>:<fieldnames>\"");
            }
        }

        if (!validateTimezoneString()) {
            throw new DebeziumException(
                    "Invalid timezone format. Please specify either a geographic timezone (e.g. America/Los_Angeles) or a UTC offset in the format of +/-hh:mm, (e.g. +08:00).");
        }

        if (!includeList.isEmpty() && !excludeList.isEmpty()) {
            throw new DebeziumException("Both include and exclude lists are specified. Please specify only one.");
        }
    }

    private boolean validateTimezoneString() {
        if (TIMEZONE_OFFSET_PATTERN.matcher(convertedTimezone).matches()) {
            return true;
        }
        else if (ZoneId.getAvailableZoneIds().contains(convertedTimezone)) {
            return true;
        }
        else {
            return Arrays.asList(TimeZone.getAvailableIDs()).contains(convertedTimezone);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    private enum Type {
        ALL,
        INCLUDE,
        EXCLUDE
    }

    private Object getTimestampWithTimezone(String schemaName, Object fieldValue) {
        Object updatedFieldValue = fieldValue;
        ZoneId zoneId = ZoneId.of(convertedTimezone);
        ZoneOffset zoneOffset = zoneId.getRules().getOffset(Instant.now());
        switch (schemaName) {
            case ZonedTimestamp.SCHEMA_NAME:
                OffsetDateTime offsetDateTime = OffsetDateTime.parse((String) fieldValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                zoneOffset = zoneId.getRules().getOffset(offsetDateTime.toLocalDateTime());
                OffsetDateTime offsetDateTimeWithZone = offsetDateTime.withOffsetSameInstant(zoneOffset);
                updatedFieldValue = ZonedTimestamp.toIsoString(offsetDateTimeWithZone, null);
                break;
            case ZonedTime.SCHEMA_NAME:
                OffsetTime offsetTime = OffsetTime.parse((String) fieldValue, DateTimeFormatter.ISO_OFFSET_TIME);
                OffsetTime offsetTimeWithZone = offsetTime.withOffsetSameInstant(zoneOffset);
                updatedFieldValue = ZonedTime.toIsoString(offsetTimeWithZone, null);
                break;
            case MicroTimestamp.SCHEMA_NAME:
                long microTimestamp = (long) fieldValue;
                Instant microInstant = Instant.ofEpochSecond(microTimestamp / 1_000_000, (microTimestamp % 1_000_000) * 1_000);
                LocalDateTime microLocalDateTime = microInstant.atOffset(ZoneOffset.UTC).toLocalDateTime();
                zoneOffset = zoneId.getRules().getOffset(microLocalDateTime);
                updatedFieldValue = microLocalDateTime.toEpochSecond(zoneOffset) * 1_000_000 + microLocalDateTime.getNano() / 1_000;
                break;
            case NanoTimestamp.SCHEMA_NAME:
                long nanoTimestamp = (long) fieldValue;
                Instant nanoInstant = Instant.ofEpochSecond(nanoTimestamp / 1_000_000_000, (nanoTimestamp % 1_000_000_000));
                LocalDateTime nanoLocalDateTime = nanoInstant.atOffset(ZoneOffset.UTC).toLocalDateTime();
                zoneOffset = zoneId.getRules().getOffset(nanoLocalDateTime);
                updatedFieldValue = nanoLocalDateTime.toEpochSecond(zoneOffset) * 1_000_000_000 + nanoLocalDateTime.getNano();
                break;
            case Timestamp.SCHEMA_NAME:
                Instant instant = Instant.ofEpochMilli((long) fieldValue);
                LocalDateTime localDateTime = instant.atOffset(ZoneOffset.UTC).toLocalDateTime();
                zoneOffset = zoneId.getRules().getOffset(localDateTime);
                updatedFieldValue = localDateTime.atOffset(zoneOffset).toInstant().toEpochMilli();
                break;
            case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                Date date = (Date) fieldValue;
                Instant timestampInstant = date.toInstant();
                LocalDateTime timestampLocalDateTime = timestampInstant.atOffset(ZoneOffset.UTC).toLocalDateTime();
                zoneOffset = zoneId.getRules().getOffset(timestampLocalDateTime);
                updatedFieldValue = Date.from(timestampLocalDateTime.atOffset(zoneOffset).toInstant());
                break;
        }
        return updatedFieldValue;
    }

    private void handleStructs(Struct value, Type type, String matchName, Set<String> fields) {
        if (type == null || matchName == null) {
            return;
        }

        Struct before = getStruct(value, Envelope.FieldName.BEFORE);
        Struct after = getStruct(value, Envelope.FieldName.AFTER);
        Struct source = getStruct(value, Envelope.FieldName.SOURCE);

        Set<String> beforeFields = new HashSet<>();
        Set<String> afterFields = new HashSet<>();
        Set<String> sourceFields = new HashSet<>();

        if (!fields.isEmpty()) {
            for (String field : fields) {
                if (field.startsWith(FIELD_SOURCE_PREFIX)) {
                    sourceFields.add(field.substring(FIELD_SOURCE_PREFIX.length()));
                }
                else if (field.startsWith(FIELD_BEFORE_PREFIX)) {
                    beforeFields.add(field.substring(FIELD_BEFORE_PREFIX.length()));
                }
                else if (field.startsWith(FIELD_AFTER_PREFIX)) {
                    afterFields.add(field.substring(FIELD_AFTER_PREFIX.length()));
                }
                else {
                    beforeFields.add(field);
                    afterFields.add(field);
                }
            }
        }

        if (before != null) {
            handleValueForFields(before, type, beforeFields);
        }
        if (after != null) {
            handleValueForFields(after, type, afterFields);
        }
        if (source != null && !sourceFields.isEmpty()) {
            handleValueForFields(source, type, sourceFields);
        }
    }

    private void handleValueForFields(Struct value, Type type, Set<String> fields) {
        Schema schema = value.schema();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            String schemaName = field.schema().name();

            if (schemaName == null) {
                continue;
            }

            boolean isUnsupportedLogicalType = UNSUPPORTED_LOGICAL_NAMES.contains(schemaName);
            boolean supportedLogicalType = SUPPORTED_TIMESTAMP_LOGICAL_NAMES.contains(schemaName);
            boolean shouldIncludeField = type == Type.ALL
                    || (type == Type.INCLUDE && fields.contains(field.name()))
                    || (type == Type.EXCLUDE && !fields.contains(field.name()));

            if (isUnsupportedLogicalType && shouldIncludeField) {
                LOGGER.warn("Skipping conversion for unsupported logical type: " + schemaName + " for field: " + field.name());
                continue;
            }

            if (shouldIncludeField && supportedLogicalType) {
                if (value.get(field) != null) {
                    handleValueForField(value, field);
                }
            }
        }
    }

    private void handleValueForField(Struct struct, org.apache.kafka.connect.data.Field field) {
        String fieldName = field.name();
        Schema schema = field.schema();
        Object newValue = getTimestampWithTimezone(schema.name(), struct.get(fieldName));
        struct.put(fieldName, newValue);
    }

    private Struct getStruct(Struct struct, String structName) {
        try {
            return Requirements.requireStructOrNull(struct.getStruct(structName), "");
        }
        catch (DataException ignored) {
        }
        return null;
    }

    private String getTableFromSource(Struct value) {
        try {
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            return source.getString("table");
        }
        catch (DataException ignored) {
        }
        return null;
    }

    private static class FieldItem {
        private final String prefix;
        private final String matchName;
        private final String fieldName;

        FieldItem(String prefix, String matchName, String fieldName) {
            this.prefix = prefix;
            this.matchName = matchName;
            this.fieldName = fieldName;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getMatchName() {
            return matchName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    private FieldItem parseItem(String item) {
        String prefix = null;
        String matchName = null;
        String fieldName = null;

        String[] parts = item.split(":");
        if (parts.length == 1) {
            // table or topic
            matchName = parts[0];
        }
        else if (parts.length >= 2 && parts.length <= 3) {
            if (parts[0].equalsIgnoreCase(SOURCE) || parts[0].equalsIgnoreCase(TOPIC)) {
                // source:table or topic:topic
                // source:table1,table2 or topic:topic1,topic2
                prefix = parts[0];
                matchName = parts[1];
                if (parts.length == 3) {
                    // source:table:field or topic:topic:field
                    fieldName = parts[2];
                }
            }
            else {
                // table:field or topic:field
                matchName = parts[0];
                fieldName = parts[1];
            }
        }
        return new FieldItem(prefix, matchName, fieldName);
    }

    private static class MatchFieldsResult {
        private final String matchName;
        private final Set<String> fields;

        MatchFieldsResult(String matchName, Set<String> fields) {
            this.matchName = matchName;
            this.fields = fields;
        }

        public String getMatchName() {
            return matchName;
        }

        public Set<String> getFields() {
            return fields;
        }
    }

    private MatchFieldsResult handleMatchNameAndFields(String table, String topic) {
        String matchName = null;
        Set<String> fields = Collections.emptySet();

        if (topicFieldsMap.containsKey(topic)) {
            matchName = topic;
            fields = topicFieldsMap.get(topic);
        }
        else if (tableFieldsMap.containsKey(table)) {
            matchName = table;
            fields = tableFieldsMap.get(table);
        }
        else if (noPrefixFieldsMap.containsKey(topic)) {
            matchName = topic;
            fields = noPrefixFieldsMap.get(topic);
        }
        else if (noPrefixFieldsMap.containsKey(table)) {
            matchName = table;
            fields = noPrefixFieldsMap.get(table);
        }

        return new MatchFieldsResult(matchName, fields);
    }

    private void handleInclude(Struct value, String table, String topic) {
        MatchFieldsResult matchFieldsResult = handleMatchNameAndFields(table, topic);
        String matchName = matchFieldsResult.getMatchName();
        Set<String> fields = matchFieldsResult.getFields();

        if (matchName != null) {
            if (!fields.isEmpty()) {
                handleStructs(value, Type.INCLUDE, matchName, fields);
            }
            else {
                handleStructs(value, Type.ALL, matchName, fields);
            }
        }
    }

    private void handleExclude(Struct value, String table, String topic) {
        MatchFieldsResult matchFieldsResult = handleMatchNameAndFields(table, topic);
        String matchName = matchFieldsResult.getMatchName();
        Set<String> fields = matchFieldsResult.getFields();

        if (matchName == null) {
            handleStructs(value, Type.ALL, table != null ? table : topic, Collections.emptySet());
        }
        else if (!fields.isEmpty()) {
            handleStructs(value, Type.EXCLUDE, matchName, fields);
        }
    }

    private void handleAllRecords(Struct value, String table, String topic) {
        if (!topicFieldsMap.containsKey(topic) && !tableFieldsMap.containsKey(table) && !noPrefixFieldsMap.containsKey(table)) {
            handleStructs(value, Type.ALL, table != null ? table : topic, Collections.emptySet());
        }
    }
}
