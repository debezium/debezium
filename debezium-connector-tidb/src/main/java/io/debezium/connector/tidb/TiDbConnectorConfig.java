/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.util.Collect;

/**
 * Configuration of the TiDB connector.
 * <p>
 * The connector consumes the Debezium-format output of a TiCDC changefeed
 * ({@code protocol=debezium}) from Kafka and layers the Debezium connector framework on top of
 * it: managed offsets, the standard event envelope with a TiDB-specific {@code source} block,
 * table filtering, and (in subsequent iterations) initial and incremental snapshots through
 * TiDB's MySQL-compatible SQL endpoint.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorConfig extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    protected static final String TICDC_CONSUMER_PREFIX = "ticdc.consumer.";

    private static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet(
            "information_schema", "mysql", "performance_schema", "sys", "metrics_schema", "inspection_schema", "lightning_task_info");

    /**
     * The set of predefined snapshot mode options. Snapshots of data are not implemented yet for
     * the first iteration of the connector; the TiCDC changefeed itself can backfill historical
     * data when created with a {@code start-ts} in the past.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Capture the structure of relevant tables only, no data, then stream changes from TiCDC.
         */
        NO_DATA("no_data"),

        /**
         * Never perform a snapshot; stream changes from the TiCDC topics only.
         */
        NEVER("never");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    /**
     * Where to start reading the TiCDC topics when no offsets have been stored yet.
     */
    public enum InitialOffset implements EnumeratedValue {

        EARLIEST("earliest"),

        LATEST("latest");

        private final String value;

        InitialOffset(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static InitialOffset parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim().toLowerCase(Locale.ROOT);
            for (InitialOffset option : InitialOffset.values()) {
                if (option.getValue().equals(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public static final Field TICDC_BOOTSTRAP_SERVERS = Field.create("ticdc.kafka.bootstrap.servers")
            .withDisplayName("TiCDC Kafka bootstrap servers")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("A list of host/port pairs used to establish the connection to the Kafka cluster "
                    + "that the TiCDC changefeed (with protocol=debezium) writes to.");

    public static final Field TICDC_TOPICS = Field.create("ticdc.topics")
            .withDisplayName("TiCDC topics")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("A comma-separated list of Kafka topics written by the TiCDC changefeed "
                    + "that this connector consumes change events from.");

    public static final Field TICDC_POLL_TIMEOUT_MS = Field.create("ticdc.poll.timeout.ms")
            .withDisplayName("TiCDC poll timeout (ms)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(500)
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum time, in milliseconds, to block while polling the TiCDC topics for new change events.");

    public static final Field TICDC_INITIAL_OFFSET = Field.create("ticdc.initial.offset")
            .withDisplayName("TiCDC initial offset")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
            .withEnum(InitialOffset.class, InitialOffset.EARLIEST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The position in the TiCDC topics to start reading from when no offsets have been stored yet: "
                    + "'earliest' to read the topics from the beginning, 'latest' to read only changes arriving after the connector start.");

    public static final Field SNAPSHOT_MODE = Field.create(SNAPSHOT_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.NO_DATA)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. Select one of the following snapshot options: "
                    + "'no_data': The connector captures the structure of all relevant tables, but no data, and streams changes from the TiCDC topics; "
                    + "'never': The connector performs no snapshot and immediately streams changes from the TiCDC topics.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(TiDbSourceInfoStructMaker.class.getName());

    public static final Field TOPIC_NAMING_STRATEGY = CommonConnectorConfig.TOPIC_NAMING_STRATEGY
            .withDefault(DefaultTopicNamingStrategy.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("TiDB")
            // The first iteration has no direct connection to TiDB's SQL endpoint; the JDBC
            // connection options return together with managed snapshot support
            .excluding(HOSTNAME, PORT, USER, PASSWORD, DATABASE_NAME)
            .group(Field.Group.CONNECTION, TICDC_BOOTSTRAP_SERVERS, TICDC_TOPICS)
            .group(Field.Group.CONNECTION_ADVANCED, TICDC_POLL_TIMEOUT_MS, TICDC_INITIAL_OFFSET)
            .group(Field.Group.CONNECTOR_SNAPSHOT, SNAPSHOT_MODE)
            .group(Field.Group.CONNECTOR_ADVANCED, SOURCE_INFO_STRUCT_MAKER, TOPIC_NAMING_STRATEGY)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final SnapshotMode snapshotMode;
    private final InitialOffset initialOffset;
    private final String ticdcBootstrapServers;
    private final List<String> ticdcTopics;
    private final Duration ticdcPollTimeout;
    private final Properties ticdcConsumerProperties;

    public TiDbConnectorConfig(Configuration config) {
        super(
                config,
                TableFilter.fromPredicate(TiDbConnectorConfig::isNotBuiltInTable),
                TableId::toString,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.CATALOG,
                true);

        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.initialOffset = InitialOffset.parse(config.getString(TICDC_INITIAL_OFFSET));
        this.ticdcBootstrapServers = config.getString(TICDC_BOOTSTRAP_SERVERS);
        this.ticdcTopics = parseTopics(config.getString(TICDC_TOPICS));
        this.ticdcPollTimeout = Duration.ofMillis(config.getInteger(TICDC_POLL_TIMEOUT_MS));
        this.ticdcConsumerProperties = config.subset(TICDC_CONSUMER_PREFIX, true).asProperties();
        if (ticdcBootstrapServers != null) {
            this.ticdcConsumerProperties.put("bootstrap.servers", ticdcBootstrapServers);
        }
    }

    private static List<String> parseTopics(String topics) {
        if (topics == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(topics.split(","))
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .toList();
    }

    public static boolean isBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase(Locale.ROOT));
    }

    public static boolean isNotBuiltInTable(TableId tableId) {
        return !isBuiltInDatabase(tableId.catalog());
    }

    public String getTicdcBootstrapServers() {
        return ticdcBootstrapServers;
    }

    public List<String> getTicdcTopics() {
        return ticdcTopics;
    }

    public Duration getTicdcPollTimeout() {
        return ticdcPollTimeout;
    }

    public InitialOffset getTicdcInitialOffset() {
        return initialOffset;
    }

    /**
     * @return the properties for the Kafka consumer reading the TiCDC topics; any property
     *         prefixed with {@code ticdc.consumer.} is passed through verbatim
     */
    public Properties getTicdcConsumerProperties() {
        final Properties props = new Properties();
        props.putAll(ticdcConsumerProperties);
        return props;
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }
}
