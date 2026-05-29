/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.Version;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;

/**
 * Configuration for the ${connectorName} connector.
 *
 * <p>Add connector-specific {@link Field} constants here. Each field should be documented
 * and included in {@link #ALL_FIELDS} and {@link #configDef()}.
 */
public class ${connectorName}ConnectorConfig extends CommonConnectorConfig {

    public enum SnapshotMode implements EnumeratedValue {

        INITIAL("initial") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return !offsetExists || snapshotInProgress;
            }
        },

        NEVER("never") {
            @Override
            public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
                return false;
            }
        };

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public abstract boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress);

        public static SnapshotMode parse(String value) {
            return Arrays.stream(values())
                    .filter(m -> m.value.equalsIgnoreCase(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown snapshot mode: " + value));
        }
    }

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies the criteria for performing a snapshot on startup. "
                    + "Options include: 'initial' (default) to snapshot only when no offset exists; "
                    + "'never' to skip snapshot entirely.");

    // Add connector-specific fields below.
    // public static final Field MY_FIELD = Field.create("my.setting") ...

    public static final Field.Set ALL_FIELDS = Field.setOf(
            CommonConnectorConfig.TOPIC_PREFIX,
            SNAPSHOT_MODE);

    private final SnapshotMode snapshotMode;

    public ${connectorName}ConnectorConfig(Configuration config) {
        super(config, 0);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
    }

    public static ConfigDef configDef() {
        ConfigDef configDef = new ConfigDef();
        Field.group(configDef, "Connector", CommonConnectorConfig.TOPIC_PREFIX);
        Field.group(configDef, "Snapshots", SNAPSHOT_MODE);
        return configDef;
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
    public String getContextName() {
        return "${connectorName}";
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        ${connectorName}SourceInfoStructMaker maker = new ${connectorName}SourceInfoStructMaker();
        maker.init(Module.name(), Module.version(), this);
        return maker;
    }
}
