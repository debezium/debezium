/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig.Version;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * Configuration for the ${connectorName} connector.
 *
 * <p>Extends {@link RelationalDatabaseConnectorConfig}, which contributes the standard
 * connection fields ({@code database.hostname}, {@code database.port}, {@code database.user},
 * {@code database.password}, {@code database.dbname}) and the table/column include-exclude
 * filters. Add connector-specific {@link Field} constants here and register them in
 * {@link #CONFIG_DEFINITION}.
 */
public class ${connectorName}ConnectorConfig extends RelationalDatabaseConnectorConfig {

    /** Default number of rows to fetch per round trip during a snapshot. */
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    public enum SnapshotMode implements EnumeratedValue {

        INITIAL("initial"),
        NO_DATA("no_data");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

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
                    + "Options include: 'initial' (default) to snapshot schema and data when no offset exists; "
                    + "'no_data' to capture only the schema. The value maps to a registered Snapshotter.");

    // Add connector-specific fields below, then register them in CONFIG_DEFINITION.
    // public static final Field MY_FIELD = Field.create("my.setting") ...

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("${connectorName}")
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME)
            .connector(SNAPSHOT_MODE)
            .create();

    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final SnapshotMode snapshotMode;

    public ${connectorName}ConnectorConfig(Configuration config) {
        super(
                config,
                new SystemTablesPredicate(),
                TableId::identifier,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                // Catalog-based databases (e.g. MySQL) use ColumnFilterMode.CATALOG instead.
                ColumnFilterMode.SCHEMA,
                false);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
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

    /**
     * Decides which tables the connector captures. Returns {@code true} for every table by
     * default; narrow this to exclude your database's system tables and schemas.
     */
    private static class SystemTablesPredicate implements TableFilter {
        @Override
        public boolean isIncluded(TableId t) {
            return true;
        }
    }
}
