/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class MySqlTaskContextTest {

    protected static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-context.txt").toAbsolutePath();

    protected String hostname;
    protected int port;
    protected String username;
    protected String password;
    protected int serverId;
    protected String serverName;
    protected String databaseName;

    protected Configuration config;
    protected MySqlTaskContext context;

    @Before
    public void beforeEach() {
        hostname = System.getProperty("database.hostname");
        if (hostname == null) hostname = "localhost";
        String portStr = System.getProperty("database.port");
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        } else {
            port = (Integer) MySqlConnectorConfig.PORT.defaultValue();
        }
        username = "snapper";
        password = "snapperpass";
        serverId = 18965;
        serverName = "logical_server_name";
        databaseName = "connector_test_ro";
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        if (context != null) {
            try {
                context.shutdown();
            } finally {
                context = null;
                Testing.Files.delete(DB_HISTORY_PATH);
            }
        }
    }

    protected Configuration.Builder simpleConfig() {
        return Configuration.create()
                            .with(MySqlConnectorConfig.HOSTNAME, hostname)
                            .with(MySqlConnectorConfig.PORT, port)
                            .with(MySqlConnectorConfig.USER, username)
                            .with(MySqlConnectorConfig.PASSWORD, password)
                            .with(MySqlConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED.name().toLowerCase())
                            .with(MySqlConnectorConfig.SERVER_ID, serverId)
                            .with(MySqlConnectorConfig.SERVER_NAME, serverName)
                            .with(MySqlConnectorConfig.DATABASE_WHITELIST, databaseName)
                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    @Test
    public void shouldCreateTaskFromConfigurationWithNeverSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.NEVER.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(false);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(true);
    }

    @Test
    public void shouldCreateTaskFromConfigurationWithWhenNeededSnapshotMode() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MODE, SnapshotMode.WHEN_NEEDED.getValue())
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        assertThat("" + context.snapshotMode().getValue()).isEqualTo(SnapshotMode.WHEN_NEEDED.getValue());
        assertThat(context.isSnapshotAllowedWhenNeeded()).isEqualTo(true);
        assertThat(context.isSnapshotNeverAllowed()).isEqualTo(false);
    }

    @Test
    public void shouldUseGtidSetIncludes() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES, "a,b,c,d.*")
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("a")).isTrue();
        assertThat(filter.test("b")).isTrue();
        assertThat(filter.test("c")).isTrue();
        assertThat(filter.test("d")).isTrue();
        assertThat(filter.test("d1")).isTrue();
        assertThat(filter.test("d2")).isTrue();
        assertThat(filter.test("d1234xdgfe")).isTrue();
        assertThat(filter.test("a1")).isFalse();
        assertThat(filter.test("a2")).isFalse();
        assertThat(filter.test("b1")).isFalse();
        assertThat(filter.test("c1")).isFalse();
        assertThat(filter.test("e")).isFalse();
    }

    @Test
    public void shouldUseGtidSetIncludesLiteralUuids() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c,7145bf69-d1ca-11e5-a588-0242ac110004")
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004")).isTrue();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c-extra")).isFalse();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004-extra")).isFalse();
        assertThat(filter.test("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isFalse();

        GtidSet original = new GtidSet(gtidStr);
        assertThat(original.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
        assertThat(original.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNotNull();
        assertThat(original.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();

        GtidSet filtered = original.retainAll(filter);
        assertThat(filtered.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
        assertThat(filtered.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNull();
        assertThat(filtered.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
    }

    @Test
    public void shouldUseGtidSetxcludesLiteralUuids() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
                + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
                + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES,
                                     "7c1de3f2-3fd2-11e6-9cdc-42010af000bc")
                               .build();
        context = new MySqlTaskContext(config);
        context.start();

        Predicate<String> filter = context.gtidSourceFilter();
        assertThat(filter).isNotNull();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004")).isTrue();
        assertThat(filter.test("036d85a9-64e5-11e6-9b48-42010af0000c-extra")).isTrue();
        assertThat(filter.test("7145bf69-d1ca-11e5-a588-0242ac110004-extra")).isTrue();
        assertThat(filter.test("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isFalse();

        GtidSet original = new GtidSet(gtidStr);
        assertThat(original.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
        assertThat(original.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNotNull();
        assertThat(original.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();

        GtidSet filtered = original.retainAll(filter);
        assertThat(filtered.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c")).isNotNull();
        assertThat(filtered.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc")).isNull();
        assertThat(filtered.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004")).isNotNull();
    }

    @Test
    public void shouldNotAllowBothGtidSetIncludesAndExcludes() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c,7145bf69-d1ca-11e5-a588-0242ac110004")
                               .with(MySqlConnectorConfig.GTID_SOURCE_EXCLUDES,
                                     "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:1-41")
                               .build();
        context = new MySqlTaskContext(config);
        boolean valid = config.validateAndRecord(MySqlConnectorConfig.ALL_FIELDS,msg->{});
        assertThat(valid).isFalse();
    }

    @Test
    public void shouldFilterAndMergeGtidSet() throws Exception {
        String gtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-2,"
          + "7c1de3f2-3fd2-11e6-9cdc-42010af000bc:5-41";
        String availableServerGtidStr = "036d85a9-64e5-11e6-9b48-42010af0000c:1-20,"
          + "7145bf69-d1ca-11e5-a588-0242ac110004:1-3200,"
          + "123e4567-e89b-12d3-a456-426655440000:1-41";
        config = simpleConfig().with(MySqlConnectorConfig.GTID_SOURCE_INCLUDES,
                                     "036d85a9-64e5-11e6-9b48-42010af0000c")
                               .build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setCompletedGtidSet(gtidStr);

        GtidSet mergedGtidSet = context.filterGtidSet(new GtidSet(availableServerGtidStr));
        assertThat(mergedGtidSet).isNotNull();
        GtidSet.UUIDSet uuidSet1 = mergedGtidSet.forServerWithId("036d85a9-64e5-11e6-9b48-42010af0000c");
        GtidSet.UUIDSet uuidSet2 = mergedGtidSet.forServerWithId("7145bf69-d1ca-11e5-a588-0242ac110004");
        GtidSet.UUIDSet uuidSet3 = mergedGtidSet.forServerWithId("123e4567-e89b-12d3-a456-426655440000");
        GtidSet.UUIDSet uuidSet4 = mergedGtidSet.forServerWithId("7c1de3f2-3fd2-11e6-9cdc-42010af000bc");

        assertThat(uuidSet1.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 2)));
        assertThat(uuidSet2.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 3200)));
        assertThat(uuidSet3.getIntervals()).isEqualTo(Arrays.asList(new GtidSet.Interval(1, 41)));
        assertThat(uuidSet4).isNull();
    }
}
