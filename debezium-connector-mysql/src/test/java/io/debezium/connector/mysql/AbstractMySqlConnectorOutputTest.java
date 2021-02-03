/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.GtidSet.UUIDSet;
import io.debezium.connector.mysql.legacy.MySqlJdbcContext;
import io.debezium.data.VerifyRecord.RecordValueComparator;
import io.debezium.embedded.ConnectorOutputTest;
import io.debezium.util.Stopwatch;
import io.debezium.util.Testing;

/**
 * Run the {@link MySqlConnector} in various configurations and against different MySQL server instances
 * and verify the output is as expected.
 *
 * @author Randall Hauch
 */
public class AbstractMySqlConnectorOutputTest extends ConnectorOutputTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static GtidSet readAvailableGtidSet(Configuration config) {
        try (MySqlJdbcContext context = new MySqlJdbcContext(new MySqlConnectorConfig(config))) {
            String availableServerGtidStr = context.knownGtidSet();
            if (availableServerGtidStr != null && !availableServerGtidStr.trim().isEmpty()) {
                return new GtidSet(availableServerGtidStr);
            }
        }
        return null;
    }

    /**
     * Wait up to 10 seconds until the replica catches up with the master.
     *
     * @param master the configuration with the {@link MySqlConnectorConfig#HOSTNAME} and {@link MySqlConnectorConfig#PORT}
     *            configuration properties for the MySQL master; may not be null
     * @param replica the configuration with the {@link MySqlConnectorConfig#HOSTNAME} and {@link MySqlConnectorConfig#PORT}
     *            configuration properties for the MySQL replica; may not be null
     * @see #waitForGtidSetsToMatch(Configuration, Configuration, long, TimeUnit)
     */
    protected static void waitForGtidSetsToMatch(Configuration master, Configuration replica) {
        waitForGtidSetsToMatch(master, replica, 10, TimeUnit.SECONDS);
    }

    /**
     * Wait a maximum amount of time until the replica catches up with the master.
     *
     * @param master the configuration with the {@link MySqlConnectorConfig#HOSTNAME} and {@link MySqlConnectorConfig#PORT}
     *            configuration properties for the MySQL master; may not be null
     * @param replica the configuration with the {@link MySqlConnectorConfig#HOSTNAME} and {@link MySqlConnectorConfig#PORT}
     *            configuration properties for the MySQL replica; may not be null
     * @param timeout the maximum amount of time to wait
     * @param unit the time unit for the timeout
     * @see #waitForGtidSetsToMatch(Configuration, Configuration)
     */
    protected static void waitForGtidSetsToMatch(Configuration master, Configuration replica, long timeout, TimeUnit unit) {
        GtidSet masterGtidSet = readAvailableGtidSet(master);
        if (masterGtidSet == null) {
            // no GTIDs ...
            return;
        }
        Stopwatch sw = Stopwatch.reusable().start();
        CountDownLatch latch = new CountDownLatch(1);
        Runnable runner = () -> {
            try {
                GtidSet replicaGtidSet = null;
                while (true) {
                    Testing.debug("Checking replica's GTIDs and comparing to primary's...");
                    replicaGtidSet = readAvailableGtidSet(replica);
                    // The replica will have extra sources, so check whether the replica has everything in the master ...
                    if (masterGtidSet.isContainedWithin(replicaGtidSet)) {
                        Testing.debug("Replica's GTIDs are caught up to the primary's.");
                        sw.stop();
                        return;
                    }
                    Testing.debug("Waiting for replica's GTIDs to catch up to primary's...");
                    Thread.sleep(100);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            finally {
                latch.countDown();
            }
        };
        // Start the thread to keep checking for the replica's GTID set ...
        Thread checker = new Thread(runner, "mysql-replica-watcher");
        checker.start();
        try {
            if (!latch.await(timeout, unit)) {
                // Timed out waiting for them to match ...
                checker.interrupt();
            }
            Testing.print("Waited a total of " + sw.durations().statistics().getTotalAsString() + " for the replica to catch up to the primary.");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Read the system variables and master status using the given connector configuration. This can be used when creating
     * {@link io.debezium.embedded.ConnectorOutputTest.TestSpecification} instances
     * {@link io.debezium.embedded.ConnectorOutputTest.TestSpecification#withVariables(VariableSupplier) with variables}.
     * <p>
     * When connected to a replica, the GTID source for the master is added to the variables using the "@{code master_uuid}"
     * variable, which does not correspond to a real MySQL system variable. The GTID source of the server to which the test
     * case connects is given by the "{@code server_uuid}" system variable.
     *
     * @param config the connector configuration; never null
     * @return the available system variables
     * @throws Exception if there is a problem connecting to the database and reading the system variables
     */
    protected Map<String, String> readSystemVariables(Configuration config) throws Exception {
        Map<String, String> variables = new HashMap<>();
        try (MySqlJdbcContext context = new MySqlJdbcContext(new MySqlConnectorConfig(config))) {
            // Read all of the system variables ...
            variables.putAll(context.readMySqlSystemVariables());
            // Now get the master GTID source ...
            String serverUuid = variables.get("server_uuid");
            if (serverUuid != null && !serverUuid.trim().isEmpty()) {
                // We are using GTIDs, so look for the known GTID set that has the master and replica GTID sources ...
                String availableServerGtidStr = context.knownGtidSet();
                if (availableServerGtidStr != null && !availableServerGtidStr.trim().isEmpty()) {
                    GtidSet gtidSet = new GtidSet(availableServerGtidStr);
                    Collection<String> uuids = gtidSet.getUUIDSets().stream().map(UUIDSet::getUUID).collect(Collectors.toSet());
                    uuids.remove(serverUuid);
                    if (uuids.size() == 1) {
                        // We have one GTID other than the 'server_uuid', so this must be the master ...
                        String masterUuid = uuids.iterator().next();
                        variables.put("master_uuid", masterUuid);
                    }
                    else if (uuids.isEmpty()) {
                        // do nothing ...
                    }
                    else {
                        logger.warn("More than 2 GTID sources were found, so unable to determine master UUID: {}", gtidSet);
                    }
                }
            }
        }
        return variables;
    }

    @Override
    protected String[] globallyIgnorableFieldNames() {
        return new String[]{ "VALUE/source/thread" };
    }

    @Override
    protected void addValueComparatorsByFieldPath(BiConsumer<String, RecordValueComparator> comparatorsByPath) {
        super.addValueComparatorsByFieldPath(comparatorsByPath);
        comparatorsByPath.accept("SOURCEOFFSET/gtids", this::assertSameGtidSet);
    }

    // @Override
    // protected void addValueComparatorsBySchemaName(BiConsumer<String, RecordValueComparator> comparatorsBySchemaName) {
    // super.addValueComparatorsBySchemaName(comparatorsBySchemaName);
    // comparatorsBySchemaName.accept(ZonedTimestamp.SCHEMA_NAME, this::assertSameZonedTimestamps);
    // }

    protected void assertSameGtidSet(String pathToField, Object actual, Object expected) {
        assertThat(actual).isInstanceOf(String.class);
        assertThat(expected).isInstanceOf(String.class);
        GtidSet actualGtidSet = new GtidSet((String) actual);
        GtidSet expectedGtidSet = new GtidSet((String) expected);
        assertThat(actualGtidSet.toString()).isEqualTo(expectedGtidSet.toString());
    }
}
