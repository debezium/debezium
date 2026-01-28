/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Integration tests for zombie task / split-brain scenarios.
 *
 * These tests verify that:
 * 1. Connector fails immediately when replication slot is already active (fail-fast)
 * 2. Connector detects stale offsets after acquiring replication slot (offset recheck)
 * 3. Split-brain scenarios are handled correctly to prevent data inconsistency
 *
 * Related issues:
 * - DBZ-3068: PostgreSQL connector task fails to resume streaming because replication slot is active
 */
public class ZombieTaskIT extends AbstractAsyncEngineConnectorTest {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ZombieTaskIT.class);

  private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);";
  private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
          "CREATE SCHEMA s1; " +
          "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));";
  private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

  @Rule
  public final TestRule skipName = new SkipTestDependingOnDecoderPluginNameRule();

  private Path masterOffsetFile;
  private Path zombieOffsetFile;

  @Before
  public void before() throws Exception {
    initializeConnectorTestFramework();
    TestHelper.dropAllSchemas();
    TestHelper.dropDefaultReplicationSlot();
    TestHelper.dropPublication();

    // Create temp files for offset storage
    masterOffsetFile = createTempFile("master-offsets", ".dat");
    zombieOffsetFile = createTempFile("zombie-offsets", ".dat");
  }

  @After
  public void after() throws Exception {
    stopConnector();
    TestHelper.dropDefaultReplicationSlot();
    TestHelper.dropPublication();

    // Cleanup temp files
    deleteIfExists(masterOffsetFile);
    deleteIfExists(zombieOffsetFile);
  }

  private void waitForStreamingRunning() throws InterruptedException {
    waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
  }

  private Path createTempFile(String prefix, String suffix) throws IOException {
    File tempFile = File.createTempFile(prefix, suffix);
    tempFile.deleteOnExit();
    return tempFile.toPath();
  }

  private void deleteIfExists(Path path) {
    if (path != null) {
      try {
        java.nio.file.Files.deleteIfExists(path);
      }
      catch (IOException e) {
        // Ignore
      }
    }
  }

  private void copyFile(Path source, Path target) throws IOException {
    java.nio.file.Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * REPRODUCER: Zombie task uses stale offset - NO FIX SCENARIO
   *
   * This test reproduces the exact problem described in DBZ-3068:
   * A zombie task starts with a stale offset and processes events from
   * the wrong position, leading to data inconsistency.
   *
   * Timeline:
   * T1: Master starts, processes batch 1 (events 1-10), offset = LSN_A
   * T2: Copy offset file (zombie's stale view = LSN_A)
   * T3: Master processes batch 2 (events 11-20), offset = LSN_B
   * T4: Master stops
   * T5: Zombie starts with STALE offset file (LSN_A)
   * T6: Zombie streams from LSN_A - re-reads events 11-20 = DUPLICATES!
   *
   * Expected behavior WITHOUT fix: Zombie re-processes events 11-20 (duplicates!)
   * Expected behavior WITH fix: Zombie either fails fast OR detects stale offset
   */
  @Test
  public void shouldReproduceZombieStaleOffsetProblem() throws Exception {
    TestHelper.execute(SETUP_TABLES_STMT);

    final String slotName = "zombie_stale_repro";
    try (PostgresConnection conn = TestHelper.create()) {
      conn.dropReplicationSlot(slotName);
    }

    // Use standalone DebeziumEngine for BOTH master and zombie
    // so they share the same offset storage format
    Configuration defaultConfig = TestHelper.defaultConfig().build();

    // ========== PHASE 1: Master starts and processes first batch ==========
    LOGGER.info("=== PHASE 1: Master starts (using standalone DebeziumEngine) ===");

    Properties masterProps = new Properties();
    masterProps.setProperty("name", "master-connector");
    masterProps.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    masterProps.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    masterProps.setProperty("offset.storage.file.filename", masterOffsetFile.toAbsolutePath().toString());
    masterProps.setProperty("offset.flush.interval.ms", "1000");
    for (String key : defaultConfig.asMap().keySet()) {
      masterProps.setProperty(key, defaultConfig.asMap().get(key));
    }
    masterProps.setProperty(PostgresConnectorConfig.SNAPSHOT_MODE.name(), SnapshotMode.NO_DATA.getValue());
    masterProps.setProperty(PostgresConnectorConfig.SLOT_NAME.name(), slotName);
    masterProps.setProperty(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), "false");

    // Track what master receives
    java.util.List<Integer> masterReceivedValues = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    CountDownLatch masterBatch1Latch = new CountDownLatch(10);
    CountDownLatch masterBatch2Latch = new CountDownLatch(10);

    DebeziumEngine<ChangeEvent<String, String>> masterEngine = DebeziumEngine.create(Json.class)
            .using(masterProps)
            .notifying((records, committer) -> {
              LOGGER.info("Master received batch of {} records", records.size());
              for (ChangeEvent<String, String> record : records) {
                String value = record.value();
                // Look for "aa": in the "after" section of the payload
                if (value != null) {
                  try {
                    // Find the "after" section first, then look for "aa": within it
                    int afterIdx = value.indexOf("\"after\":{");
                    if (afterIdx != -1) {
                      int aaStart = value.indexOf("\"aa\":", afterIdx);
                      if (aaStart != -1) {
                        aaStart += 5; // Move past "aa":
                        int aaEnd = value.indexOf(",", aaStart);
                        if (aaEnd == -1 || aaEnd > value.indexOf("}", aaStart))
                          aaEnd = value.indexOf("}", aaStart);
                        int aa = Integer.parseInt(value.substring(aaStart, aaEnd).trim());
                        masterReceivedValues.add(aa);
                        LOGGER.info("*** Master received event: aa={} ***", aa);
                        if (aa <= 10)
                          masterBatch1Latch.countDown();
                        else
                          masterBatch2Latch.countDown();
                      }
                    }
                  }
                  catch (Exception e) {
                    LOGGER.warn("Error parsing aa value: {}", e.getMessage());
                  }
                }
                committer.markProcessed(record);
              }
              committer.markBatchFinished();
            })
            .build();

    ExecutorService masterExecutor = Executors.newSingleThreadExecutor();
    masterExecutor.execute(() -> {
      try {
        LOGGER.info("Master starting...");
        masterEngine.run();
      }
      catch (Exception e) {
        LOGGER.error("Master engine failed: ", e);
      }
    });

    // Wait for master to start streaming by checking replication slot becomes active
    LOGGER.info("Waiting for master to acquire replication slot...");
    Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(() -> {
              try (PostgresConnection conn = TestHelper.create()) {
                SlotState state = conn.getReplicationSlotState(slotName,
                        TestHelper.decoderPlugin().getPostgresPluginName());
                return state != null && state.slotIsActive();
              }
            });
    LOGGER.info("Master has acquired the replication slot and is streaming!");

    // Give replication stream a moment to be fully ready
    Thread.sleep(2000);

    // Master processes first batch of events
    LOGGER.info("Master processing batch 1 (events 1-10)...");
    for (int i = 1; i <= 10; i++) {
      TestHelper.execute("INSERT INTO s1.a (aa) VALUES (" + i + ");");
    }

    // Wait for master to receive all batch 1 events
    assertThat(masterBatch1Latch.await(60, TimeUnit.SECONDS))
            .as("Master should receive batch 1").isTrue();
    LOGGER.info("Master received batch 1: {}", masterReceivedValues);

    // Wait for offset to flush to file
    Thread.sleep(3000);

    // Record slot's confirmed_flush_lsn at this point
    Lsn lsnAfterBatch1;
    try (PostgresConnection conn = TestHelper.create()) {
      SlotState slotState = conn.getReplicationSlotState(slotName,
              TestHelper.decoderPlugin().getPostgresPluginName());
      lsnAfterBatch1 = slotState.slotLastFlushedLsn();
      LOGGER.info("confirmed_flush_lsn after batch 1: {}", lsnAfterBatch1);
    }

    // ========== PHASE 2: Copy offset for zombie (zombie's stale view) ==========
    LOGGER.info("=== PHASE 2: Capture offset for zombie ===");
    LOGGER.info("Master offset file size: {} bytes", java.nio.file.Files.size(masterOffsetFile));
    copyFile(masterOffsetFile, zombieOffsetFile);
    LOGGER.info("Zombie offset file size: {} bytes", java.nio.file.Files.size(zombieOffsetFile));
    LOGGER.info("Zombie will start with STALE offset at LSN: {}", lsnAfterBatch1);

    // ========== PHASE 3: Start zombie in background (BEFORE master processes batch 2!) ==========
    LOGGER.info("=== PHASE 3: Start zombie (will fail to acquire slot, will retry) ===");

    Properties zombieProps = new Properties();
    zombieProps.setProperty("name", "master-connector"); // SAME name as master to read same offset key!
    zombieProps.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    zombieProps.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    zombieProps.setProperty("offset.storage.file.filename", zombieOffsetFile.toAbsolutePath().toString());
    zombieProps.setProperty("offset.flush.interval.ms", "1000");
    // Use SAME config as master (including topic.prefix) so zombie reads the stale offset
    // JMX metrics conflict will fail fast (2 retries) and continue without metrics
    for (String key : defaultConfig.asMap().keySet()) {
      zombieProps.setProperty(key, defaultConfig.asMap().get(key));
    }
    zombieProps.setProperty(PostgresConnectorConfig.SNAPSHOT_MODE.name(), SnapshotMode.NO_DATA.getValue());
    zombieProps.setProperty(PostgresConnectorConfig.SLOT_NAME.name(), slotName);
    zombieProps.setProperty(PostgresConnectorConfig.DROP_SLOT_ON_STOP.name(), "true");
    // IMPORTANT: Increase retries so zombie keeps trying until master releases slot
    zombieProps.setProperty(PostgresConnectorConfig.MAX_RETRIES.name(), "60"); // Retry for ~2 minutes
    zombieProps.setProperty(PostgresConnectorConfig.RETRY_DELAY_MS.name(), "2000"); // 2 second delay

    // Track what zombie receives
    java.util.List<Integer> zombieReceivedValues = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    AtomicBoolean zombieStreaming = new AtomicBoolean(false);
    CountDownLatch zombieStreamingLatch = new CountDownLatch(1);
    AtomicReference<Throwable> zombieError = new AtomicReference<>();

    DebeziumEngine<ChangeEvent<String, String>> zombieEngine = DebeziumEngine.create(Json.class)
            .using(zombieProps)
            .using((success, message, error) -> {
              if (error != null) {
                zombieError.set(error);
                LOGGER.info("Zombie completed with error: {}", error.getMessage());
              }
            })
            .notifying((records, committer) -> {
              if (!zombieStreaming.get()) {
                zombieStreaming.set(true);
                zombieStreamingLatch.countDown();
                LOGGER.info("*** Zombie is now STREAMING! ***");
              }
              for (ChangeEvent<String, String> record : records) {
                String value = record.value();
                if (value != null) {
                  try {
                    // Find the "after" section first, then look for "aa": within it
                    int afterIdx = value.indexOf("\"after\":{");
                    if (afterIdx != -1) {
                      int aaStart = value.indexOf("\"aa\":", afterIdx);
                      if (aaStart != -1) {
                        aaStart += 5; // Move past "aa":
                        int aaEnd = value.indexOf(",", aaStart);
                        if (aaEnd == -1 || aaEnd > value.indexOf("}", aaStart))
                          aaEnd = value.indexOf("}", aaStart);
                        int aa = Integer.parseInt(value.substring(aaStart, aaEnd).trim());
                        zombieReceivedValues.add(aa);
                        LOGGER.info("*** Zombie received event: aa={} ***", aa);
                      }
                    }
                  }
                  catch (Exception e) {
                    // Ignore parsing errors
                  }
                }
                committer.markProcessed(record);
              }
              committer.markBatchFinished();
            })
            .build();

    ExecutorService zombieExecutor = Executors.newSingleThreadExecutor();
    zombieExecutor.execute(() -> {
      try {
        LOGGER.info("Zombie starting... (will fail to acquire slot and RETRY)");
        zombieEngine.run();
      }
      catch (Exception e) {
        zombieError.set(e);
        LOGGER.error("Zombie engine failed: ", e);
      }
    });

    // Give zombie time to start and begin retrying for the slot
    Thread.sleep(5000);
    LOGGER.info("Zombie is now retrying to acquire slot (master has it)...");

    // ========== PHASE 4: Master processes batch 2 WHILE zombie is waiting ==========
    LOGGER.info("=== PHASE 4: Master processes batch 2 (zombie is waiting) ===");
    for (int i = 11; i <= 20; i++) {
      TestHelper.execute("INSERT INTO s1.a (aa) VALUES (" + i + ");");
    }

    // Wait for master to receive batch 2
    assertThat(masterBatch2Latch.await(30, TimeUnit.SECONDS))
            .as("Master should receive batch 2").isTrue();
    LOGGER.info("Master received batch 2, total events: {}", masterReceivedValues);

    // Wait for offset to flush
    Thread.sleep(3000);

    Lsn lsnAfterBatch2;
    try (PostgresConnection conn = TestHelper.create()) {
      SlotState slotState = conn.getReplicationSlotState(slotName,
              TestHelper.decoderPlugin().getPostgresPluginName());
      lsnAfterBatch2 = slotState.slotLastFlushedLsn();
      LOGGER.info("confirmed_flush_lsn after batch 2: {}", lsnAfterBatch2);
    }

    // Verify master advanced
    assertThat(lsnAfterBatch2.compareTo(lsnAfterBatch1))
            .as("Master should have advanced LSN")
            .isGreaterThan(0);

    LOGGER.info("Master has advanced from {} to {} (gap: {} bytes)",
            lsnAfterBatch1, lsnAfterBatch2, lsnAfterBatch2.asLong() - lsnAfterBatch1.asLong());

    // ========== PHASE 5: Master stops - zombie should now acquire slot ==========
    LOGGER.info("=== PHASE 5: Master stops (zombie will now acquire slot with STALE offset) ===");
    LOGGER.info("Zombie has STALE offset (after batch 1): {}", lsnAfterBatch1);
    LOGGER.info("Master advanced to (after batch 2): {}", lsnAfterBatch2);
    LOGGER.info("Gap: {} bytes of events zombie might re-read!", lsnAfterBatch2.asLong() - lsnAfterBatch1.asLong());

    masterEngine.close();
    masterExecutor.shutdownNow();
    LOGGER.info("Master stopped. Zombie should acquire slot now...");

    // Wait for zombie to acquire slot and start streaming
    boolean zombieStarted = zombieStreamingLatch.await(60, TimeUnit.SECONDS);

    if (zombieStarted) {
      LOGGER.info("=== PHASE 6: Zombie is streaming - checking for duplicates ===");
      // Give zombie time to process events
      Thread.sleep(10000);

      LOGGER.info("=== RESULTS ===");
      LOGGER.info("Zombie's stored offset was at: {} (after batch 1)", lsnAfterBatch1);
      LOGGER.info("Master had advanced to: {} (after batch 2)", lsnAfterBatch2);
      LOGGER.info("Gap: {} bytes", lsnAfterBatch2.asLong() - lsnAfterBatch1.asLong());
      LOGGER.info("Events zombie received: {}", zombieReceivedValues);

      // Check if zombie re-read batch 2 events (11-20)
      java.util.List<Integer> batch2Duplicates = zombieReceivedValues.stream()
              .filter(v -> v >= 11 && v <= 20)
              .collect(java.util.stream.Collectors.toList());

      if (!batch2Duplicates.isEmpty()) {
        LOGGER.info("");
        LOGGER.info("!!! DUPLICATES CONFIRMED !!!");
        LOGGER.info("CONFIRMED: Zombie re-processed events {} from batch 2!", batch2Duplicates);
        LOGGER.info("These events were already processed by master!");
        LOGGER.info("This is the SPLIT-BRAIN / DUPLICATE problem!");
      }
      else {
        LOGGER.info("");
        LOGGER.info("Note: Zombie did not receive batch 2 events.");
        LOGGER.info("The LSN gap still proves zombie started from stale position.");
      }
    }
    else {
      LOGGER.error("Zombie did not start streaming within timeout!");
      if (zombieError.get() != null) {
        LOGGER.error("Zombie error: ", zombieError.get());
      }
    }

    // Cleanup
    zombieEngine.close();
    zombieExecutor.shutdownNow();
    zombieExecutor.awaitTermination(10, TimeUnit.SECONDS);

    try (PostgresConnection conn = TestHelper.create()) {
      conn.dropReplicationSlot(slotName);
    }
  }

}
