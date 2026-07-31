/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.perf;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

public class TestConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestConfiguration.class);

    private static final String TEST_PREFIX = "test.";
    private static final String DEPLOYMENT_PREFIX = "deploy.";
    private static final String DATABASE_PREFIX = "database.";

    private static final String PROP_TEST_CONFIG_FILE = TEST_PREFIX + "config.file";

    private static final String PROP_DEPLOYMENT_JMX_HOST = DEPLOYMENT_PREFIX + "jmx.host";
    private static final String PROP_DEPLOYMENT_JMX_PORT = DEPLOYMENT_PREFIX + "jmx.port";
    private static final String PROP_DEPLOYMENT_KAFKA_CONNECT_HOST = DEPLOYMENT_PREFIX + "kafka.connect.host";
    private static final String PROP_DEPLOYMENT_KAFKA_CONNECT_PORT = DEPLOYMENT_PREFIX + "kafka.connect.port";
    private static final String PROP_DEPLOYMENT_KAFKA_HOST = DEPLOYMENT_PREFIX + "kafka.host";
    private static final String PROP_DEPLOYMENT_KAFKA_PORT = DEPLOYMENT_PREFIX + "kafka.port";

    private static final String PROP_TEST_POLL_INTERVAL = TEST_PREFIX + "poll.interval";
    private static final String PROP_TEST_WAIT_TIME = TEST_PREFIX + "wait.time";
    private static final String PROP_TEST_REPORT_FILE = TEST_PREFIX + "report.file";
    private static final String PROP_TEST_SCHEMAS_COUNT = TEST_PREFIX + "schema.count";
    private static final String PROP_TEST_TABLES_COUNT = TEST_PREFIX + "table.count";
    private static final String PROP_TEST_COLUMNS_COUNT = TEST_PREFIX + "column.count";
    private static final String PROP_TEST_COLUMN_LENGTH = TEST_PREFIX + "column.length";
    private static final String PROP_TEST_TX_COUNT = TEST_PREFIX + "tx.count";
    private static final String PROP_TEST_TX_CHANGES = TEST_PREFIX + "tx.changes";

    private final String testRunId = Integer.toString(new Random().nextInt(1000));
    private final String connectorType;

    private final String jmxHost;
    private final int jmxPort;
    private final String kafkaConnectHost;
    private final int kafkaConnectPort;
    private final String kafkaHost;
    private final int kafkaPort;

    private final Duration pollInterval;
    private final Duration waitTime;
    private final String reportFile;
    private final int schemasCount;
    private final int tablesCount;
    private final int columnsCount;
    private final int columnLength;
    private final int populateTxCount;
    private final int populateChangesPerTxCount;

    private final Configuration databaseConfiguration;

    public TestConfiguration(String connectorType) {
        Configuration config;
        this.connectorType = connectorType;

        final String configFile = System.getProperty(PROP_TEST_CONFIG_FILE);
        if (configFile != null && !configFile.isEmpty()) {
            try {
                config = Configuration.load(configFile, TestConfiguration.class);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        else {
            config = Configuration.empty();
        }
        config = config
                .withSystemProperties(prop -> prop.startsWith(DATABASE_PREFIX) || prop.startsWith(DEPLOYMENT_PREFIX) || prop.startsWith(TEST_PREFIX) ? prop : null);

        jmxHost = config.getString(PROP_DEPLOYMENT_JMX_HOST, "localhost");
        jmxPort = config.getInteger(PROP_DEPLOYMENT_JMX_PORT, 5006);
        kafkaConnectHost = config.getString(PROP_DEPLOYMENT_KAFKA_CONNECT_HOST, "localhost");
        kafkaConnectPort = config.getInteger(PROP_DEPLOYMENT_KAFKA_CONNECT_PORT, 8083);
        kafkaHost = config.getString(PROP_DEPLOYMENT_KAFKA_HOST, "localhost");
        kafkaPort = config.getInteger(PROP_DEPLOYMENT_KAFKA_PORT, 9092);

        pollInterval = Duration.ofMillis(config.getLong(PROP_TEST_POLL_INTERVAL, 100L));
        waitTime = Duration.ofSeconds(config.getLong(PROP_TEST_WAIT_TIME, 600L));

        reportFile = config.getString(PROP_TEST_REPORT_FILE, "test_report.csv");
        schemasCount = config.getInteger(PROP_TEST_SCHEMAS_COUNT, 0);
        tablesCount = config.getInteger(PROP_TEST_TABLES_COUNT, 0);
        columnsCount = config.getInteger(PROP_TEST_COLUMNS_COUNT, 1);
        columnLength = config.getInteger(PROP_TEST_COLUMN_LENGTH, 1024);
        populateTxCount = config.getInteger(PROP_TEST_TX_COUNT, 100);
        populateChangesPerTxCount = config.getInteger(PROP_TEST_TX_CHANGES, 100);

        databaseConfiguration = config.subset(DATABASE_PREFIX, true);
    }

    public Map<String, String> recordingSettings() {
        final Map<String, String> opts = new HashMap<>();
        opts.put("jdk.CompilerConfiguration#period", "beginChunk");
        opts.put("jdk.FileWrite#stackTrace", "true");
        opts.put("jdk.ObjectCount#enabled", "false");
        opts.put("jdk.ThreadStart#enabled", "true");
        opts.put("jdk.TenuringDistribution#enabled", "true");
        opts.put("jdk.CompilerStatistics#period", "1 s");
        opts.put("jdk.CPUInformation#enabled", "true");
        opts.put("jdk.SafepointCleanup#threshold", "0 ns");
        opts.put("jdk.ClassDefine#enabled", "false");
        opts.put("jdk.GCConfiguration#enabled", "true");
        opts.put("jdk.EvacuationFailed#enabled", "true");
        opts.put("jdk.ClassLoad#threshold", "0 ns");
        opts.put("jdk.ReservedStackActivation#enabled", "true");
        opts.put("jdk.PromoteObjectInNewPLAB#enabled", "true");
        opts.put("jdk.IntFlag#period", "beginChunk");
        opts.put("jdk.ThreadSleep#stackTrace", "true");
        opts.put("jdk.ClassLoad#stackTrace", "true");
        opts.put("jdk.ModuleRequire#enabled", "true");
        opts.put("jdk.JavaErrorThrow#stackTrace", "true");
        opts.put("jdk.ObjectAllocationInNewTLAB#stackTrace", "true");
        opts.put("jdk.SocketRead#stackTrace", "true");
        opts.put("jdk.IntFlag#enabled", "true");
        opts.put("jdk.GCPhasePauseLevel2#enabled", "true");
        opts.put("jdk.G1GarbageCollection#enabled", "true");
        opts.put("jdk.AllocationRequiringGC#stackTrace", "true");
        opts.put("jdk.StringFlagChanged#enabled", "true");
        opts.put("jdk.ModuleExport#enabled", "true");
        opts.put("jdk.JavaExceptionThrow#stackTrace", "true");
        opts.put("jdk.GCReferenceStatistics#enabled", "true");
        opts.put("jdk.ShenandoahHeapRegionInformation#enabled", "false");
        opts.put("jdk.G1EvacuationOldStatistics#enabled", "true");
        opts.put("jdk.BiasedLockClassRevocation#threshold", "0 ns");
        opts.put("jdk.VirtualizationInformation#enabled", "true");
        opts.put("jdk.FileRead#stackTrace", "true");
        opts.put("jdk.InitialEnvironmentVariable#enabled", "true");
        opts.put("jdk.Shutdown#enabled", "true");
        opts.put("jdk.GCPhasePause#enabled", "true");
        opts.put("jdk.GCPhasePauseLevel3#threshold", "0 ns");
        opts.put("jdk.JVMInformation#enabled", "true");
        opts.put("jdk.ThreadAllocationStatistics#period", "everyChunk");
        opts.put("jdk.CompilerConfiguration#enabled", "true");
        opts.put("jdk.FileRead#threshold", "10 ms");
        opts.put("jdk.FileRead#enabled", "true");
        opts.put("jdk.CodeSweeperConfiguration#enabled", "true");
        opts.put("jdk.ZStatisticsSampler#threshold", "10 ms");
        opts.put("jdk.UnsignedIntFlagChanged#enabled", "true");
        opts.put("jdk.OldObjectSample#cutoff", "0 ns");
        opts.put("jdk.SocketWrite#enabled", "true");
        opts.put("jdk.UnsignedIntFlag#period", "beginChunk");
        opts.put("jdk.SafepointWaitBlocked#threshold", "0 ns");
        opts.put("jdk.BooleanFlagChanged#enabled", "true");
        opts.put("jdk.ShenandoahHeapRegionStateChange#enabled", "false");
        opts.put("jdk.ExecuteVMOperation#threshold", "0 ns");
        opts.put("jdk.CPULoad#period", "1 s");
        opts.put("jdk.CompilationFailure#enabled", "true");
        opts.put("jdk.Compilation#threshold", "100 ms");
        opts.put("jdk.PromotionFailed#enabled", "true");
        opts.put("jdk.BooleanFlag#period", "beginChunk");
        opts.put("jdk.JavaMonitorEnter#stackTrace", "true");
        opts.put("jdk.ZStatisticsSampler#enabled", "true");
        opts.put("jdk.SecurityPropertyModification#stackTrace", "true");
        opts.put("jdk.ThreadPark#threshold", "10 ms");
        opts.put("jdk.StringFlag#enabled", "true");
        opts.put("jdk.MetaspaceOOM#stackTrace", "true");
        opts.put("jdk.YoungGarbageCollection#threshold", "0 ns");
        opts.put("jdk.CodeCacheFull#enabled", "true");
        opts.put("jdk.YoungGarbageCollection#enabled", "true");
        opts.put("jdk.CPULoad#enabled", "true");
        opts.put("jdk.BiasedLockRevocation#threshold", "0 ns");
        opts.put("jdk.CodeCacheConfiguration#period", "beginChunk");
        opts.put("jdk.JavaMonitorWait#enabled", "true");
        opts.put("jdk.BiasedLockSelfRevocation#stackTrace", "true");
        opts.put("jdk.ModuleRequire#period", "endChunk");
        opts.put("jdk.OldGarbageCollection#enabled", "true");
        opts.put("jdk.ExceptionStatistics#period", "1 s");
        opts.put("jdk.NativeLibrary#enabled", "true");
        opts.put("jdk.CompilerInlining#enabled", "true");
        opts.put("jdk.ActiveSetting#enabled", "true");
        opts.put("jdk.ZPageAllocation#enabled", "true");
        opts.put("jdk.SafepointWaitBlocked#enabled", "false");
        opts.put("jdk.InitialSystemProperty#enabled", "true");
        opts.put("jdk.GarbageCollection#enabled", "true");
        opts.put("jdk.NativeMethodSample#period", "20 ms");
        opts.put("jdk.ZPageAllocation#threshold", "10 ms");
        opts.put("jdk.ZThreadPhase#threshold", "0 ns");
        opts.put("jdk.ThreadContextSwitchRate#period", "10 s");
        opts.put("jdk.ThreadCPULoad#enabled", "true");
        opts.put("jdk.JavaMonitorInflate#threshold", "10 ms");
        opts.put("jdk.OldObjectSample#stackTrace", "true");
        opts.put("jdk.MetaspaceAllocationFailure#enabled", "true");
        opts.put("jdk.OSInformation#enabled", "true");
        opts.put("jdk.FileWrite#enabled", "true");
        opts.put("jdk.BiasedLockRevocation#enabled", "true");
        opts.put("jdk.GCConfiguration#period", "everyChunk");
        opts.put("jdk.PhysicalMemory#period", "everyChunk");
        opts.put("jdk.CodeCacheConfiguration#enabled", "true");
        opts.put("jdk.GCHeapSummary#enabled", "true");
        opts.put("jdk.ConcurrentModeFailure#enabled", "true");
        opts.put("jdk.JavaMonitorEnter#threshold", "10 ms");
        opts.put("jdk.ThreadSleep#enabled", "true");
        opts.put("jdk.G1HeapRegionInformation#enabled", "false");
        opts.put("jdk.PromoteObjectOutsidePLAB#enabled", "true");
        opts.put("jdk.ThreadPark#enabled", "true");
        opts.put("jdk.YoungGenerationConfiguration#period", "beginChunk");
        opts.put("jdk.ClassDefine#stackTrace", "true");
        opts.put("jdk.FileForce#stackTrace", "true");
        opts.put("jdk.SocketRead#threshold", "10 ms");
        opts.put("jdk.MetaspaceGCThreshold#enabled", "true");
        opts.put("jdk.SafepointEnd#enabled", "false");
        opts.put("jdk.JavaMonitorWait#stackTrace", "true");
        opts.put("jdk.TLSHandshake#enabled", "false");
        opts.put("jdk.CodeSweeperStatistics#period", "everyChunk");
        opts.put("jdk.IntFlagChanged#enabled", "true");
        opts.put("jdk.CompilerPhase#enabled", "true");
        opts.put("jdk.ClassUnload#enabled", "false");
        opts.put("jdk.MetaspaceAllocationFailure#stackTrace", "true");
        opts.put("jdk.ExecuteVMOperation#enabled", "true");
        opts.put("jdk.ThreadSleep#threshold", "10 ms");
        opts.put("jdk.ThreadAllocationStatistics#enabled", "true");
        opts.put("jdk.ZThreadPhase#enabled", "true");
        opts.put("jdk.ThreadCPULoad#period", "10 s");
        opts.put("jdk.ModuleExport#period", "endChunk");
        opts.put("jdk.SystemProcess#enabled", "true");
        opts.put("jdk.GCHeapConfiguration#period", "beginChunk");
        opts.put("jdk.GCPhasePauseLevel4#threshold", "0 ns");
        opts.put("jdk.Compilation#enabled", "true");
        opts.put("jdk.JavaThreadStatistics#period", "1 s");
        opts.put("jdk.BooleanFlag#enabled", "true");
        opts.put("jdk.G1MMU#enabled", "true");
        opts.put("jdk.G1HeapRegionInformation#period", "everyChunk");
        opts.put("jdk.SweepCodeCache#threshold", "100 ms");
        opts.put("jdk.LongFlag#enabled", "true");
        opts.put("jdk.GCHeapConfiguration#enabled", "true");
        opts.put("jdk.OldGarbageCollection#threshold", "0 ns");
        opts.put("jdk.ClassLoaderStatistics#period", "everyChunk");
        opts.put("jdk.SafepointCleanupTask#enabled", "false");
        opts.put("jdk.CodeSweeperConfiguration#period", "beginChunk");
        opts.put("jdk.G1HeapSummary#enabled", "true");
        opts.put("jdk.LongFlagChanged#enabled", "true");
        opts.put("jdk.NativeMethodSample#enabled", "true");
        opts.put("jdk.SocketWrite#threshold", "10 ms");
        opts.put("jdk.VirtualizationInformation#period", "beginChunk");
        opts.put("jdk.ExecutionSample#enabled", "true");
        opts.put("jdk.GCPhaseConcurrent#threshold", "0 ns");
        opts.put("jdk.JavaMonitorWait#threshold", "10 ms");
        opts.put("jdk.CPUInformation#period", "beginChunk");
        opts.put("jdk.ObjectAllocationInNewTLAB#enabled", "true");
        opts.put("jdk.JavaMonitorInflate#enabled", "true");
        opts.put("jdk.ClassLoad#enabled", "false");
        opts.put("jdk.SafepointCleanupTask#threshold", "0 ns");
        opts.put("jdk.ExceptionStatistics#enabled", "true");
        opts.put("jdk.DataLoss#enabled", "true");
        opts.put("jdk.ThreadStart#stackTrace", "true");
        opts.put("jdk.ObjectAllocationOutsideTLAB#stackTrace", "true");
        opts.put("jdk.ZStatisticsCounter#enabled", "true");
        opts.put("jdk.EvacuationInformation#enabled", "true");
        opts.put("jdk.BiasedLockRevocation#stackTrace", "true");
        opts.put("jdk.BiasedLockClassRevocation#enabled", "true");
        opts.put("jdk.ParallelOldGarbageCollection#threshold", "0 ns");
        opts.put("jdk.FileForce#threshold", "10 ms");
        opts.put("jdk.OldObjectSample#enabled", "true");
        opts.put("jdk.MetaspaceOOM#enabled", "true");
        opts.put("jdk.CompilerPhase#threshold", "10 s");
        opts.put("jdk.ThreadDump#enabled", "true");
        opts.put("jdk.SafepointCleanup#enabled", "false");
        opts.put("jdk.SafepointStateSynchronization#threshold", "0 ns");
        opts.put("jdk.ClassLoadingStatistics#enabled", "true");
        opts.put("jdk.CompilerStatistics#enabled", "true");
        opts.put("jdk.ClassLoadingStatistics#period", "1 s");
        opts.put("jdk.G1GarbageCollection#threshold", "0 ns");
        opts.put("jdk.GCSurvivorConfiguration#enabled", "true");
        opts.put("jdk.CPUTimeStampCounter#enabled", "true");
        opts.put("jdk.PhysicalMemory#enabled", "true");
        opts.put("jdk.GCPhaseConcurrent#enabled", "false");
        opts.put("jdk.ParallelOldGarbageCollection#enabled", "true");
        opts.put("jdk.GCPhasePauseLevel3#enabled", "false");
        opts.put("jdk.UnsignedLongFlagChanged#enabled", "true");
        opts.put("jdk.SweepCodeCache#enabled", "true");
        opts.put("jdk.DumpReason#enabled", "true");
        opts.put("jdk.SafepointBegin#threshold", "0 ns");
        opts.put("jdk.X509Validation#enabled", "false");
        opts.put("jdk.FileForce#enabled", "true");
        opts.put("jdk.JavaExceptionThrow#enabled", "false");
        opts.put("jdk.InitialEnvironmentVariable#period", "beginChunk");
        opts.put("jdk.SecurityPropertyModification#enabled", "false");
        opts.put("jdk.ThreadContextSwitchRate#enabled", "true");
        opts.put("jdk.DoubleFlag#period", "beginChunk");
        opts.put("jdk.AllocationRequiringGC#enabled", "false");
        opts.put("jdk.SafepointBegin#enabled", "true");
        opts.put("jdk.NetworkUtilization#enabled", "true");
        opts.put("jdk.GarbageCollection#threshold", "0 ns");
        opts.put("jdk.StringFlag#period", "beginChunk");
        opts.put("jdk.X509Certificate#enabled", "false");
        opts.put("jdk.GCPhasePauseLevel1#threshold", "0 ns");
        opts.put("jdk.UnsignedLongFlag#enabled", "true");
        opts.put("jdk.OSInformation#period", "beginChunk");
        opts.put("jdk.MetaspaceSummary#enabled", "true");
        opts.put("jdk.ExecutionSample#period", "20 ms");
        opts.put("jdk.G1AdaptiveIHOP#enabled", "true");
        opts.put("jdk.TLSHandshake#stackTrace", "true");
        opts.put("jdk.JavaMonitorEnter#enabled", "true");
        opts.put("jdk.G1BasicIHOP#enabled", "true");
        opts.put("jdk.GCPhasePauseLevel1#enabled", "true");
        opts.put("jdk.DoubleFlag#enabled", "true");
        opts.put("jdk.BiasedLockSelfRevocation#threshold", "0 ns");
        opts.put("jdk.NativeLibrary#period", "everyChunk");
        opts.put("jdk.GCPhasePauseLevel2#threshold", "0 ns");
        opts.put("jdk.CodeCacheStatistics#enabled", "true");
        opts.put("jdk.InitialSystemProperty#period", "beginChunk");
        opts.put("jdk.FileWrite#threshold", "10 ms");
        opts.put("jdk.GCPhasePauseLevel4#enabled", "false");
        opts.put("jdk.YoungGenerationConfiguration#enabled", "true");
        opts.put("jdk.SocketWrite#stackTrace", "true");
        opts.put("jdk.JavaMonitorInflate#stackTrace", "true");
        opts.put("jdk.CodeCacheStatistics#period", "everyChunk");
        opts.put("jdk.LongFlag#period", "beginChunk");
        opts.put("jdk.G1EvacuationYoungStatistics#enabled", "true");
        opts.put("jdk.JavaThreadStatistics#enabled", "true");
        opts.put("jdk.ShenandoahHeapRegionInformation#period", "everyChunk");
        opts.put("jdk.GCTLABConfiguration#enabled", "true");
        opts.put("jdk.UnsignedIntFlag#enabled", "true");
        opts.put("jdk.G1HeapRegionTypeChange#enabled", "false");
        opts.put("jdk.ThreadEnd#enabled", "true");
        opts.put("jdk.GCTLABConfiguration#period", "beginChunk");
        opts.put("jdk.DoubleFlagChanged#enabled", "true");
        opts.put("jdk.ActiveRecording#enabled", "true");
        opts.put("jdk.SafepointStateSynchronization#enabled", "false");
        opts.put("jdk.BiasedLockClassRevocation#stackTrace", "true");
        opts.put("jdk.GCPhasePause#threshold", "0 ns");
        opts.put("jdk.ObjectCount#period", "everyChunk");
        opts.put("jdk.ZStatisticsCounter#threshold", "10 ms");
        opts.put("jdk.ThreadPark#stackTrace", "true");
        opts.put("jdk.SystemProcess#period", "endChunk");
        opts.put("jdk.ClassLoaderStatistics#enabled", "true");
        opts.put("jdk.PSHeapSummary#enabled", "true");
        opts.put("jdk.BiasedLockSelfRevocation#enabled", "true");
        opts.put("jdk.UnsignedLongFlag#period", "beginChunk");
        opts.put("jdk.Shutdown#stackTrace", "true");
        opts.put("jdk.X509Certificate#stackTrace", "true");
        opts.put("jdk.ThreadDump#period", "60 s");
        opts.put("jdk.X509Validation#stackTrace", "true");
        opts.put("jdk.ObjectCountAfterGC#enabled", "false");
        opts.put("jdk.GCSurvivorConfiguration#period", "beginChunk");
        opts.put("jdk.ReservedStackActivation#stackTrace", "true");
        opts.put("jdk.JVMInformation#period", "beginChunk");
        opts.put("jdk.CodeSweeperStatistics#enabled", "true");
        opts.put("jdk.SafepointEnd#threshold", "0 ns");
        opts.put("jdk.CPUTimeStampCounter#period", "beginChunk");
        opts.put("jdk.SocketRead#enabled", "true");
        opts.put("jdk.JavaErrorThrow#enabled", "true");
        opts.put("jdk.ObjectAllocationOutsideTLAB#enabled", "true");
        opts.put("jdk.NetworkUtilization#period", "5 s");
        opts.put("jdk.MetaspaceChunkFreeListSummary#enabled", "true");

        return opts;
    }

    public String connectorType() {
        return connectorType;
    }

    public String connectorName() {
        return connectorType() + "-" + testRunId;
    }

    public String capturedSchemaName() {
        return "inventory";
    }

    public String capturedTableName() {
        return "ctable";
    }

    public String finishTableName() {
        return "finish";
    }

    public String finishTopicName() {
        return String.format("dbserver-%s.%s.%s", testRunId, capturedSchemaName(), finishTableName());
    }

    public String capturedTopicName() {
        return String.format("dbserver-%s.%s.%s", testRunId, capturedSchemaName(), capturedTableName());
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    public Duration waitTime() {
        return waitTime;
    }

    public int populateTxCount() {
        return populateTxCount;
    }

    public int populateChangesPerTxCount() {
        return populateChangesPerTxCount;
    }

    public int schemasCount() {
        return schemasCount;
    }

    public int tablesCount() {
        return tablesCount;
    }

    public int columnsCount() {
        return columnsCount;
    }

    public int columnLength() {
        return columnLength;
    }

    public int totalMessageCount() {
        return populateTxCount() * populateChangesPerTxCount();
    }

    public String kafkaConnectHost() {
        return kafkaConnectHost;
    }

    public int kafkaConnectPort() {
        return kafkaConnectPort;
    }

    public String kafkaHost() {
        return kafkaHost;
    }

    public int kafkaPort() {
        return kafkaPort;
    }

    public String jmxHost() {
        return jmxHost;
    }

    public int jmxPort() {
        return jmxPort;
    }

    public String testRunId() {
        return testRunId;
    }

    public String testReportFile() {
        return reportFile;
    }

    public Configuration databaseConfiguration() {
        return databaseConfiguration;
    }
}
