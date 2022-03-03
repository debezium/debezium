/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.perf;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetricsMXBean;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;
import io.debezium.util.IoUtil;

import jdk.management.jfr.FlightRecorderMXBean;

public class DeploymentController {

    private static final String MEDIA_TYPE_JSON = "application/json";
    private static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_NO_CONTENT = 204;

    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentController.class);

    private static final String MBEAN_NAME_JFR = "jdk.management.jfr:type=FlightRecorder";
    private static final String MBEAN_NAME_DEBEZIUM_STREAMING_PATTERN = "debezium.%s:type=connector-metrics,context=streaming,server=dbserver-%s";
    private static final String MBEAN_NAME_DEBEZIUM_SNAPSHOT_PATTERN = "debezium.%s:type=connector-metrics,context=snapshot,server=dbserver-%s";

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private final TestConfiguration config;
    private final String kafkaConnectUrl;
    private final String jmxUrl;
    private final ObjectName snapshotMBeanName;
    private final ObjectName streamingMBeanName;

    private FlightRecorderMXBean mbeanJfr;
    private MBeanServerConnection mbeanConnection;
    private long jfrRecordingId;
    private Instant firstDataMessageTimestamp;
    private Instant lastDataMessageTimestamp;
    private String recordingName = "";

    public DeploymentController(TestConfiguration config, String connectorType) {
        this.config = config;
        this.kafkaConnectUrl = String.format("http://%s:%s/connectors/", config.kafkaConnectHost(), config.kafkaConnectPort());
        this.jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", config.jmxHost(), config.jmxPort());
        try {
            this.snapshotMBeanName = new ObjectName(
                    String.format(MBEAN_NAME_DEBEZIUM_SNAPSHOT_PATTERN, connectorType, config.testRunId()));
            this.streamingMBeanName = new ObjectName(
                    String.format(MBEAN_NAME_DEBEZIUM_STREAMING_PATTERN, connectorType, config.testRunId()));
            connectToJmx();
        }
        catch (MalformedObjectNameException | IOException e) {
            throw new DebeziumException(e);
        }
    }

    private String connectorUrl() {
        return kafkaConnectUrl + config.connectorName();
    }

    private String jfrRecordingFilename(long recordingId) {
        return "test_" + config.connectorType() + "_" + recordingName + ".jfr";
    }

    private String readRegistrationRequest(String scenario) throws Exception {
        final InputStream is = getClass().getResourceAsStream("/" + config.connectorType() + "/" + scenario + ".json");
        return IoUtil.read(is)
                .replace("#CONNECTOR_NAME#", config.connectorName())
                .replace("#CAPTURED_SCHEMA#", config.capturedSchemaName())
                .replace("#CAPTURED_TABLE#", config.capturedTableName())
                .replace("#TEST_ID#", config.testRunId());
    }

    public void connectToJmx() throws IOException, MalformedObjectNameException {
        final JMXServiceURL url = new JMXServiceURL(jmxUrl);
        final JMXConnector jmxConnector = JMXConnectorFactory.newJMXConnector(url, null);
        jmxConnector.connect();
        mbeanConnection = jmxConnector.getMBeanServerConnection();
        mbeanJfr = JMX.newMXBeanProxy(mbeanConnection, new ObjectName(MBEAN_NAME_JFR),
                FlightRecorderMXBean.class);
    }

    public void registerConnector(String scenario) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(kafkaConnectUrl))
                .header(HTTP_HEADER_CONTENT_TYPE, MEDIA_TYPE_JSON)
                .POST(BodyPublishers.ofString(readRegistrationRequest(scenario)))
                .build();
        final HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        LOGGER.debug("Registration requested completed with result: {}", response.body());
        Assertions.assertThat(response.statusCode()).isEqualTo(HTTP_CREATED);
    }

    public void unregisterConnector() throws Exception {
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(connectorUrl()))
                .DELETE()
                .build();
        final HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        LOGGER.debug("Unregistration requested completed with result: {}", response.body());
        Assertions.assertThat(response.statusCode()).isEqualTo(HTTP_NO_CONTENT);
    }

    public Instant firstDataMessageTimestamp() {
        return firstDataMessageTimestamp;
    }

    public Instant lastDataMessageTimestamp() {
        return lastDataMessageTimestamp;
    }

    private StreamingChangeEventSourceMetricsMXBean debeziumStreamingMBean() throws Exception {
        return JMX.newMXBeanProxy(mbeanConnection, streamingMBeanName,
                StreamingChangeEventSourceMetricsMXBean.class);
    }

    private SnapshotChangeEventSourceMetricsMXBean debeziumSnapshotMBean() throws Exception {
        return JMX.newMXBeanProxy(mbeanConnection, snapshotMBeanName,
                SnapshotChangeEventSourceMetricsMXBean.class);
    }

    public Instant waitForStreamingToStart() {
        final AtomicReference<Instant> now = new AtomicReference<>();
        Awaitility.waitAtMost(config.waitTime())
                .pollInterval(config.pollInterval())
                .ignoreException(UndeclaredThrowableException.class)
                .until(() -> {
                    now.set(Instant.now());
                    return debeziumStreamingMBean().isConnected();
                });
        LOGGER.info("Streaming started at {}", now);
        return now.get();
    }

    public Instant waitForSnapshotToStart() {
        final AtomicReference<Instant> now = new AtomicReference<>();
        Awaitility.waitAtMost(config.waitTime())
                .pollInterval(config.pollInterval())
                .ignoreException(UndeclaredThrowableException.class)
                .until(() -> {
                    now.set(Instant.now());
                    return debeziumSnapshotMBean().getSnapshotRunning();
                });
        LOGGER.info("Snapshot started at {}", now);
        return now.get();
    }

    public Instant waitForSnapshotToStop() {
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .ignoreException(UndeclaredThrowableException.class)
                .until(() -> debeziumSnapshotMBean().getSnapshotCompleted());
        final Instant now = Instant.now();
        LOGGER.info("Snapshot stopped at {}", now);
        return now;
    }

    public void startRecording(String name) throws Exception {
        recordingName = name;
        jfrRecordingId = mbeanJfr.newRecording();
        mbeanJfr.setRecordingSettings(jfrRecordingId, config.recordingSettings());
        mbeanJfr.startRecording(jfrRecordingId);
        LOGGER.info("Java Flight Recording '{}' started", recordingName);
    }

    public void stopRecording() throws Exception {
        mbeanJfr.stopRecording(jfrRecordingId);
        LOGGER.info("Java Flight Recording stopped");
        dumpRecordingToFile(jfrRecordingId);
        mbeanJfr.closeRecording(jfrRecordingId);
        LOGGER.info("Java Flight Recording file saved");
    }

    public void waitForCaptureCompletion() throws Exception {
        final Map<String, Object> kafkaConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaHost() + ":" + config.kafkaPort(),
                ConsumerConfig.GROUP_ID_CONFIG, "40",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final AtomicLong startTimestamp = new AtomicLong();
        final AtomicLong endTimestamp = new AtomicLong();
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Collections.singleton(config.finishTopicName()));
            consumer.seekToBeginning(Collections.emptySet());
            LOGGER.info("Waiting for last message to arrive");
            Awaitility.await().atMost(config.waitTime())
                    .pollInterval(config.pollInterval())
                    .until(() -> {
                        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                        if (!records.isEmpty()) {
                            final ConsumerRecord<String, String> record = records.iterator().next();
                            endTimestamp.set(parseTimestamp(record.value()));
                            endTimestamp.set(record.timestamp());
                        }
                        return !records.isEmpty();
                    });
            lastDataMessageTimestamp = Instant.ofEpochMilli(endTimestamp.get());
            LOGGER.info("Last message with timestamp {} arrived", lastDataMessageTimestamp);

            LOGGER.info("Reading timestamp of the first message");
            consumer.unsubscribe();
            consumer.subscribe(Collections.singleton(config.capturedTopicName()));
            consumer.seekToBeginning(Collections.emptySet());
            Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    final ConsumerRecord<String, String> record = records.iterator().next();
                    startTimestamp.set(parseTimestamp(record.value()));
                    startTimestamp.set(record.timestamp());
                }
                return !records.isEmpty();
            });
        }
        firstDataMessageTimestamp = Instant.ofEpochMilli(startTimestamp.get());
        LOGGER.info("First message with timestamp {} acquired", firstDataMessageTimestamp);
    }

    private long parseTimestamp(String envelopeStr) throws Exception {
        final Document envelope = DocumentReader.defaultReader().read(envelopeStr);
        return envelope.getDocument("payload").getLong("ts_ms");
    }

    private void dumpRecordingToFile(long recordingId) throws Exception {
        final File dumpFile = new File(jfrRecordingFilename(recordingId));
        final long streamId = mbeanJfr.openStream(recordingId, null);
        try (OutputStream fos = new FileOutputStream(dumpFile); OutputStream bos = new BufferedOutputStream(fos)) {
            while (true) {
                byte[] data = mbeanJfr.readStream(streamId);
                if (data == null) {
                    bos.flush();
                    break;
                }
                bos.write(data);
            }
        }
        mbeanJfr.closeStream(streamId);
    }
}
