/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy;

import static io.debezium.util.Strings.isNullOrEmpty;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.DefaultSSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.EventDataDeserializationExceptionData;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.RowDeserializers;
import io.debezium.connector.mysql.StopEventDataDeserializer;
import io.debezium.connector.mysql.TransactionPayloadDeserializer;

/**
 * @author Chris Cranford
 */
public abstract class AbstractBinaryLogClientConfigurator implements BinaryLogClientConfigurator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBinaryLogClientConfigurator.class);

    private final MySqlConnectorConfig connectorConfig;
    private final float heartbeatIntervalFactor = 0.8f;
    private final EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode;

    public AbstractBinaryLogClientConfigurator(MySqlConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.eventDeserializationFailureHandlingMode = connectorConfig.getEventProcessingFailureHandlingMode();
    }

    @Override
    public BinaryLogClient configure(BinaryLogClient client, ThreadFactory threadFactory, AbstractConnectorConnection connection) {
        client.setThreadFactory(threadFactory);
        client.setServerId(connectorConfig.serverId());
        client.setSSLMode(sslModeFor(connectorConfig.sslMode()));

        if (connectorConfig.sslModeEnabled()) {
            SSLSocketFactory sslSocketFactory = getBinlogSslSocketFactory(connectorConfig, connection);
            if (sslSocketFactory != null) {
                client.setSslSocketFactory(sslSocketFactory);
            }
        }

        configureReplicaCompatibility(client);

        Configuration configuration = connectorConfig.getConfig();
        client.setKeepAlive(configuration.getBoolean(MySqlConnectorConfig.KEEP_ALIVE));
        final long keepAliveInterval = configuration.getLong(MySqlConnectorConfig.KEEP_ALIVE_INTERVAL_MS);
        client.setKeepAliveInterval(keepAliveInterval);
        // Considering heartbeatInterval should be less than keepAliveInterval, we use the heartbeatIntervalFactor
        // multiply by keepAliveInterval and set the result value to heartbeatInterval.The default value of heartbeatIntervalFactor
        // is 0.8, and we believe the left time (0.2 * keepAliveInterval) is enough to process the packet received from the MySQL server.
        client.setHeartbeatInterval((long) (keepAliveInterval * heartbeatIntervalFactor));

        client.setEventDeserializer(createEventDeserializer());

        return client;
    }

    protected EventDeserializer createEventDeserializer() {
        // Set up the event deserializer with additional type(s) ...
        final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();
        EventDeserializer eventDeserializer = new EventDeserializer() {
            @Override
            public Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
                try {
                    // Delegate to the superclass ...
                    Event event = super.nextEvent(inputStream);

                    // We have to record the most recent TableMapEventData for each table number for our custom deserializers ...
                    if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                        TableMapEventData tableMapEvent = event.getData();
                        tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                    }

                    // DBZ-2663 Handle for transaction payload and capture the table map event and add it to the map
                    if (event.getHeader().getEventType() == EventType.TRANSACTION_PAYLOAD) {
                        TransactionPayloadEventData transactionPayloadEventData = (TransactionPayloadEventData) event.getData();
                        /**
                         * Loop over the uncompressed events in the transaction payload event and add the table map
                         * event in the map of table events
                         **/
                        for (Event uncompressedEvent : transactionPayloadEventData.getUncompressedEvents()) {
                            if (uncompressedEvent.getHeader().getEventType() == EventType.TABLE_MAP
                                    && uncompressedEvent.getData() != null) {
                                TableMapEventData tableMapEvent = (TableMapEventData) uncompressedEvent.getData();
                                tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                            }
                        }
                    }

                    // DBZ-5126 Clean cache on rotate event to prevent it from growing indefinitely.
                    if (event.getHeader().getEventType() == EventType.ROTATE && event.getHeader().getTimestamp() != 0) {
                        tableMapEventByTableId.clear();
                    }
                    return event;
                }
                // DBZ-217 In case an event couldn't be read we create a pseudo-event for the sake of logging
                catch (EventDataDeserializationException edde) {
                    // DBZ-3095 As of Java 15, when reaching EOF in the binlog stream, the polling loop in
                    // BinaryLogClient#listenForEventPackets() keeps returning values != -1 from peek();
                    // this causes the loop to never finish
                    // Propagating the exception (either EOF or socket closed) causes the loop to be aborted
                    // in this case
                    if (edde.getCause() instanceof IOException) {
                        throw edde;
                    }

                    EventHeaderV4 header = new EventHeaderV4();
                    header.setEventType(EventType.INCIDENT);
                    header.setTimestamp(edde.getEventHeader().getTimestamp());
                    header.setServerId(edde.getEventHeader().getServerId());

                    if (edde.getEventHeader() instanceof EventHeaderV4) {
                        header.setEventLength(((EventHeaderV4) edde.getEventHeader()).getEventLength());
                        header.setNextPosition(((EventHeaderV4) edde.getEventHeader()).getNextPosition());
                        header.setFlags(((EventHeaderV4) edde.getEventHeader()).getFlags());
                    }

                    EventData data = new EventDataDeserializationExceptionData(edde);
                    return new Event(header, data);
                }
            }
        };

        // Add our custom deserializers ...
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.TRANSACTION_PAYLOAD,
                new TransactionPayloadDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));

        return eventDeserializer;
    }

    @Override
    public EventType getIncludeSqlQueryEventType() {
        return EventType.ROWS_QUERY;
    }

    protected MySqlConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    protected void configureReplicaCompatibility(BinaryLogClient client) {
        // default is a no-op
    }

    private SSLMode sslModeFor(SecureConnectionMode mode) {
        switch (mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

    private SSLSocketFactory getBinlogSslSocketFactory(MySqlConnectorConfig connectorConfig, AbstractConnectorConnection connection) {
        String acceptedTlsVersion = connection.getSessionVariableForSslVersion();
        if (!isNullOrEmpty(acceptedTlsVersion)) {
            SSLMode sslMode = sslModeFor(connectorConfig.sslMode());
            LOGGER.info("Enable ssl " + sslMode + " mode for connector " + connectorConfig.getLogicalName());

            final char[] keyPasswordArray = connection.connectionConfig().sslKeyStorePassword();
            final String keyFilename = connection.connectionConfig().sslKeyStore();
            final char[] trustPasswordArray = connection.connectionConfig().sslTrustStorePassword();
            final String trustFilename = connection.connectionConfig().sslTrustStore();
            KeyManager[] keyManagers = null;
            if (keyFilename != null) {
                try {
                    KeyStore ks = connection.loadKeyStore(keyFilename, keyPasswordArray);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
                    kmf.init(ks, keyPasswordArray);

                    keyManagers = kmf.getKeyManagers();
                }
                catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
                    throw new DebeziumException("Could not load keystore", e);
                }
            }
            TrustManager[] trustManagers;
            try {
                KeyStore ks = null;
                if (trustFilename != null) {
                    ks = connection.loadKeyStore(trustFilename, trustPasswordArray);
                }

                if (ks == null && (sslMode == SSLMode.PREFERRED || sslMode == SSLMode.REQUIRED)) {
                    trustManagers = new TrustManager[]{
                            new X509TrustManager() {
                                @Override
                                public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                                        throws CertificateException {
                                }

                                @Override
                                public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                                        throws CertificateException {
                                }

                                @Override
                                public X509Certificate[] getAcceptedIssuers() {
                                    return new X509Certificate[0];
                                }
                            }
                    };
                }
                else {
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ks);
                    trustManagers = tmf.getTrustManagers();
                }
            }
            catch (KeyStoreException | NoSuchAlgorithmException e) {
                throw new DebeziumException("Could not load truststore", e);
            }
            // DBZ-1208 Resembles the logic from the upstream BinaryLogClient, only that
            // the accepted TLS version is passed to the constructed factory
            final KeyManager[] finalKMS = keyManagers;
            return new DefaultSSLSocketFactory(acceptedTlsVersion) {

                @Override
                protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
                    sc.init(finalKMS, trustManagers, null);
                }
            };
        }

        return null;
    }
}
