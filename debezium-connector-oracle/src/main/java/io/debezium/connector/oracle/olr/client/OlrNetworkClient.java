/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoRequest;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RedoResponse;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.RequestCode;
import io.debezium.connector.oracle.proto.OpenLogReplicatorProtocol.ResponseCode;

/**
 * An OpenLogReplicator network client that communicates using JSON streaming payloads.
 *
 * @author Chris Cranford
 */
public class OlrNetworkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlrNetworkClient.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final String hostName;
    private final int port;
    private final String sourceName;

    private SocketChannel channel;
    private boolean skipToStartScn;
    private Scn startScn;
    private long prevScn;

    /**
     * Create the OpenLogReplicator network client.
     *
     * @param connectorConfig connector configuration
     */
    public OlrNetworkClient(OracleConnectorConfig connectorConfig) {
        this.hostName = connectorConfig.getOpenLogReplicatorHostname();
        this.port = connectorConfig.getOpenLogReplicatorPort();
        this.sourceName = connectorConfig.getOpenLogReplicatorSource();
    }

    /**
     * Connect to the OpenLogReplicator process.
     *
     * @param scn the checkpoint commit to begin streaming from
     * @param index the checkpoint commit sequence index to begin streaming from
     * @return true if the connection was established, false if the connection failed
     */
    public boolean connect(Scn scn, Long index) {
        if (scn == null || scn.isNull()) {
            throw new OlrNetworkClientException("Cannot connect and start with a null system change number");
        }
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(true);
            if (channel.connect(new InetSocketAddress(hostName, port))) {
                this.startScn = scn;
                return startFrom(scn, index);
            }
            return false;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to connect and start", e);
        }
    }

    /**
     * Disconnect from the OpenLogReplicator network service.
     */
    public void disconnect() {
        try {
            if (channel.isOpen()) {
                try {
                    channel.shutdownInput();
                }
                catch (Exception e) {
                    // ignored
                }
                try {
                    channel.shutdownOutput();
                }
                catch (Exception e) {
                    // ignored
                }
                channel.close();
            }
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to disconnect client.", e);
        }
    }

    /**
     * Returns whether the network client is connected to the OpenLogReplicator process.
     *
     * @return true if the client is connected, false otherwise.
     */
    public boolean isConnected() {
        return channel.isConnected();
    }

    /**
     * A blocking call that reads the next streaming event from the OpenLogReplicator process.
     *
     * @return the streaming event
     */
    public StreamingEvent readEvent() throws OlrNetworkClientException {
        final StreamingEvent event;
        if (skipToStartScn) {
            event = readNextEventWithStartScnSkip();
        }
        else {
            event = readNextEvent();
        }

        LOGGER.trace("Received Event: {}", event);
        return event;
    }

    public void confirm(Scn scn, Long index) {
        confirm(scn.longValue(), index);
    }

    private StreamingEvent readNextEventWithStartScnSkip() {
        boolean notifySkip = true;

        StreamingEvent event = null;
        while (skipToStartScn) {
            event = readNextEvent();
            // todo: what if we restart mid-transaction?
            if (event.getScn().compareTo(startScn) < 0) {
                if (notifySkip) {
                    LOGGER.info("Advancing change stream to SCN {}", startScn);
                    notifySkip = false;
                }
                continue;
            }
            skipToStartScn = false;
        }

        LOGGER.info("Stream advanced, reading stream starting at {}", event.getScn());
        return event;
    }

    private StreamingEvent readNextEvent() {
        final String data = new String(read().array(), StandardCharsets.UTF_8);
        try {
            return mapper.readValue(data, StreamingEvent.class);
        }
        catch (JsonProcessingException e) {
            throw new OlrNetworkClientException("Failed to deserialize network packet: " + data, e);
        }
    }

    private void confirm(long newScn, Long index) {
        if (prevScn != 0 && prevScn < newScn && index != null) {
            LOGGER.info("Confirming SCN {} with index {}", newScn, index);
            send(createRequest(RequestCode.CONFIRM).setCScn(newScn).setCIdx(index).build());
        }
        prevScn = newScn;
    }

    private boolean startFrom(Scn scn, Long index) {
        if (index != null) {
            LOGGER.info("Streaming will start at SCN {} with index {}.", scn, index);
        }
        else {
            LOGGER.info("Streaming will start at SCN {}.", scn);
            skipToStartScn = true;
        }
        send(createRequest(RequestCode.INFO).build());

        RedoResponse response = readResponse();
        if (response.getCode() == ResponseCode.REPLICATE) {
            LOGGER.info("OpenLogReplicator has already started, continue from SCN {}", scn);
            if (index != null) {
                send(createRequest(RequestCode.CONTINUE).setCScn(scn.longValue()).setCIdx(index).build());
            }
            else {
                send(createRequest(RequestCode.CONTINUE).setScn(scn.longValue()).build());
            }
        }
        else if (response.getCode() == ResponseCode.READY) {
            // todo: add support for continue index (c_idx)??
            LOGGER.info("OpenLogReplicator ready, streaming from SCN {}.", scn);
            send(createRequest(RequestCode.START).setScn(scn.longValue()).build());
        }
        else {
            LOGGER.warn("Failed to get proper response from INFO request.");
            return false;
        }

        response = readResponse();
        if (response.getCode() != ResponseCode.REPLICATE) {
            LOGGER.warn("Server failed to enter streaming mode, OpenLogReplicator client shutting down.");
            return false;
        }

        LOGGER.info("OpenLogReplicator streaming client started successfully.");
        return true;
    }

    private RedoRequest.Builder createRequest(RequestCode requestCode) {
        return RedoRequest.newBuilder().setCode(requestCode).setDatabaseName(sourceName);
    }

    private RedoResponse readResponse() {
        try {
            return RedoResponse.parseFrom(read().array());
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to read response", e);
        }
    }

    private ByteBuffer read() {
        // Read the packet size
        final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
        fillBuffer(sizeBuffer);

        // Read the packet
        final int messageSize = sizeBuffer.getInt();
        final ByteBuffer payload = ByteBuffer.allocate(messageSize);
        fillBuffer(payload);

        return payload;
    }

    @SuppressWarnings("UnusedReturnValue")
    private int send(RedoRequest request) {
        try {
            // We need to write the size (4 bytes) plus the payload
            final ByteBuffer buffer = ByteBuffer.allocate(4 + request.getSerializedSize());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(request.getSerializedSize());
            buffer.put(request.toByteArray());
            buffer.flip();
            return channel.write(buffer);
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to send request to server", e);
        }
    }

    private void fillBuffer(ByteBuffer buffer) {
        try {
            int remaining = buffer.remaining();
            while (remaining > 0) {
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    throw new OlrNetworkClientException("Connection lost");
                }
                remaining -= bytesRead;
            }
            buffer.flip();
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to fill byte buffer", e);
        }
    }

}
