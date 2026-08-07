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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
 * <p>The initial handshake is performed while the channel is in blocking mode as it consists of a
 * fixed request and response exchange. Once the server has entered streaming mode, the channel is
 * switched to non-blocking mode and {@link #readEvent()} waits at most {@link #READ_TIMEOUT} for a
 * complete message to arrive before returning {@code null}, buffering any partially received
 * message until the next call.
 *
 * <p>An interrupt raised while the client is communicating with the server is a request to shut
 * down rather than a network failure. Such an interrupt is handled internally by marking the client
 * as no longer connected, so that {@link #isConnected()} reports {@code false} and the caller's read
 * loop stops of its own accord instead of having to interpret a failure.
 *
 * @author Chris Cranford
 */
public class OlrNetworkClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(OlrNetworkClient.class);

    private static final int MESSAGE_SIZE_LENGTH = 4;
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(1);

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final String hostName;
    private final int port;
    private final String sourceName;
    private final ByteBuffer sizeBuffer = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH).order(ByteOrder.LITTLE_ENDIAN);

    private SocketChannel channel;
    private Selector selector;
    private ByteBuffer payloadBuffer;
    private volatile boolean disconnected;
    private boolean skipToStartScn;
    private boolean notifiedSkip;
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
            if (!channel.connect(new InetSocketAddress(hostName, port))) {
                return false;
            }

            this.startScn = scn;
            if (!startFrom(scn, index)) {
                return false;
            }

            // The handshake completed and the server is streaming, all subsequent reads are
            // performed without blocking the caller.
            channel.configureBlocking(false);
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_READ);
            return true;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
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
            disconnected = true;
            if (selector != null) {
                try {
                    selector.close();
                }
                catch (Exception e) {
                    // ignored
                }
                selector = null;
            }
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
        // The channel is only created by #connect and is cleared again by #disconnect, while this is
        // also called from the thread that flushes offsets, so it may be observed as unset.
        return !disconnected && channel != null && channel.isConnected();
    }

    /**
     * Reads the next streaming event from the OpenLogReplicator process, waiting at most
     * {@link #READ_TIMEOUT} for one to arrive.
     *
     * @return the streaming event, or {@code null} if no event became available
     */
    public StreamingEvent readEvent() throws OlrNetworkClientException {
        final StreamingEvent event = readNextEvent();
        if (event == null) {
            return null;
        }

        // Logged before the start SCN check so that skipped events are recorded too.
        LOGGER.trace("Received Event: {}", event);

        if (skipToStartScn && !isStartScnReached(event)) {
            return null;
        }

        return event;
    }

    public void confirm(Scn scn, Long index) {
        confirm(scn.longValue(), index);
    }

    /**
     * Checks whether the change stream has advanced to the requested start system change number,
     * discarding the event when it precedes that point.
     *
     * <p>Discarded events are not confirmed. A change carries the system change number of its own
     * commit block, which is at or below the number the change itself was made at, so a change that
     * is still wanted can belong to a commit block that precedes one being discarded here.
     * Confirming a discarded event would release that commit block before it has been streamed, and
     * OpenLogReplicator only sends what follows a confirmed position. The server is instead allowed
     * to release these once the connector confirms a position it has emitted from.
     *
     * @param event the event that was read from the change stream, never {@code null}
     * @return {@code true} if the event should be emitted, {@code false} if it should be skipped
     */
    private boolean isStartScnReached(StreamingEvent event) {
        // todo: what if we restart mid-transaction?
        if (event.getScn().compareTo(startScn) < 0) {
            LOGGER.trace("Skipped event at SCN {}, has not yet reached start SCN {}: {}", event.getScn(), startScn, event);
            if (!notifiedSkip) {
                LOGGER.info("Advancing change stream to SCN {}", startScn);
                notifiedSkip = true;
            }
            return false;
        }

        skipToStartScn = false;
        LOGGER.info("Stream advanced, reading stream starting at {}", event.getScn());
        return true;
    }

    private StreamingEvent readNextEvent() {
        final ByteBuffer message = read();
        if (message == null) {
            return null;
        }

        final String data = new String(message.array(), StandardCharsets.UTF_8);
        try {
            return mapper.readValue(data, StreamingEvent.class);
        }
        catch (JsonProcessingException e) {
            throw new OlrNetworkClientException("Failed to deserialize network packet: " + data, e);
        }
    }

    private void confirm(long newScn, Long index) {
        if (prevScn != 0 && prevScn < newScn && index != null) {
            LOGGER.debug("Confirming SCN {} with index {}", newScn, index);
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
        if (response == null) {
            return false;
        }
        else if (response.getCode() == ResponseCode.REPLICATE) {
            LOGGER.info("OpenLogReplicator has already started, continue from SCN {}", scn);
            // The position a source is continued from is carried by c_scn and c_idx. The system
            // change number on its own says where to begin reading redo, which only applies to
            // starting a source, and is ignored when continuing one. Sending it there leaves the
            // server to continue from whatever it last had confirmed instead, which is as far back
            // as it still holds, so it streams changes that were emitted long ago.
            send(createRequest(RequestCode.CONTINUE)
                    .setCScn(scn.longValue())
                    .setCIdx(index != null ? index : 0)
                    .build());
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
        if (response == null) {
            return false;
        }
        else if (response.getCode() != ResponseCode.REPLICATE) {
            LOGGER.warn("Server failed to enter streaming mode, OpenLogReplicator client shutting down.");
            return false;
        }

        LOGGER.info("OpenLogReplicator streaming client started successfully.");
        return true;
    }

    private RedoRequest.Builder createRequest(RequestCode requestCode) {
        return RedoRequest.newBuilder().setCode(requestCode).setDatabaseName(sourceName);
    }

    /**
     * Reads a handshake response from the server, while the channel is still in blocking mode.
     *
     * @return the response, or {@code null} if the client was interrupted awaiting the response
     */
    private RedoResponse readResponse() {
        final ByteBuffer response = read();
        if (response == null) {
            return null;
        }
        try {
            return RedoResponse.parseFrom(response.array());
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to read response", e);
        }
    }

    /**
     * Reads the next message from the channel, waiting up to {@link #READ_TIMEOUT} for the message
     * to arrive when the channel is in non-blocking mode.
     *
     * <p>A message that has only partially arrived within that window is retained across calls so
     * that the next read resumes where the previous one left off.
     *
     * @return the message payload, or {@code null} if the message has not been fully received
     */
    private ByteBuffer read() {
        final long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        while (true) {
            // Read the packet size
            if (payloadBuffer == null && fillBuffer(sizeBuffer)) {
                sizeBuffer.flip();
                payloadBuffer = ByteBuffer.allocate(sizeBuffer.getInt());
                sizeBuffer.clear();
            }

            // Read the packet
            if (payloadBuffer != null && fillBuffer(payloadBuffer)) {
                final ByteBuffer payload = payloadBuffer;
                payloadBuffer = null;
                payload.flip();
                return payload;
            }

            if (disconnected || !awaitData(deadline)) {
                return null;
            }
        }
    }

    /**
     * Waits for the channel to become readable, up to the supplied deadline.
     *
     * @param deadline the {@link System#nanoTime()} value at which the wait should give up
     * @return {@code true} if the channel should be read again, {@code false} if the deadline
     *         elapsed or the thread was interrupted
     */
    private boolean awaitData(long deadline) {
        if (Thread.currentThread().isInterrupted()) {
            markInterrupted();
            return false;
        }
        final long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            return false;
        }
        try {
            // Selector#select returns immediately when the thread is interrupted, which the next
            // pass detects above, so a shutdown request is never delayed by the timeout.
            selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            selector.selectedKeys().clear();
            return true;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to wait for data from OpenLogReplicator", e);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private int send(RedoRequest request) {
        try {
            // We need to write the size (4 bytes) plus the payload
            final ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH + request.getSerializedSize());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(request.getSerializedSize());
            buffer.put(request.toByteArray());
            buffer.flip();

            // A non-blocking channel is free to accept only part of the buffer per write
            int written = 0;
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            return written;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
            return 0;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to send request to server", e);
        }
    }

    /**
     * Reads from the channel into the supplied buffer until the buffer is full or, when the channel
     * is in non-blocking mode, until the channel has no more data available.
     *
     * @param buffer the buffer to be filled, never {@code null}
     * @return {@code true} if the buffer was filled, {@code false} if no more data is currently
     *         available and the buffer remains partially filled
     */
    private boolean fillBuffer(ByteBuffer buffer) {
        try {
            while (buffer.hasRemaining()) {
                final int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    throw new OlrNetworkClientException("Connection lost");
                }
                if (bytesRead == 0) {
                    // The channel is non-blocking and there is nothing left to read for now,
                    // the buffer keeps whatever was read so the next call can resume from here.
                    return false;
                }
            }
            return true;
        }
        catch (ClosedByInterruptException e) {
            markInterrupted();
            return false;
        }
        catch (IOException e) {
            throw new OlrNetworkClientException("Failed to fill byte buffer", e);
        }
    }

    /**
     * Records that the client was interrupted while communicating with OpenLogReplicator.
     *
     * <p>An interrupt is a request to shut down rather than a network failure, and it closes the
     * channel when raised by a blocking operation. The interrupt status is preserved and the client
     * marks itself as disconnected so that the caller's read loop ends without an exception having
     * to be raised and interpreted.
     */
    private void markInterrupted() {
        LOGGER.debug("Interrupted while communicating with OpenLogReplicator, marking client disconnected.");
        disconnected = true;
        Thread.currentThread().interrupt();
    }

}