/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A TCP proxy that forwards traffic to a database, so that a test can break a connection at the network
 * level instead of shutting the database down.
 *
 * <p>{@link #blackhole()} reproduces the failure mode where the TCP connection stays ESTABLISHED while the
 * database session is gone: traffic stops flowing in both directions, the sockets that are already open are
 * left open so that neither side observes an error, and further connection attempts are refused so that a
 * reconnect cannot succeed.
 *
 * @author Chris Cranford
 */
public class DatabaseTcpProxy implements Closeable {

    private final String targetHost;
    private final int targetPort;
    private final ServerSocket serverSocket;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();

    private volatile boolean blackholed;
    private volatile boolean closed;
    private volatile byte[] requestToFail;
    private volatile byte[] requestToArmFailure;
    private volatile boolean requestFailureArmed;
    private volatile boolean requestFailed;

    private DatabaseTcpProxy(String targetHost, int targetPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());

        final Thread acceptThread = new Thread(this::acceptLoop, "database-tcp-proxy-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /**
     * Starts a proxy on an ephemeral loopback port that forwards to the given database.
     *
     * @param targetHost the database hostname
     * @param targetPort the database port
     * @return the started proxy, never null
     */
    public static DatabaseTcpProxy forward(String targetHost, int targetPort) throws IOException {
        return new DatabaseTcpProxy(targetHost, targetPort);
    }

    public String getHostname() {
        return serverSocket.getInetAddress().getHostAddress();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void failRequestAfter(String request, String precedingRequest) {
        requestToFail = request.getBytes(StandardCharsets.UTF_8);
        requestToArmFailure = precedingRequest.getBytes(StandardCharsets.UTF_8);
    }

    public boolean hasFailedRequest() {
        return requestFailed;
    }

    /**
     * Stops forwarding traffic without closing the connections that are already established, and refuses
     * any further connection attempt.
     */
    public void blackhole() {
        blackholed = true;
        // reconnect attempts now fail immediately rather than hanging
        closeQuietly(serverSocket);
    }

    @Override
    public void close() {
        closed = true;
        closeQuietly(serverSocket);
        sockets.forEach(DatabaseTcpProxy::closeQuietly);
        sockets.clear();
    }

    private void acceptLoop() {
        while (!closed) {
            final Socket incoming;
            try {
                incoming = serverSocket.accept();
            }
            catch (IOException e) {
                // the server socket was closed, either by blackhole() or by close()
                return;
            }

            try {
                final Socket outgoing = new Socket();
                outgoing.connect(new InetSocketAddress(targetHost, targetPort));
                sockets.add(incoming);
                sockets.add(outgoing);
                pump(incoming, outgoing);
                pump(outgoing, incoming);
            }
            catch (IOException e) {
                closeQuietly(incoming);
            }
        }
    }

    private void pump(Socket from, Socket to) {
        final Thread thread = new Thread(() -> {
            final byte[] buffer = new byte[8192];
            try {
                final InputStream input = from.getInputStream();
                final OutputStream output = to.getOutputStream();
                while (!closed) {
                    final int read = input.read(buffer);
                    if (read == -1) {
                        break;
                    }
                    if (blackholed) {
                        // keep draining so that the sender never blocks, but deliver nothing
                        continue;
                    }
                    if (shouldFailRequest(buffer, read)) {
                        closeQuietly(from);
                        closeQuietly(to);
                        return;
                    }
                    output.write(buffer, 0, read);
                    output.flush();
                }
            }
            catch (IOException e) {
                // the connection is gone, there is nothing left to forward
            }
            finally {
                if (!blackholed) {
                    // while blackholed the sockets are deliberately left open so that neither side
                    // notices that the connection is dead; close() tears them down at the end
                    closeQuietly(from);
                    closeQuietly(to);
                }
            }
        }, "database-tcp-proxy-pump");
        thread.setDaemon(true);
        thread.start();
    }

    private boolean shouldFailRequest(byte[] buffer, int length) {
        if (!requestFailureArmed && contains(buffer, length, requestToArmFailure)) {
            requestFailureArmed = true;
            return false;
        }
        if (requestFailureArmed && contains(buffer, length, requestToFail)) {
            requestFailed = true;
            return true;
        }
        return false;
    }

    private boolean contains(byte[] buffer, int length, byte[] request) {
        if (request == null || request.length > length) {
            return false;
        }
        for (int i = 0; i <= length - request.length; i++) {
            int j = 0;
            while (j < request.length && buffer[i + j] == request[j]) {
                j++;
            }
            if (j == request.length) {
                return true;
            }
        }
        return false;
    }

    private static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        }
        catch (IOException e) {
            // ignored
        }
    }
}
