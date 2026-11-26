/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.jev;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * A real in-process HTTP server on the loopback interface standing in for the Jev API.
 * It records every request and answers according to a function of the 1-based request number.
 *
 * @author Jiri Pechanec
 */
public class FakeJevServer implements AutoCloseable {

    /**
     * What the server answers to a request.
     */
    public record Reply(int status, String body, Map<String, String> headers, long delayMs) {

        public static Reply ok(final String body) {
            return new Reply(200, body, Map.of(), 0);
        }

        public static Reply status(final int status, final String body) {
            return new Reply(status, body, Map.of(), 0);
        }

        public Reply withHeader(final String name, final String value) {
            final Map<String, String> copy = new TreeMap<>(headers);
            copy.put(name, value);
            return new Reply(status, body, copy, delayMs);
        }

        public Reply withDelay(final long millis) {
            return new Reply(status, body, headers, millis);
        }
    }

    /**
     * A request received by the server. Header names are case insensitive.
     */
    public record Received(String method, String path, Map<String, String> headers, String body) {

        public String header(final String name) {
            return headers.get(name);
        }
    }

    private final HttpServer server;
    private final ExecutorService executor;
    private final List<Received> received = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger counter = new AtomicInteger();
    private volatile IntFunction<Reply> responder = n -> Reply.status(500, "no responder configured");

    public FakeJevServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        executor = Executors.newCachedThreadPool(runnable -> {
            final var thread = new Thread(runnable, "fake-jev");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/", this::handle);
        server.start();
    }

    /**
     * Reads the fixture {@code /jev/<name>} from the test classpath.
     */
    public static String fixture(final String name) throws IOException {
        try (InputStream stream = FakeJevServer.class.getResourceAsStream("/jev/" + name)) {
            if (stream == null) {
                throw new IOException("Missing fixture " + name);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public FakeJevServer respond(final IntFunction<Reply> function) {
        this.responder = function;
        return this;
    }

    public FakeJevServer respondAlways(final Reply reply) {
        return respond(n -> reply);
    }

    public String baseUrl() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    public List<Received> requests() {
        synchronized (received) {
            return List.copyOf(received);
        }
    }

    public int requestCount() {
        return counter.get();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(final HttpExchange exchange) throws IOException {
        try (exchange) {
            final var number = counter.incrementAndGet();
            final Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            exchange.getRequestHeaders().forEach((name, values) -> headers.put(name, values.get(0)));
            final var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.add(new Received(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), headers, body));

            final var reply = responder.apply(number);
            if (reply.delayMs() > 0) {
                try {
                    Thread.sleep(reply.delayMs());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            reply.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            final var bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                exchange.getResponseBody().write(bytes);
            }
        }
        catch (IOException e) {
            // the client may have given up (timeout), nothing to report
        }
    }
}
