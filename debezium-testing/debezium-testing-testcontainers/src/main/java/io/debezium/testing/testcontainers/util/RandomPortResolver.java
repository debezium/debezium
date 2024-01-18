/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * A port resolver which provides a random free port
 * <p>
 * Due to the naive implementation this may be prone
 * <ul>
 *     <li>Allocation race conditions</li>
 *     <li>TCP ports stuck in TIME_WAIT state </li>
 * </ul>
 */
public class RandomPortResolver implements PortResolver {
    @Override
    public int resolveFreePort() {
        try (var serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
        catch (IOException e) {
            return -1;
        }
    }

    @Override
    public void releasePort(int port) {
        // no-op
    }
}
