/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.io.IOException;

public interface PortForwardableDatabaseController {
    void forwardDatabasePorts() throws IOException;

    void closeDatabasePortForwards() throws IOException;
}
