/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.junit.Test;

import io.debezium.connector.simple.SimpleSourceConnector;

public class PreMadeMainTest {

    @Test
    public void main() {
        System.setProperty(Main.HANDLER_CLASS_CONF_NAME, TestHandler.class.getName());
        System.setProperty(EmbeddedEngine.ENGINE_NAME.name(), "name");
        System.setProperty(EmbeddedEngine.CONNECTOR_CLASS.name(), SimpleSourceConnector.class.getName());
        System.setProperty(EmbeddedEngine.OFFSET_STORAGE.name(), MemoryOffsetBackingStore.class.getName());

        final Main main = new Main();
        main.run();
    }
}
