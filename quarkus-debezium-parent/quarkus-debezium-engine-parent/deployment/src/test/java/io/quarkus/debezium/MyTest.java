/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.runtime.Debezium;
import io.quarkus.test.InjectMock;
import io.quarkus.test.QuarkusUnitTest;

public class MyTest {

    @InjectMock
    Debezium debezium;

    @RegisterExtension
    static final QuarkusUnitTest application = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class))
            .overrideConfigKey("quarkus.debezium.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .overrideConfigKey("quarkus.debezium.name", "test")
            .overrideConfigKey("quarkus.debezium.topic.prefix", "dbserver1")
            .overrideConfigKey("quarkus.debezium.table.include.list", "inventory.products")
            .overrideConfigKey("quarkus.debezium.plugin.name", "pgoutput")
            .overrideConfigKey("quarkus.debezium.snapshot.mode", "never");

    @Test
    void name() {
        System.out.println(application);
        System.out.println("ssssssdddddddd");
    }
}
