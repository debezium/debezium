/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.kafka.KafkaCluster;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserSql2003;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class KafkaDatabaseHistoryTest {

    private Configuration config;
    private KafkaDatabaseHistory history;
    private KafkaCluster kafka;
    private File dataDir;
    private Map<String, String> source;
    private Map<String, Object> position;
    private String topicName;
    private String ddl;

    @Before
    public void beforeEach() throws Exception {
        source = Collect.hashMapOf("server", "my-server");
        setLogPosition(0);
        topicName = "schema-changes-topic";

        dataDir = Testing.Files.createTestingDirectory("cluster");
        Testing.Files.delete(dataDir);
        kafka = new KafkaCluster().usingDirectory(dataDir)
                                  .deleteDataPriorToStartup(true)
                                  .deleteDataUponShutdown(true)
                                  .addBrokers(1)
                                  .startup();
        history = new KafkaDatabaseHistory();
    }

    @After
    public void afterEach() {
        try {
            if (history != null) history.stop();
        } finally {
            history = null;
            try {
                if (kafka != null) kafka.shutdown();
            } finally {
                kafka = null;
            }
        }
    }

    @Test
    public void shouldStartWithEmptyTopicAndStoreDataAndRecoverAllState() throws Exception {
        // Create the empty topic ...
        kafka.createTopic(topicName, 1, 1);

        // Start up the history ...
        config = Configuration.create()
                              .with(KafkaDatabaseHistory.BOOTSTRAP_SERVERS, kafka.brokerList())
                              .with(KafkaDatabaseHistory.TOPIC, topicName)
                              .with(DatabaseHistory.NAME, "my-db-history")
                              .build();
        history.configure(config,null);
        history.start();
        
        // Should be able to call start more than once ...
        history.start();

        DdlParser recoveryParser = new DdlParserSql2003();
        DdlParser ddlParser = new DdlParserSql2003();
        ddlParser.setCurrentSchema("db1"); // recover does this, so we need to as well
        Tables tables1 = new Tables();
        Tables tables2 = new Tables();
        Tables tables3 = new Tables();

        // Recover from the very beginning ...
        setLogPosition(0);
        history.recover(source, position, tables1, recoveryParser);

        // There should have been nothing to recover ...
        assertThat(tables1.size()).isEqualTo(0);

        // Now record schema changes, which writes out to kafka but doesn't actually change the Tables ...
        setLogPosition(10);
        ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, desc VARCHAR(255) NOT NULL); \n";
        history.record(source, position, "db1", tables1, ddl);
        ddlParser.parse(ddl, tables1);
        assertThat(tables1.size()).isEqualTo(3);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(3);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        setLogPosition(39);
        ddl = "DROP TABLE foo;";
        history.record(source, position, "db1", tables2, ddl);
        ddlParser.parse(ddl, tables2);
        assertThat(tables2.size()).isEqualTo(2);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(2);

        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(source, position, "db1", tables3, ddl);
        ddlParser.parse(ddl, tables3);
        assertThat(tables3.size()).isEqualTo(3);

        // Stop the history (which should stop the producer) ...
        history.stop();
        history = new KafkaDatabaseHistory();
        history.configure(config, null);
        // no need to start

        // Recover from the very beginning to just past the first change ...
        Tables recoveredTables = new Tables();
        setLogPosition(15);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables1);

        // Recover from the very beginning to just past the second change ...
        recoveredTables = new Tables();
        setLogPosition(50);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables2);

        // Recover from the very beginning to just past the third change ...
        recoveredTables = new Tables();
        setLogPosition(10010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);

        // Recover from the very beginning to way past the third change ...
        recoveredTables = new Tables();
        setLogPosition(100000010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertThat(recoveredTables).isEqualTo(tables3);
    }

    protected void setLogPosition(int index) {
        this.position = Collect.hashMapOf("filename", "my-txn-file.log",
                                          "position", index);
    }

}
