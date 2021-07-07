/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.mysql;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.openshift.OcpConnectorTest;
import io.debezium.testing.openshift.fixtures.connectors.MySqlConnector;
import io.debezium.testing.openshift.fixtures.databases.OcpMySql;
import io.debezium.testing.openshift.fixtures.kafka.OcpKafka;
import io.debezium.testing.openshift.fixtures.registry.OcpApicurio;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("mysql")
@Tag("openshift")
@Tag("avro")
@Tag("apicurio")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OcpAvroMySqlConnectorIT
        extends OcpConnectorTest<SqlDatabaseController>
        implements OcpKafka, OcpMySql, MySqlConnector, MySqlTestCases, OcpApicurio {
}
