/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.sqlserver;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.system.fixtures.connectors.SqlServerConnector;
import io.debezium.testing.system.fixtures.databases.DockerSqlServer;
import io.debezium.testing.system.fixtures.kafka.DockerKafka;
import io.debezium.testing.system.tests.DockerConnectorTest;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("sqlserver")
@Tag("docker")
@Tag("rhel")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DockerRhelSqlServerConnectorIT
        extends DockerConnectorTest<SqlDatabaseController>
        implements DockerKafka, DockerSqlServer, SqlServerConnector, SqlServerTestCases {
}
