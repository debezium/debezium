/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tests.oracle;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.system.fixtures.connectors.OracleConnector;
import io.debezium.testing.system.fixtures.databases.OcpOracle;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.fixtures.registry.ApicurioAvroConnectorDecorator;
import io.debezium.testing.system.fixtures.registry.OcpApicurio;
import io.debezium.testing.system.tests.OcpConnectorTest;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("oracle")
@Tag("openshift")
@Tag("avro")
@Tag("apicurio")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OcpAvroOracleConnectorIT
        extends OcpConnectorTest<SqlDatabaseController>
        implements OcpKafka, OcpOracle, OracleConnector, OcpApicurio, ApicurioAvroConnectorDecorator, OracleTestCases {
}
