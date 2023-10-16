package io.debezium.testing.system.tests.mongodb.sharded;


import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.connectors.ShardedReplicaMongoConnector;
import io.debezium.testing.system.fixtures.databases.ocp.OcpMongoSharded;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.fixtures.operator.OcpStrimziOperator;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mongo")
@Tag("openshift")
@Tag("mongo-sharded")
@Fixture(OcpClient.class)
@Fixture(OcpStrimziOperator.class)
@Fixture(OcpKafka.class)
@Fixture(OcpMongoSharded.class)
@Fixture(ShardedReplicaMongoConnector.class)
@ExtendWith(FixtureExtension.class)
public class OcpShardedReplicaMongoConnectorIT extends ShardedMongoTests {

    public OcpShardedReplicaMongoConnectorIT(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                                      KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    @Test
    public void shouldStreamInReplicaSetMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        String topic = connectorConfig.getConnectorName() + ".inventory.customers";
        assertions.assertTopicsExist(
                connectorConfig.getConnectorName() + ".inventory.customers");

        insertCustomer(dbController, "Eve", "Sharded", "eshard@test.com", 1007);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "eshard@test.com"));
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 7));

        insertProduct(dbController, "replicaset product", "demonstrates that replicaset connector mode works", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorConfig.getConnectorName() + ".inventory.products", "replicaset product"));

        addAndRemoveShardTest(dbController, connectorConfig.getConnectorName());

        insertCustomer(dbController, "Eric", "Eh", "ee@test.com", 1008);
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ee@test.com"));
    }
}
