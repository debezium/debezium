package io.debezium.connector.postgresql;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class PostgresConnectorTest {
  PostgresConnector connector;

  @Before
  public void before() {
    connector = new PostgresConnector();
  }

  @Test
  public void testValidateUnableToConnectNoThrow() {
    Map<String, String> config = ImmutableMap.of(
        PostgresConnectorConfig.HOSTNAME.name(), "narnia",
        PostgresConnectorConfig.PORT.name(), "1234",
        PostgresConnectorConfig.DATABASE_NAME.name(), "postgres",
        PostgresConnectorConfig.USER.name(), "pikachu",
        PostgresConnectorConfig.PASSWORD.name(), "pika"
    );

    Config validated = connector.validate(config);
    for (ConfigValue value : validated.configValues()) {
      if (config.containsKey(value.name())) {
        assertThat(value.errorMessages().get(0), startsWith("Unable to connect:"));
      }
    }
  }
}
