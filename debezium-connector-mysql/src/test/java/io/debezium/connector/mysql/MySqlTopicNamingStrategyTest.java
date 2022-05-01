/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultRegexTopicNamingStrategy;
import io.debezium.schema.DefaultTopicNamingStrategy;

public class MySqlTopicNamingStrategyTest {

    @Test
    public void testDataChangeTopic() {
        final TableId tableId = TableId.parse("test_db.dbz_4180");
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put(CommonConnectorConfig.LOGICAL_NAME, logicalName);
        final DefaultTopicNamingStrategy defaultStrategy = new DefaultTopicNamingStrategy(props);
        String dataChangeTopic = defaultStrategy.dataChangeTopic(tableId);
        assertThat(dataChangeTopic).isEqualTo("mysql-server-1.test_db.dbz_4180");
        String sanitizedDataChangeTopic = defaultStrategy.dataChangeTopic(TableId.parse("test_db.dbz#4180#2"));
        assertThat(sanitizedDataChangeTopic).isEqualTo("mysql-server-1.test_db.dbz_4180_2");

        props.put("topic.prefix", "my_prefix");
        defaultStrategy.configure(props);
        String prefixDataChangeTopic = defaultStrategy.dataChangeTopic(tableId);
        assertThat(prefixDataChangeTopic).isEqualTo("my_prefix.test_db.dbz_4180");

        props.put("topic.delimiter", "_");
        defaultStrategy.configure(props);
        String delimiterDataChangeTopic = defaultStrategy.dataChangeTopic(tableId);
        assertThat(delimiterDataChangeTopic).isEqualTo("my_prefix_test_db_dbz_4180");
    }

    @Test
    public void testSchemaChangeTopic() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put(CommonConnectorConfig.LOGICAL_NAME, logicalName);
        final DefaultTopicNamingStrategy defaultStrategy = new DefaultTopicNamingStrategy(props);
        String schemaChangeTopic = defaultStrategy.schemaChangeTopic();
        assertThat(schemaChangeTopic).isEqualTo("mysql-server-1");

        props.put("topic.prefix", "my_prefix");
        defaultStrategy.configure(props);
        String prefixSchemaChangeTopic = defaultStrategy.schemaChangeTopic();
        assertThat(prefixSchemaChangeTopic).isEqualTo("my_prefix");
    }

    @Test
    public void testTransactionTopic() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put(CommonConnectorConfig.LOGICAL_NAME, logicalName);
        final DefaultTopicNamingStrategy mySqlStrategy = new DefaultTopicNamingStrategy(props);
        String transactionTopic = mySqlStrategy.transactionTopic();
        String expectedTopic = "mysql-server-1." + DefaultTopicNamingStrategy.DEFAULT_TRANSACTION_TOPIC;
        assertThat(transactionTopic).isEqualTo(expectedTopic);
    }

    @Test
    public void testHeartbeatTopic() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put(CommonConnectorConfig.LOGICAL_NAME, logicalName);
        final DefaultTopicNamingStrategy mySqlStrategy = new DefaultTopicNamingStrategy(props);
        String heartbeatTopic = mySqlStrategy.heartbeatTopic();
        String expectedTopic = DefaultTopicNamingStrategy.DEFAULT_HEARTBEAT_TOPIC_PREFIX + ".mysql-server-1";
        assertThat(heartbeatTopic).isEqualTo(expectedTopic);
    }

    @Test
    public void testLogicTableTopic() {
        final TableId tableId = TableId.parse("test_db.dbz_4180_01");
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.regex.enable", "true");
        props.put("topic.regex", "(.*)(dbz_4180|test)(.*)");
        props.put("topic.replacement", "$1$2_all_shards");
        props.put(CommonConnectorConfig.LOGICAL_NAME, logicalName);

        final DefaultRegexTopicNamingStrategy byLogicalStrategy = new DefaultRegexTopicNamingStrategy(props);
        String dataChangeTopic = byLogicalStrategy.dataChangeTopic(tableId);
        assertThat(dataChangeTopic).isEqualTo("mysql-server-1.test_db.dbz_4180_all_shards");
    }
}
