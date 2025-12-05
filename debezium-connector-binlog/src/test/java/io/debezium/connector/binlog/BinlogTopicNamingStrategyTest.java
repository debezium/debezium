/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.config.CommonConnectorConfig.MULTI_PARTITION_MODE;
import static io.debezium.config.CommonConnectorConfig.TOPIC_PREFIX;
import static io.debezium.schema.AbstractTopicNamingStrategy.TOPIC_DELIMITER;
import static io.debezium.schema.AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX;
import static io.debezium.schema.AbstractTopicNamingStrategy.TOPIC_TRANSACTION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultRegexTopicNamingStrategy;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.DefaultUnicodeTopicNamingStrategy;
import io.debezium.schema.SchemaUnicodeTopicNamingStrategy;

public class BinlogTopicNamingStrategyTest {

    @Test
    public void testSanitizedTopicName() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", logicalName);
        final DefaultTopicNamingStrategy defaultStrategy = new DefaultTopicNamingStrategy(props);

        String dataChangeTopic = defaultStrategy.sanitizedTopicName(".");
        assertThat(dataChangeTopic).isEqualTo("_");

        dataChangeTopic = defaultStrategy.sanitizedTopicName("..");
        assertThat(dataChangeTopic).isEqualTo("__");

        String originTopicName = "test_avro_strategy.test.t_orders_all_shards_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        String expectedTopicName = "test_avro_strategy.test.t_orders_all_shards_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        dataChangeTopic = defaultStrategy.sanitizedTopicName(originTopicName);
        assertThat(dataChangeTopic.length()).isEqualTo(defaultStrategy.MAX_NAME_LENGTH);
        assertThat(dataChangeTopic).isEqualTo(expectedTopicName);
    }

    @Test
    public void testDataChangeTopic() {
        final TableId tableId = TableId.parse("test_db.dbz_4180");
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.delimiter", ".");
        props.put("topic.prefix", logicalName);
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
        props.put("topic.prefix", logicalName);
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
        props.put("topic.prefix", logicalName);
        final DefaultTopicNamingStrategy mySqlStrategy = new DefaultTopicNamingStrategy(props);
        String transactionTopic = mySqlStrategy.transactionTopic();
        String expectedTopic = "mysql-server-1." + DefaultTopicNamingStrategy.DEFAULT_TRANSACTION_TOPIC;
        assertThat(transactionTopic).isEqualTo(expectedTopic);
    }

    @Test
    public void testHeartbeatTopic() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.prefix", logicalName);
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
        props.put("topic.prefix", logicalName);

        final DefaultRegexTopicNamingStrategy byLogicalStrategy = new DefaultRegexTopicNamingStrategy(props);
        String dataChangeTopic = byLogicalStrategy.dataChangeTopic(tableId);
        assertThat(dataChangeTopic).isEqualTo("mysql-server-1.test_db.dbz_4180_all_shards");
    }

    @Test
    public void testValidateRelativeTopicNames() {
        String errorMessageSuffix = " has invalid format (only the underscore, hyphen, dot and alphanumeric characters are allowed)";
        Configuration config = Configuration.create().with(TOPIC_DELIMITER, "&").build();
        List<String> errorList = config.validate(Field.setOf(TOPIC_DELIMITER)).get(TOPIC_DELIMITER.name()).errorMessages();
        assertThat(errorList.get(0)).isEqualTo(Field.validationOutput(TOPIC_DELIMITER, "&" + errorMessageSuffix));

        config = Configuration.create().with(TOPIC_PREFIX, "server@X").build();
        errorList = config.validate(Field.setOf(TOPIC_PREFIX)).get(TOPIC_PREFIX.name()).errorMessages();
        assertThat(errorList.get(0)).isEqualTo(Field.validationOutput(TOPIC_PREFIX, "server@X" + errorMessageSuffix));

        config = Configuration.create().with(TOPIC_HEARTBEAT_PREFIX, "#heartbeat#").build();
        errorList = config.validate(Field.setOf(TOPIC_HEARTBEAT_PREFIX)).get(TOPIC_HEARTBEAT_PREFIX.name()).errorMessages();
        assertThat(errorList.get(0)).isEqualTo(Field.validationOutput(TOPIC_HEARTBEAT_PREFIX, "#heartbeat#" + errorMessageSuffix));

        config = Configuration.create().with(TOPIC_TRANSACTION, "*transaction*").build();
        errorList = config.validate(Field.setOf(TOPIC_TRANSACTION)).get(TOPIC_TRANSACTION.name()).errorMessages();
        assertThat(errorList.get(0)).isEqualTo(Field.validationOutput(TOPIC_TRANSACTION, "*transaction*" + errorMessageSuffix));
    }

    @Test
    public void testDefaultUnicodeTopicNamingStrategy() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.prefix", logicalName);
        final DefaultUnicodeTopicNamingStrategy strategy = new DefaultUnicodeTopicNamingStrategy(props);

        TableId tableId = TableId.parse("testdb.test_中文");
        String tableTopic = strategy.dataChangeTopic(tableId);
        String expectedTopic = "mysql-server-1.testdb.test_u005f_u4e2d_u6587";
        assertThat(tableTopic).isEqualTo(expectedTopic);

        tableId = TableId.parse("testdb.test_한국인");
        tableTopic = strategy.dataChangeTopic(tableId);
        expectedTopic = "mysql-server-1.testdb.test_u005f_ud55c_uad6d_uc778";
        assertThat(tableTopic).isEqualTo(expectedTopic);

        tableId = TableId.parse("testdb.test_カタカナ");
        tableTopic = strategy.dataChangeTopic(tableId);
        expectedTopic = "mysql-server-1.testdb.test_u005f_u30ab_u30bf_u30ab_u30ca";
        assertThat(tableTopic).isEqualTo(expectedTopic);

        tableId = TableId.parse("testdb.test_normal");
        tableTopic = strategy.dataChangeTopic(tableId);
        expectedTopic = "mysql-server-1.testdb.test_u005fnormal";
        assertThat(tableTopic).isEqualTo(expectedTopic);
    }

    @Test
    public void testSchemaUnicodeTopicNamingStrategy() {
        final String logicalName = "mysql-server-1";
        final Properties props = new Properties();
        props.put("topic.prefix", logicalName);
        SchemaUnicodeTopicNamingStrategy strategy = new SchemaUnicodeTopicNamingStrategy(props);

        TableId tableId = TableId.parse("testdb.dbo.test_中文", false);
        String tableTopic = strategy.dataChangeTopic(tableId);
        String expectedTopic = "mysql-server-1.dbo.test_u005f_u4e2d_u6587";
        assertThat(tableTopic).isEqualTo(expectedTopic);

        // multi partition mode
        props.put(MULTI_PARTITION_MODE, true);
        strategy = new SchemaUnicodeTopicNamingStrategy(props);
        tableTopic = strategy.dataChangeTopic(tableId);
        expectedTopic = "mysql-server-1.testdb.dbo.test_u005f_u4e2d_u6587";
        assertThat(tableTopic).isEqualTo(expectedTopic);
    }
}
