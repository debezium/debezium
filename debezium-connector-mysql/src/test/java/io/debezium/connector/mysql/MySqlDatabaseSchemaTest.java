/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDatabaseSchemaTest;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.connector.mysql.util.MySqlValueConvertersFactory;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.IoUtil;

/**
 * @author Randall Hauch
 */
public class MySqlDatabaseSchemaTest extends BinlogDatabaseSchemaTest<MySqlConnectorConfig, MySqlDatabaseSchema, MySqlPartition, MySqlOffsetContext> {

    private static final String ddlStatements = IoUtil.readClassPathResource("ddl/mysql-products.ddl");

    public MySqlDatabaseSchemaTest() {
        super(ddlStatements + """
                CREATE TABLE connector_test.orders (
                  order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  order_date DATE NOT NULL,
                  purchaser INTEGER NOT NULL,
                  quantity INTEGER NOT NULL,
                  product_id INTEGER NOT NULL,
                  FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
                  FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
                ) AUTO_INCREMENT = 10001;
                """);
    }

    @Override
    protected MySqlConnectorConfig getConnectorConfig(Configuration config) {
        config = config.edit().with(AbstractSchemaHistory.INTERNAL_PREFER_DDL, true).build();
        return new MySqlConnectorConfig(config);
    }

    @Override
    protected MySqlDatabaseSchema getSchema(Configuration config) {
        this.connectorConfig = getConnectorConfig(config);
        return new MySqlDatabaseSchema(
                connectorConfig,
                new MySqlValueConvertersFactory().create(
                        RelationalDatabaseConnectorConfig.DecimalHandlingMode.parse(DecimalMode.PRECISE.name()),
                        TemporalPrecisionMode.ADAPTIVE,
                        BinlogConnectorConfig.BigIntUnsignedHandlingMode.parse(BigIntUnsignedMode.LONG.name()),
                        BinaryHandlingMode.BYTES,
                        MySqlValueConverters::adjustTemporal,
                        CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.create(),
                false);
    }

    @Override
    protected MySqlPartition initializePartition(MySqlConnectorConfig connectorConfig, Configuration taskConfig) {
        Set<MySqlPartition> partitions = (new MySqlPartition.Provider(connectorConfig, taskConfig)).getPartitions();
        assertThat(partitions.size()).isEqualTo(1);
        return partitions.iterator().next();
    }

    @Override
    protected MySqlOffsetContext initializeOffset(MySqlConnectorConfig connectorConfig) {
        return MySqlOffsetContext.initial(connectorConfig);
    }
}
