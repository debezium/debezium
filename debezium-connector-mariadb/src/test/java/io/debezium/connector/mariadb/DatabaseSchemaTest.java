/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Set;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogDatabaseSchemaTest;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;
import io.debezium.connector.mariadb.util.MariaDbValueConvertersFactory;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.IoUtil;

/**
 * @author Chris Cranford
 */
public class DatabaseSchemaTest extends BinlogDatabaseSchemaTest<MariaDbConnectorConfig, MariaDbDatabaseSchema, MariaDbPartition, MariaDbOffsetContext> {

    private static final String baseStatements = IoUtil.readClassPathResource("ddl/mysql-products.ddl");

    public DatabaseSchemaTest() {
        super(baseStatements + """
                # creating a table without primary key which is valid for MariaDB
                CREATE TABLE connector_test.orders (
                  order_number INTEGER NOT NULL,
                  order_date DATE NOT NULL,
                  purchaser INTEGER NOT NULL,
                  quantity INTEGER NOT NULL,
                  product_id INTEGER NOT NULL,
                  FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
                  FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
                ) AUTO_INCREMENT = 10001;

                ALTER TABLE connector_test.customers ADD PRIMARY KEY IF NOT EXISTS (id);
                ALTER TABLE connector_test.orders ADD PRIMARY KEY IF NOT EXISTS (order_number);
                """);
    }

    @Override
    protected MariaDbConnectorConfig getConnectorConfig(Configuration config) {
        config = config.edit().with(AbstractSchemaHistory.INTERNAL_PREFER_DDL, true).build();
        return new MariaDbConnectorConfig(config);
    }

    @Override
    protected MariaDbDatabaseSchema getSchema(Configuration config) {
        this.connectorConfig = getConnectorConfig(config);
        return new MariaDbDatabaseSchema(
                connectorConfig,
                new MariaDbValueConvertersFactory().create(
                        DecimalHandlingMode.PRECISE,
                        TemporalPrecisionMode.ADAPTIVE,
                        BinlogConnectorConfig.BigIntUnsignedHandlingMode.LONG,
                        CommonConnectorConfig.BinaryHandlingMode.BYTES,
                        MariaDbValueConverters::adjustTemporal,
                        CommonConnectorConfig.EventConvertingFailureHandlingMode.WARN),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.create(),
                false, new CustomConverterRegistry(Collections.emptyList()));
    }

    @Override
    protected MariaDbPartition initializePartition(MariaDbConnectorConfig connectorConfig, Configuration taskConfig) {
        Set<MariaDbPartition> partitions = new MariaDbPartition.Provider(connectorConfig, taskConfig).getPartitions();
        assertThat(partitions).hasSize(1);
        return partitions.iterator().next();
    }

    @Override
    protected MariaDbOffsetContext initializeOffset(MariaDbConnectorConfig connectorConfig) {
        return MariaDbOffsetContext.initial(connectorConfig);
    }
}
