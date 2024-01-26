/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;

public class BeanAwareSnapshotter implements BeanRegistryAware {
    protected BeanRegistry beanRegistry;

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    protected Class<? extends AbstractConnectorConnection> getConnectionClass(MySqlConnectorConfig config) {
        // TODO review this when MariaDB becomes a first class connector
        if (config.getConnectorAdapter() instanceof MariaDbConnectorAdapter) {
            return MariaDbConnection.class;
        }
        else {
            return MySqlConnection.class;
        }
    }
}
