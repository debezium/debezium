/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.cube;

import org.jboss.arquillian.core.spi.LoadableExtension;

public class Extension implements LoadableExtension {

    @Override
    public void register(final ExtensionBuilder builder) {
        builder.observer(MySQLDatabaseCubeProducer.class);
    }

}
