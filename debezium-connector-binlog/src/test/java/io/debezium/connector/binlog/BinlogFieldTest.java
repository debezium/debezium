/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Before;

import io.debezium.config.AbstractFieldTest;
import io.debezium.config.Field;

/**
 * @author Chris Cranford
 */
public abstract class BinlogFieldTest<C extends SourceConnector>
        extends AbstractFieldTest
        implements BinlogConnectorTest<C> {

    @Before
    public void before() {
        setAllConnectorFields(getAllFields());
    }

    protected abstract Field.Set getAllFields();
}
