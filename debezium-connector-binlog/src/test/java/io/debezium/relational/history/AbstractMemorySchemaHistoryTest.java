/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import org.apache.kafka.connect.source.SourceConnector;

/**
 * @author Randall Hauch
 */
public abstract class AbstractMemorySchemaHistoryTest<C extends SourceConnector> extends AbstractSchemaHistoryTest<C> {
    @Override
    protected SchemaHistory createHistory() {
        return new MemorySchemaHistory();
    }
}
