/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;
import io.debezium.util.CounterIdBuilder;
import io.debezium.util.NoOpOrderedIdBuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jiri Pechanec
 *
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
    public void beforeEach() {
        source = new SourceInfo("serverX", "databaseX", new NoOpOrderedIdBuilder());
        source.update(1L, new TableId("catalogNameX", "schemaNameX", "tableNameX"));
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.source().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.source().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void idsGetIncrementedAsExpected() {
        source = new SourceInfo("serverX", "databaseX", new CounterIdBuilder());
        TableId t = new TableId("c", "s", "t");
        source.update(1L, 1L, 1L, t, 1L);
        source.offset();
        assertThat(source.offset().get(AbstractSourceInfo.ORDER_ID_KEY)).isEqualTo("1");
        assertThat(source.source().get(AbstractSourceInfo.ORDER_ID_KEY)).isEqualTo("1");
        source.update(2L, t);
        assertThat(source.offset().get(AbstractSourceInfo.ORDER_ID_KEY)).isEqualTo("2");
        assertThat(source.source().get(AbstractSourceInfo.ORDER_ID_KEY)).isEqualTo("2");
    }
}
