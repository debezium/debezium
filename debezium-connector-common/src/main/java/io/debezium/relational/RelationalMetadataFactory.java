/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import io.debezium.common.annotation.Incubating;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Creates the objects that describe a connector's relational schema.
 * <p>
 * The default implementation, {@link DefaultRelationalMetadataFactory}, produces the standard
 * {@link ColumnEditorImpl} and {@link TableEditorImpl}. A connector that needs to carry additional
 * metadata on its columns or tables supplies its own factory through
 * {@link RelationalDatabaseConnectorConfig#getRelationalMetadataFactory()} and returns editors that
 * create its own {@link ColumnImpl} and {@link TableImpl} subclasses. Every path in the shared code that
 * produces a column or table that ends up in a connector's {@link Tables} obtains its editor from this factory;
 * the static {@link Column#editor()} and {@link Table#editor()} remain for callers that have no
 * connector context.
 * <p>
 * Connectors that keep the default factory still build columns and tables through the static editors in their
 * own code, such as their DDL parser listeners and their {@link io.debezium.jdbc.JdbcConnection} subclass. That is
 * correct for them, but a connector that adopts its own factory must convert every such site in its module to
 * this factory, or those columns and tables are created without its metadata.
 *
 * @author Chris Cranford
 */
@Incubating
public interface RelationalMetadataFactory {

    /**
     * @return a fresh column editor; never {@code null}
     */
    ColumnEditor columnEditor();

    /**
     * @return a fresh table editor; never {@code null}
     */
    TableEditor tableEditor();

    /**
     * The serializer that schema history storage uses to write table definitions as JSON and to rebuild
     * them on recovery. The default writes the base fields only and rebuilds tables through this factory's
     * editors; a connector whose columns or tables carry extra metadata returns a subclass that overrides
     * the serializer's extension hooks, or that metadata is not persisted.
     *
     * @return a serializer bound to this factory; never {@code null}
     */
    default JsonTableChangeSerializer jsonTableChangesSerializer() {
        return new JsonTableChangeSerializer(this);
    }

    /**
     * The serializer that writes table definitions as Kafka Connect structs for the schema change topic.
     * A connector that adds fields to the struct returns a subclass built with a {@code SchemaFactory}
     * whose column and table schemas declare them.
     *
     * @param adjuster the schema name adjuster; may not be null
     * @return a serializer; never {@code null}
     */
    default ConnectTableChangeSerializer connectTableChangesSerializer(SchemaNameAdjuster adjuster) {
        return new ConnectTableChangeSerializer(adjuster);
    }
}
