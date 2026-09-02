/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.SessionFactory;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Xml;
import io.debezium.sink.column.ColumnDescriptor;


@Tag("UnitTests")
class SqlServerDatabaseDialectQueryBindingTest {

    private DatabaseDialect dialect;

    @BeforeEach
    void createDialect() {
        final SessionFactory sessionFactory = mock(SessionFactory.class);
        final SessionFactoryImplementor implementor = mock(SessionFactoryImplementor.class);
        final JdbcServices jdbcServices = mock(JdbcServices.class);
        final JdbcEnvironment jdbcEnvironment = mock(JdbcEnvironment.class);
        final IdentifierHelper identifierHelper = mock(IdentifierHelper.class);
        final TypeConfiguration typeConfiguration = mock(TypeConfiguration.class);
        final DdlTypeRegistry ddlTypeRegistry = mock(DdlTypeRegistry.class);

        when(sessionFactory.unwrap(SessionFactoryImplementor.class)).thenReturn(implementor);
        when(implementor.getJdbcServices()).thenReturn(jdbcServices);
        when(implementor.getTypeConfiguration()).thenReturn(typeConfiguration);
        // Real Hibernate dialect: the SQL Server provider and identifier/quoting logic inspect the
        // actual type, not a mock.
        when(jdbcServices.getDialect()).thenReturn(new SQLServerDialect());
        when(jdbcServices.getJdbcEnvironment()).thenReturn(jdbcEnvironment);
        when(jdbcEnvironment.getIdentifierHelper()).thenReturn(identifierHelper);
        when(typeConfiguration.getDdlTypeRegistry()).thenReturn(ddlTypeRegistry);
        // sessionFactory.openStatelessSession() is intentionally left unstubbed: the constructor's
        // best-effort database timezone lookup swallows any failure and falls back to "N/A".

        final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                JdbcSinkConnectorConfig.CONNECTION_URL, "jdbc:sqlserver://localhost:1433",
                JdbcSinkConnectorConfig.CONNECTION_USER, "sa"));

        dialect = new SqlServerDatabaseDialect.SqlServerDatabaseDialectProvider().instantiate(config, sessionFactory);
    }

    @Test
    @DisplayName("Should bind varchar field with cast to varchar")
    void shouldBindVarcharFieldWithCastToVarchar() {
        final ColumnDescriptor column = ColumnDescriptor.builder()
                .columnName("name")
                .jdbcType(Types.VARCHAR)
                .typeName("varchar")
                .build();

        final JdbcType type = dialect.getSchemaType(Schema.STRING_SCHEMA);

        assertThat(type.getQueryBinding(column, Schema.STRING_SCHEMA, "hello")).isEqualTo("cast(? as varchar(max))");
    }

    @Test
    @DisplayName("Should bind nvarchar field without cast")
    void shouldBindNvarcharFieldWithoutCast() {
        final ColumnDescriptor column = ColumnDescriptor.builder()
                .columnName("name")
                .jdbcType(Types.NVARCHAR)
                .typeName("nvarchar")
                .build();

        final JdbcType type = dialect.getSchemaType(Schema.STRING_SCHEMA);

        assertThat(type.getQueryBinding(column, Schema.STRING_SCHEMA, "hello")).isEqualTo("?");
    }

    @Test
    @DisplayName("Should bind xml field with cast to xml")
    void shouldBindXmlFieldWithCastToXml() {
        final Schema schema = Xml.schema();

        final ColumnDescriptor column = ColumnDescriptor.builder()
                .columnName("payload")
                .jdbcType(Types.SQLXML)
                .typeName("xml")
                .build();

        final JdbcType type = dialect.getSchemaType(schema);

        assertThat(type.getQueryBinding(column, schema, "<a/>")).isEqualTo("cast(? as xml)");
    }
}
