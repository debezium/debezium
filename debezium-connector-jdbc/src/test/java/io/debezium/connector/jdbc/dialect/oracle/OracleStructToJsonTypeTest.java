/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.engine.jdbc.connections.internal.UserSuppliedConnectionProviderImpl;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.connect.ConnectStructToConnectStringType;
import io.debezium.doc.FixFor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Verifies that {@link OracleDatabaseDialect} routes {@code STRUCT} schema types to the native Oracle
 * {@code JSON} column type on Oracle 21c and later, and to the string/CLOB fallback on Oracle 19 and
 * earlier. The dialect version is forced through Hibernate, so no database connection is required.
 *
 * @author minleejae
 */
class OracleStructToJsonTypeTest {

    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("sku", Schema.STRING_SCHEMA)
            .field("quantity", Schema.INT32_SCHEMA)
            .build();

    @Test
    @FixFor("debezium/dbz#2573")
    void shouldMapStructToNativeJsonOnOracle21OrLater() {
        final JdbcType type = resolveStructType(21);
        assertThat(type).isSameAs(OracleStructToJsonType.INSTANCE);
        assertThat(type.getTypeName(STRUCT_SCHEMA, false)).isEqualTo("json");
        // The serialized value is bound exactly as the string fallback binds it.
        assertThat(type.bind(1, STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA).put("sku", "A").put("quantity", 1)))
                .extracting(ValueBindDescriptor::getValue)
                .containsExactly("{\"sku\":\"A\",\"quantity\":1}");
    }

    @Test
    @FixFor("debezium/dbz#2573")
    void shouldMapStructToStringFallbackBeforeOracle21() {
        final JdbcType type = resolveStructType(19);
        assertThat(type).isSameAs(ConnectStructToConnectStringType.INSTANCE);
        assertThat(type.getTypeName(STRUCT_SCHEMA, false)).isNotEqualTo("json");
    }

    private static JdbcType resolveStructType(int majorVersion) {
        StandardServiceRegistry registry = null;
        SessionFactory sessionFactory = null;
        try {
            // Type resolution needs real Hibernate metadata, but no database connection; forcing the
            // OracleDialect version drives the version-conditional type registration.
            registry = new StandardServiceRegistryBuilder()
                    .applySetting(AvailableSettings.DIALECT, new OracleDialect(DatabaseVersion.make(majorVersion)))
                    .applySetting(AvailableSettings.ALLOW_METADATA_ON_BOOT, false)
                    .applySetting(AvailableSettings.CONNECTION_PROVIDER, UserSuppliedConnectionProviderImpl.class.getName())
                    .build();
            sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();
            final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig(Map.of(
                    "connection.url", "jdbc:oracle:thin:@localhost:1521/unused",
                    "connection.username", "unused"));
            final DatabaseDialect dialect = new OracleDatabaseDialect.OracleDatabaseDialectProvider()
                    .instantiate(config, sessionFactory);
            return dialect.getSchemaType(STRUCT_SCHEMA);
        }
        finally {
            if (sessionFactory != null) {
                sessionFactory.close();
            }
            if (registry != null) {
                StandardServiceRegistryBuilder.destroy(registry);
            }
        }
    }
}
