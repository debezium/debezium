/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.jdbc.connections.internal.UserSuppliedConnectionProviderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.postgres.PostgresDatabaseDialect;
import io.debezium.connector.jdbc.type.connect.ConnectStructToConnectStringType;
import io.debezium.connector.jdbc.type.debezium.VariableScaleDecimalType;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.data.vector.SparseDoubleVector;
import io.debezium.doc.FixFor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

@Tag("UnitTests")
class GeneralDatabaseDialectTest {

    private StandardServiceRegistry registry;
    private SessionFactory sessionFactory;
    private JdbcSinkConnectorConfig config;
    private GeneralDatabaseDialect dialect;

    @BeforeEach
    void beforeEach() {
        // Type resolution needs real Hibernate metadata, but no database connection.
        registry = new StandardServiceRegistryBuilder()
                .applySetting(AvailableSettings.DIALECT, PostgreSQLDialect.class.getName())
                .applySetting(AvailableSettings.ALLOW_METADATA_ON_BOOT, false)
                .applySetting(AvailableSettings.CONNECTION_PROVIDER, UserSuppliedConnectionProviderImpl.class.getName())
                .build();
        sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();
        config = new JdbcSinkConnectorConfig(Map.of(
                "connection.url", "jdbc:postgresql://localhost/unused",
                "connection.username", "unused"));
        dialect = new GeneralDatabaseDialect(config, sessionFactory);
    }

    @AfterEach
    void afterEach() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
        StandardServiceRegistryBuilder.destroy(registry);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "com.example.Order")
    @FixFor("debezium/dbz#2573")
    void shouldSerializeAnonymousAndNamedStructs(String schemaName) {
        final var builder = SchemaBuilder.struct().field("sku", Schema.STRING_SCHEMA);
        if (schemaName != null) {
            builder.name(schemaName);
        }
        final var schema = builder.build();
        final var type = dialect.getSchemaType(schema);

        assertThat(type).isSameAs(ConnectStructToConnectStringType.INSTANCE);
        assertThat(type.bind(1, schema, new Struct(schema).put("sku", "A")))
                .extracting(ValueBindDescriptor::getValue)
                .containsExactly("{\"sku\":\"A\"}");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2573")
    void shouldRejectUnsupportedSparseVector(boolean propagateColumnType) {
        final var builder = SparseDoubleVector.builder();
        if (propagateColumnType) {
            builder.parameter("__debezium.source.column.type", "sparsevec");
        }
        assertThatThrownBy(() -> dialect.getSchemaType(builder.build()))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Dialect does not support schema type " + SparseDoubleVector.LOGICAL_NAME)
                .hasMessageContaining("VectorToJsonConverter");
    }

    @Test
    @FixFor("debezium/dbz#2573")
    void shouldKeepDedicatedLogicalTypeMappings() {
        assertThat(dialect.getSchemaType(VariableScaleDecimal.schema()))
                .isSameAs(VariableScaleDecimalType.INSTANCE);

        final var postgres = new PostgresDatabaseDialect.PostgresDatabaseDialectProvider().instantiate(config, sessionFactory);
        final var schema = SparseDoubleVector.schema();
        assertThat(postgres.getSchemaType(schema).getTypeName(schema, false)).isEqualTo("sparsevec");
    }

    @Test
    @FixFor("debezium/dbz#2573")
    void shouldKeepUnsupportedDenseVectorErrors() {
        for (Schema schema : List.of(FloatVector.schema(), DoubleVector.schema())) {
            assertThatThrownBy(() -> dialect.getSchemaType(schema))
                    .isInstanceOf(ConnectException.class)
                    .hasMessageContaining("Dialect does not support schema type " + schema.name());
        }
    }
}
