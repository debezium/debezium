/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.postgres;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;

/**
 * Simple JMH benchmark of Type metadata used in Debezium Postgres connector.
 *
 * @author vjuranek
 */
@State(Scope.Benchmark)
public class PostgresTypeMetadataPerf {

    private static final int OP_COUNT = 10;
    private static final int MOD_COUNT = 10;
    private static final String[] MODIFIERS = {
            "text",
            "character varying(255)",
            "numeric(12,3)",
            "geometry(MultiPolygon,4326)",
            "timestamp (12) with time zone",
            "int[]",
            "myschema.geometry",
            "float[10]",
            "date",
            "bytea"
    };

    private ReplicationMessage.Column[] columns = new ReplicationMessage.Column[OP_COUNT];

    private ReplicationMessage.Column createColumn(int modifierIndex) {
        String columnName = "test";
        PostgresType columnType = PostgresType.UNKNOWN;
        String typeWithModifiers = MODIFIERS[modifierIndex];
        boolean optional = true;
        return new AbstractReplicationMessageColumn(columnName, columnType, typeWithModifiers, optional) {
            @Override
            public Object getValue(PostgresStreamingChangeEventSource.PgConnectionSupplier connection,
                                   boolean includeUnknownDatatypes) {
                return null;
            }
        };
    }

    @Setup(Level.Invocation)
    public void setup() {
        Random random = new Random(1234);
        for (int i = 0; i < OP_COUNT; i++) {
            columns[i] = createColumn(random.nextInt(MOD_COUNT));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @OperationsPerInvocation(OP_COUNT)
    public void columnMetadata(Blackhole bh) {
        for (int i = 0; i < OP_COUNT; i++) {
            bh.consume(columns[i].getTypeMetadata());
        }
    }
}
