/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.oracle.parser;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import io.debezium.connector.oracle.logminer.parser.DmlParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;

/**
 * A basic test to determine the performance of the new LogMiner DML parser for Oracle.
 *
 * @author Chris Cranford
 */
public class LogMinerDmlParserPerf {

    @State(Scope.Thread)
    public static class ParserState {
        public DmlParser dmlParser;
        public String insertDml;
        public String updateDml;
        public String deleteDml;
        public String txId;

        @Param({ "1", "2", "5", "10", "20", "50" })
        public int columnCount;

        @Setup(Level.Trial)
        public void doSetup() {
            dmlParser = new LogMinerDmlParser();
            this.insertDml = insertStatement();
            this.updateDml = updateStatement();
            this.deleteDml = deleteStatement();
            this.txId = "1234567890";
        }

        private String insertStatement() {
            final StringBuilder sb = new StringBuilder("insert into \"DEBEZIUM\".\"TEST\"(\"ID\"");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(",").append("\"C").append(i).append("\"");
            }
            sb.append(") values (");
            sb.append("'").append(1).append("'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(",").append("'V").append(i).append("'");
            }
            return sb.append(");").toString();
        }

        private String updateStatement() {
            final StringBuilder sb = new StringBuilder("update \"DEBEZIUM\".\"TEST\" set \"ID\" = '1'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(", \"C").append(i).append("\" = 'VAL").append(i).append("'");
            }
            sb.append(" where \"ID\" = '1'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(" and \"C").append(i).append("\" = 'V").append(i).append("'");
            }
            return sb.append(";").toString();
        }

        private String deleteStatement() {
            final StringBuilder sb = new StringBuilder("delete from \"DEBEZIUM\".\"TEST\" where \"ID\" = '1'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(" and \"C").append(i).append("\" = 'V").append(i).append("'");
            }
            return sb.append(";").toString();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testInserts(ParserState state) {
        state.dmlParser.parse(state.insertDml, null, state.txId);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testUpdates(ParserState state) {
        state.dmlParser.parse(state.deleteDml, null, state.txId);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testDeletes(ParserState state) {
        state.dmlParser.parse(state.deleteDml, null, state.txId);
    }
}
