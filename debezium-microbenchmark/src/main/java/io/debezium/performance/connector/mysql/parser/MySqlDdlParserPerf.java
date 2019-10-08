/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.mysql.parser;

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

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.AbstractDdlParser;

/**
 * A basic test to compare performance of legacy and antlr DDL parsers depending on the amount
 * of columns in the statement.
 *
 * @author Jiri Pechanec <jpechane@redhat.com>
 *
 */
public class MySqlDdlParserPerf {

    @State(Scope.Thread)
    public static class ParserState {

        public AbstractDdlParser antlrParser;
        public Tables tables;
        public String ddl;

        @Param({ "1", "2", "5", "10", "20", "50" })
        public int columnCount;

        @Setup(Level.Trial)
        public void doSetup() {
            antlrParser = new MySqlAntlrDdlParser();
            tables = new Tables();
            ddl = testStatement();
        }

        private String testStatement() {
            final StringBuilder sb = new StringBuilder("CREATE TABLE t1 (id int primary key");
            for (int i = 0; i < columnCount; i++) {
                sb.append(", v").append(i).append(" int");
            }
            final String statement = sb.append(")").toString();
            return statement;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
    public void antlr(ParserState state) {
        state.antlrParser.parse(state.ddl, state.tables);
    }
}
