/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.connector.oracle.parser;

import java.util.Random;
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

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.parser.DmlParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * A benchmark that measures the performance of parsing {@code UPDATE} statements by the
 * LogMiner DML parser when column values are large character payloads, such as standard
 * {@code VARCHAR2} columns or 32K extended strings.
 *
 * Two value shapes are measured for each value size:
 * <ul>
 *     <li>plain values that contain no single quotes</li>
 *     <li>values that contain escaped single quotes ({@code ''}) at regular intervals</li>
 * </ul>
 *
 * The SQL text is generated from a fixed random seed so that runs before and after parser
 * changes operate on byte-identical statements.
 *
 * @author Chris Cranford
 */
public class LogMinerDmlParserUpdatePerf {

    @State(Scope.Thread)
    public static class ParserState {

        private static final int QUOTE_INTERVAL = 64;
        private static final long SEED = 42L;
        private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 abcdefghijklmnopqrstuvwxyz";

        public DmlParser dmlParser;
        public Table table;
        public String plainValuesUpdateDml;
        public String escapedQuotesUpdateDml;

        @Param({ "2", "10", "50" })
        public int columnCount;

        @Param({ "50", "4000", "32000" })
        public int valueLength;

        @Setup(Level.Trial)
        public void doSetup() {
            dmlParser = new LogMinerDmlParser(new OracleConnectorConfig(Configuration.empty()));
            table = createTable();
            plainValuesUpdateDml = updateStatement(false);
            escapedQuotesUpdateDml = updateStatement(true);
        }

        private Table createTable() {
            TableEditor editor = Table.editor()
                    .tableId(TableId.parse("DEBEZIUM.TEST"))
                    .addColumn(Column.editor().name("ID").create());

            for (int i = 0; i < columnCount; ++i) {
                editor.addColumn(Column.editor().name("COL" + i).create());
            }

            return editor.create();
        }

        private String updateStatement(boolean withEscapedQuotes) {
            final Random random = new Random(SEED);
            final StringBuilder sb = new StringBuilder("update \"DEBEZIUM\".\"TEST\" set \"ID\" = '1'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(", \"COL").append(i).append("\" = '").append(getColumnValue(random, withEscapedQuotes)).append("'");
            }
            sb.append(" where \"ID\" = '1'");
            for (int i = 0; i < columnCount; ++i) {
                sb.append(" and \"COL").append(i).append("\" = '").append(getColumnValue(random, withEscapedQuotes)).append("'");
            }
            return sb.append(";").toString();
        }

        private String getColumnValue(Random random, boolean withEscapedQuotes) {
            final StringBuilder sb = new StringBuilder(valueLength + 16);
            for (int i = 0; i < valueLength; ++i) {
                if (withEscapedQuotes && i > 0 && i % QUOTE_INTERVAL == 0) {
                    // an escaped single quote, contributing one literal quote character to the value
                    sb.append("''");
                }
                else {
                    sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
                }
            }
            return sb.toString();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public LogMinerDmlEntry testUpdateWithPlainValues(ParserState state) {
        return state.dmlParser.parse(state.plainValuesUpdateDml, state.table);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Fork(value = 1)
    @Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public LogMinerDmlEntry testUpdateWithEscapedQuoteValues(ParserState state) {
        return state.dmlParser.parse(state.escapedQuotesUpdateDml, state.table);
    }
}
