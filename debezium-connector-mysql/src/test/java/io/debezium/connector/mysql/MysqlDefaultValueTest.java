/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.relational.ddl.SimpleDdlParserListener;

/**
 * @author laomei
 */
public class MysqlDefaultValueTest extends AbstractMysqlDefaultValueTest {

    private SimpleDdlParserListener listener;
    {
        parserProducer = (converters) -> {
            MySqlDdlParser parser = new MySqlDdlParser(false, converters);
            listener = new SimpleDdlParserListener();
            parser.addListener(listener);
            return parser;
        };
    }
}
