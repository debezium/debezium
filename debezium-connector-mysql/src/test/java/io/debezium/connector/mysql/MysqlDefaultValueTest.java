/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;

/**
 * @author laomei
 */
public class MysqlDefaultValueTest extends AbstractMysqlDefaultValueTest {

    {
        parserProducer = (converters) -> {
            return new MySqlAntlrDdlParser(converters);
        };
    }
}
