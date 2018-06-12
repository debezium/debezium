/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;

/**
 * @author Jiri Pechanec <jpechane@redhat.com>
 */
public class MysqlAntlrDefaultValueTest extends AbstractMysqlDefaultValueTest {

    {
        parserProducer = MySqlAntlrDdlParser::new;
    }
}
