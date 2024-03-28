/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;

/**
 * MariaDB-specific global transaction identifier tests.
 *
 * @author Chris Cranford
 */
public class MariaDbGtidSetTest {

    private static final String DOMAIN_SERVER_ID = "1-2";

    private MariaDbGtidSet gtids;

    @Test
    public void shouldParseGtid() {
        gtids = new MariaDbGtidSet(DOMAIN_SERVER_ID + "-3");
        assertThat(gtids.forStreamId(new MariaDbGtidSet.MariaDbGtidStreamId(1, 2)).hasSequence(3)).isTrue();
    }

}
