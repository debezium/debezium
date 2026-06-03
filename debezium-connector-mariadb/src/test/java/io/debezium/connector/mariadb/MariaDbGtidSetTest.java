/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.doc.FixFor;

/**
 * MariaDB-specific global transaction identifier tests.
 *
 * @author Chris Cranford
 */
public class MariaDbGtidSetTest {

    private static final String DOMAIN_SERVER_ID = "1-2";

    private MariaDbGtidSet gtids;

    @Test
    void shouldParseGtid() {
        gtids = new MariaDbGtidSet(DOMAIN_SERVER_ID + "-3");
        assertThat(gtids.forStreamId(new MariaDbGtidSet.MariaDbGtidStreamId(1, 2)).hasSequence(3)).isTrue();
    }

    @Test
    void shouldBeContainedWithinWhenSequenceIsBehindOnSameStream() {
        assertThat(new MariaDbGtidSet("0-1-3").isContainedWithin(new MariaDbGtidSet("0-1-5"))).isTrue();
        assertThat(new MariaDbGtidSet("0-1-5").isContainedWithin(new MariaDbGtidSet("0-1-5"))).isTrue();
    }

    @Test
    void shouldNotBeContainedWithinWhenSequenceIsAheadOnSameStream() {
        assertThat(new MariaDbGtidSet("0-1-5").isContainedWithin(new MariaDbGtidSet("0-1-3"))).isFalse();
    }

    /**
     * A MariaDB primary failover/switchover changes the server id that writes a domain, while the
     * domain's sequence keeps advancing monotonically. The earlier position written by the old
     * server must still be considered contained within the later position written by the new
     * server, because they belong to the same domain. Regression test for debezium#1672, where
     * recovery skipped every schema-history record after a failover and the connector failed with
     * "no changes will be captured".
     */
    @Test
    @FixFor("debezium/dbz#1672")
    void shouldBeContainedWithinAcrossServerIdChangeWithinSameDomain() {
        // history was written under server id 2; the connector offset is now under server id 1
        MariaDbGtidSet history = new MariaDbGtidSet("0-2-892554529");
        MariaDbGtidSet offset = new MariaDbGtidSet("0-1-964871206");
        assertThat(history.isContainedWithin(offset)).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#1672")
    void shouldNotBeContainedWithinWhenSequenceIsAheadAcrossServerIdChange() {
        // a position ahead of the offset must not be considered contained, even on a new server id
        MariaDbGtidSet ahead = new MariaDbGtidSet("0-1-964871206");
        MariaDbGtidSet behind = new MariaDbGtidSet("0-2-892554529");
        assertThat(ahead.isContainedWithin(behind)).isFalse();
    }

    @Test
    void shouldNotBeContainedWithinWhenDomainIsAbsentInOther() {
        // domain 0 is not present in the other set, so it cannot be contained
        assertThat(new MariaDbGtidSet("0-1-100").isContainedWithin(new MariaDbGtidSet("1-1-100"))).isFalse();
    }

    @Test
    void shouldBeContainedWithinAcrossMultipleDomainsIgnoringServerId() {
        // every domain is behind-or-equal in the other set, despite different server ids
        MariaDbGtidSet history = new MariaDbGtidSet("0-2-100,1-2-50");
        MariaDbGtidSet offset = new MariaDbGtidSet("0-1-200,1-3-80");
        assertThat(history.isContainedWithin(offset)).isTrue();
    }

    @Test
    void shouldNotBeContainedWithinWhenAnyDomainIsAhead() {
        // domain 1 is ahead in 'history' (90 > 80), so the whole set is not contained
        MariaDbGtidSet history = new MariaDbGtidSet("0-2-100,1-2-90");
        MariaDbGtidSet offset = new MariaDbGtidSet("0-1-200,1-3-80");
        assertThat(history.isContainedWithin(offset)).isFalse();
    }

    @Test
    void shouldMergeServersOfSameDomainWhenLookingUpByDomain() {
        MariaDbGtidSet gtidSet = new MariaDbGtidSet("0-1-100,0-2-200");
        MariaDbGtidSet.MariaDbStreamSet domain0 = gtidSet.forDomain(0);
        assertThat(domain0).isNotNull();
        assertThat(domain0.hasSequence(100)).isTrue();
        assertThat(domain0.hasSequence(200)).isTrue();
        assertThat(gtidSet.forDomain(9)).isNull();
    }
}
