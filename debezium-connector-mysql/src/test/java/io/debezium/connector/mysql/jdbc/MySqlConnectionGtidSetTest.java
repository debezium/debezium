/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.mysql.gtid.MySqlGtidSet;
import io.debezium.doc.FixFor;

/**
 * Tests the GTID set adjustment that keeps a lineage missing from the offset from being replayed.
 *
 * @author Ricky Makhija
 */
public class MySqlConnectionGtidSetTest {

    /** A lineage inherited from a previous primary and no longer written to. */
    private static final String INHERITED = "2c9a83f5-1d7b-11ee-8a61-0242ac130002";

    /** The lineage the server is currently writing. */
    private static final String OWN = "b7d41e02-4f19-11ef-9c2a-0242ac130004";

    /** A lineage that has just become active, as after a failover. */
    private static final String PROMOTED = "cc4f1b90-2ae7-11ef-9d31-0242ac130009";

    private static final MySqlGtidSet SERVER = new MySqlGtidSet(
            INHERITED + ":1-1472899466," + OWN + ":1-8913557");
    private static final MySqlGtidSet PURGED = new MySqlGtidSet(INHERITED + ":1-1457646934");

    /** Everything the server executed before the file the offset resumes from. */
    private static final String PREVIOUS_GTIDS = INHERITED + ":1-1472899466," + OWN + ":1-8900000";

    /** The merge performed by {@link MySqlConnection#filterGtidSet}. */
    private static GtidSet merge(MySqlGtidSet server, MySqlGtidSet purged, MySqlGtidSet offset, String previousGtids) {
        return server.retainAllKnownTsids(offset)
                .with(purged)
                .with(offset)
                .with(MySqlConnection.untrackedGtids(previousGtids, offset));
    }

    private static long highestGtid(GtidSet set, String uuid) {
        final MySqlGtidSet.UUIDSet uuidSet = ((MySqlGtidSet) set).forServerWithId(uuid);
        if (uuidSet == null) {
            return 0L;
        }
        long highest = 0L;
        for (MySqlGtidSet.Interval interval : uuidSet.getIntervals()) {
            highest = Math.max(highest, interval.getEnd());
        }
        return highest;
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldClaimALineageTheOffsetDoesNotTrackButTheServerExecutedEarlier() {
        // An offset that started part-way through the binlog never observed the inherited lineage.
        final MySqlGtidSet offset = new MySqlGtidSet(OWN + ":9-8912233");

        // Without the adjustment the lineage is claimed only as far as gtid_purged, so the server
        // re-delivers everything it still retains for it.
        assertThat(highestGtid(SERVER.retainAllKnownTsids(offset).with(PURGED).with(offset), INHERITED))
                .isEqualTo(1457646934L);

        // Previous_gtids shows it was executed before the resume position, so it is consumed.
        assertThat(highestGtid(merge(SERVER, PURGED, offset, PREVIOUS_GTIDS), INHERITED))
                .isEqualTo(1472899466L);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldLeaveAnOffsetThatTracksEveryLineageUnchanged() {
        final MySqlGtidSet offset = new MySqlGtidSet(
                INHERITED + ":1-1472899466," + OWN + ":1-8912233");

        final GtidSet merged = merge(SERVER, PURGED, offset, PREVIOUS_GTIDS);

        assertThat(highestGtid(merged, INHERITED)).isEqualTo(1472899466L);
        assertThat(highestGtid(merged, OWN)).isEqualTo(8912233L);
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldNotClaimALineageWithTransactionsAfterTheResumePosition() {
        // A promoted primary writing its own lineage: those transactions are not in Previous_gtids
        // of the resume file, so they must still be read from the earliest available position.
        final MySqlGtidSet server = new MySqlGtidSet(PROMOTED + ":1-5000," + OWN + ":1-8913557");
        final MySqlGtidSet offset = new MySqlGtidSet(OWN + ":1-8912233");

        final GtidSet merged = merge(server, new MySqlGtidSet(""), offset, OWN + ":1-8900000");

        assertThat(highestGtid(merged, PROMOTED)).isZero();
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldNotOverrideAPositionTheOffsetAlreadyRecords() {
        final MySqlGtidSet offset = new MySqlGtidSet(OWN + ":1-8912233");

        // Previous_gtids reports an earlier position for the tracked lineage; it must be ignored.
        assertThat(MySqlConnection.untrackedGtids(OWN + ":1-8900000", offset).isEmpty()).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2731")
    public void shouldToleratePreviousGtidsThatCannotBeUsed() {
        final MySqlGtidSet offset = new MySqlGtidSet(OWN + ":1-8912233");

        // A file with no earlier transactions reports an empty value, and a blank one would
        // otherwise fail to parse.
        assertThat(MySqlConnection.untrackedGtids("", offset).isEmpty()).isTrue();
        assertThat(MySqlConnection.untrackedGtids("   ", offset).isEmpty()).isTrue();
        assertThat(MySqlConnection.untrackedGtids(null, offset).isEmpty()).isTrue();
    }
}
