/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.mongodb.ConnectionString;

import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.ReplicaSet;

/**
 * @author Randall Hauch
 *
 */
public class ReplicaSetsTest {

    @Test
    public void shouldHaveNoReplicaSetsInEmptyInstance() {
        assertThat(ReplicaSets.empty().size()).isEqualTo(0);
    }

    @Test
    public void shouldParseNullHostString() {
        assertThat(ConnectionStrings.parseFromHosts(null)).isEmpty();
    }

    @Test
    public void shouldParseEmptyHostString() {
        assertThat(ConnectionStrings.parseFromHosts("")).isEmpty();
    }

    @Test
    public void shouldParseBlankHostString() {
        assertThat(ConnectionStrings.parseFromHosts("")).isEmpty();
    }

    @Test
    public void shouldParseSingleHostStringWithStandaloneAddress() {
        var cs = ConnectionStrings.parseFromHosts("localhost:27017");
        assertThat(cs).hasValue("mongodb://localhost:27017/");
    }

    @Test
    public void shouldParseHostStringWithStandaloneAddresses() {
        var hosts = "localhost:27017,1.2.3.4:27017,localhost:28017,[fe80::601:9bff:feab:ec01]:27017";
        var cs = ConnectionStrings.parseFromHosts(hosts);
        assertThat(cs).hasValue("mongodb://" + hosts + "/");
    }

    @Test
    public void shouldParseHostStringWithAddressAndReplicaSet() {
        var cs = ConnectionStrings.parseFromHosts("myReplicaSet/localhost:27017");
        assertThat(cs).hasValue("mongodb://localhost:27017/?replicaSet=myReplicaSet");
    }

    @Test
    public void shouldParseHostStringWithIpv6AddressAndReplicaSet() {
        var cs = ConnectionStrings.parseFromHosts("myReplicaSet/[fe80::601:9bff:feab:ec01]:27017");
        assertThat(cs).hasValue("mongodb://[fe80::601:9bff:feab:ec01]:27017/?replicaSet=myReplicaSet");
    }

    @Test
    public void shouldParseHostStringWithAddressesAndReplicaSet() {
        var hosts = "localhost:27017,1.2.3.4:27017,localhost:28017,[fe80::601:9bff:feab:ec01]:27017";
        var cs = ConnectionStrings.parseFromHosts("myReplicaSet/" + hosts);
        assertThat(cs).hasValue("mongodb://" + hosts + "/?replicaSet=myReplicaSet");
    }

    @Test
    public void shouldHaveAttributesFromConnectionString() {
        var cs = new ConnectionString("mongodb://localhost:27017/?replicaSet=rs0");
        var rs = new ReplicaSet(cs);

        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.replicaSetName()).isEqualTo("rs0");
        assertThat(rs.connectionString()).isEqualTo(cs);
    }

    @Test
    public void shouldConsiderUnchangedSameInstance() {
        var rs = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var sets = ReplicaSets.of(rs);

        assertThat(sets.haveChangedSince(sets)).isFalse();
    }

    @Test
    public void shouldConsiderUnchangedSameReplicaSets() {
        var rs0 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var sets0 = ReplicaSets.of(rs0);
        var sets1 = ReplicaSets.of(rs1);

        assertThat(sets0.haveChangedSince(sets1)).isFalse();
    }

    @Test
    public void shouldConsiderUnchangedSameReplicaSetsWithDifferentAddresses() {
        var rs0 = new ReplicaSet("mongodb://1.2.3.4:27017,localhost:28017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var rs2 = new ReplicaSet("mongodb://localhost:28017/?replicaSet=rs2");
        var sets0 = ReplicaSets.of(rs0, rs2);
        var sets1 = ReplicaSets.of(rs1, rs2);

        assertThat(sets0.haveChangedSince(sets1)).isFalse();
    }

    @Test
    public void shouldConsiderChangedDifferentReplicaSetsWithSameAddresses() {
        var rs0 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs1");
        var sets0 = ReplicaSets.of(rs0);
        var sets1 = ReplicaSets.of(rs1);

        assertThat(sets0.haveChangedSince(sets1)).isTrue();
    }

    @Test
    public void shouldConsiderChangedReplicaSetsWithAdditionalReplicaSet() {
        var rs0 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var sets0 = ReplicaSets.of(rs0);
        var sets1 = ReplicaSets.of(rs0, rs1);

        assertThat(sets1.haveChangedSince(sets0)).isTrue();
    }

    @Test
    public void shouldConsiderChangedReplicaSetsWithRemovedReplicaSet() {
        var rs0 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://localhost:27017/?replicaSet=rs0");
        var sets0 = ReplicaSets.of(rs0, rs1);
        var sets1 = ReplicaSets.of(rs0);

        assertThat(sets1.haveChangedSince(sets0)).isTrue();
    }

    @Test
    public void shouldNotSubdivideOneReplicaSet() {
        var rs0 = new ReplicaSet("mongodb://host0:27017,host1:27017/?replicaSet=rs0");
        var sets = ReplicaSets.of(rs0);

        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(1, divided::add);
        assertThat(divided.size()).isEqualTo(1);
        assertThat(divided.get(0)).isEqualTo(sets);
    }

    @Test
    public void shouldNotSubdivideMultipleReplicaSetsIntoOneGroup() {
        var rs0 = new ReplicaSet("mongodb://host0:27017,host1:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://host2:27017/?replicaSet=rs01");
        var sets = ReplicaSets.of(rs0);

        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(1, divided::add);
        assertThat(divided.size()).isEqualTo(1);
        assertThat(divided.get(0)).isEqualTo(sets);

    }

    @Test
    public void shouldSubdivideMultipleReplicaSetsWithIntoMultipleGroups() {
        var rs0 = new ReplicaSet("mongodb://host0:27017,host1:27017/?replicaSet=rs0");
        var rs1 = new ReplicaSet("mongodb://host2:27017/?replicaSet=rs01");
        var sets = ReplicaSets.of(rs0, rs1);

        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(2, divided::add);
        assertThat(divided.size()).isEqualTo(2);
        assertThat(divided.get(0).all()).containsExactly(sets.all().get(0));
        assertThat(divided.get(1).all()).containsExactly(sets.all().get(1));
    }

}
