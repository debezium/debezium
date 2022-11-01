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

import com.mongodb.ServerAddress;

/**
 * @author Randall Hauch
 *
 */
public class ReplicaSetsTest {

    private ReplicaSets sets;
    private ReplicaSet rs;

    @Test
    public void shouldHaveNoReplicaSetsInEmptyInstance() {
        assertThat(ReplicaSets.empty().replicaSetCount()).isEqualTo(0);
    }

    @Test
    public void shouldParseNullHostString() {
        assertThat(ReplicaSets.parse(null)).isEqualTo(ReplicaSets.empty());
    }

    @Test
    public void shouldParseEmptyHostString() {
        assertThat(ReplicaSets.parse("")).isEqualTo(ReplicaSets.empty());
    }

    @Test
    public void shouldParseBlankHostString() {
        assertThat(ReplicaSets.parse("   ")).isEqualTo(ReplicaSets.empty());
    }

    @Test
    public void shouldParseHostStringWithStandaloneAddress() {
        sets = ReplicaSets.parse("localhost:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("localhost:27017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isFalse();
        assertThat(rs.isStandaloneServer()).isTrue();
        assertThat(rs.replicaSetName()).isNull();
        assertThat(rs.shardName()).isNull();
        ServerAddress expected = new ServerAddress("localhost", 27017);
        assertThat(rs.addresses().size()).isEqualTo(1);
        assertThat(rs.addresses()).containsOnly(expected);
    }

    @Test
    public void shouldParseHostStringWithStandaloneAddresses() {
        sets = ReplicaSets.parse("localhost:27017,1.2.3.4:27017,localhost:28017,[fe80::601:9bff:feab:ec01]:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("1.2.3.4:27017,[fe80::601:9bff:feab:ec01]:27017,localhost:27017,localhost:28017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isFalse();
        assertThat(rs.isStandaloneServer()).isTrue();
        assertThat(rs.replicaSetName()).isNull();
        assertThat(rs.shardName()).isNull();
        ServerAddress expected1 = new ServerAddress("1.2.3.4", 27017);
        ServerAddress expected2 = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        ServerAddress expected3 = new ServerAddress("localhost", 27017);
        ServerAddress expected4 = new ServerAddress("localhost", 28017);
        assertThat(rs.addresses().size()).isEqualTo(4);
        assertThat(rs.addresses()).containsOnly(expected1, expected2, expected3, expected4);
    }

    @Test
    public void shouldParseHostStringWithAddressForOneReplicaSet() {
        sets = ReplicaSets.parse("myReplicaSet/localhost:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("myReplicaSet/localhost:27017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isNull();
        ServerAddress expected = new ServerAddress("localhost", 27017);
        assertThat(rs.addresses().size()).isEqualTo(1);
        assertThat(rs.addresses()).containsOnly(expected);
    }

    @Test
    public void shouldParseHostStringWithIpv6AddressForOneReplicaSet() {
        sets = ReplicaSets.parse("myReplicaSet/[fe80::601:9bff:feab:ec01]:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("myReplicaSet/[fe80::601:9bff:feab:ec01]:27017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isNull();
        ServerAddress expected = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        assertThat(rs.addresses().size()).isEqualTo(1);
        assertThat(rs.addresses()).containsOnly(expected);
    }

    @Test
    public void shouldParseHostStringWithAddressesForOneReplicaSet() {
        sets = ReplicaSets.parse("myReplicaSet/localhost:27017,1.2.3.4:27017,localhost:28017,[fe80::601:9bff:feab:ec01]:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("myReplicaSet/1.2.3.4:27017,[fe80::601:9bff:feab:ec01]:27017,localhost:27017,localhost:28017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isNull();
        ServerAddress expected1 = new ServerAddress("1.2.3.4", 27017);
        ServerAddress expected2 = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        ServerAddress expected3 = new ServerAddress("localhost", 27017);
        ServerAddress expected4 = new ServerAddress("localhost", 28017);
        assertThat(rs.addresses().size()).isEqualTo(4);
        assertThat(rs.addresses()).containsOnly(expected1, expected2, expected3, expected4);
    }

    @Test
    public void shouldParseHostStringWithAddressesForMultipleReplicaSet() {
        sets = ReplicaSets.parse("myReplicaSet/host1:27017,[fe80::601:9bff:feab:ec01]:27017;otherReplicaset/1.2.3.4:27017,localhost:28017");
        assertThat(sets.replicaSetCount()).isEqualTo(2);
        assertThat(sets.hosts()).isEqualTo("myReplicaSet/[fe80::601:9bff:feab:ec01]:27017,host1:27017;otherReplicaset/1.2.3.4:27017,localhost:28017");

        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isNull();
        ServerAddress expected1 = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        ServerAddress expected2 = new ServerAddress("host1", 27017);
        assertThat(rs.addresses().size()).isEqualTo(2);
        assertThat(rs.addresses()).containsOnly(expected1, expected2);

        rs = sets.all().get(1);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("otherReplicaset");
        assertThat(rs.shardName()).isNull();
        expected1 = new ServerAddress("1.2.3.4", 27017);
        expected2 = new ServerAddress("localhost", 28017);
        assertThat(rs.addresses().size()).isEqualTo(2);
        assertThat(rs.addresses()).containsOnly(expected1, expected2);
    }

    @Test
    public void shouldParseHostStringWithAddressesForOneShard() {
        sets = ReplicaSets.parse("shard1=myReplicaSet/localhost:27017,1.2.3.4:27017,localhost:28017,[fe80::601:9bff:feab:ec01]:27017");
        assertThat(sets.replicaSetCount()).isEqualTo(1);
        assertThat(sets.hosts()).isEqualTo("shard1=myReplicaSet/1.2.3.4:27017,[fe80::601:9bff:feab:ec01]:27017,localhost:27017,localhost:28017");
        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isEqualTo("shard1");
        ServerAddress expected1 = new ServerAddress("1.2.3.4", 27017);
        ServerAddress expected2 = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        ServerAddress expected3 = new ServerAddress("localhost", 27017);
        ServerAddress expected4 = new ServerAddress("localhost", 28017);
        assertThat(rs.addresses().size()).isEqualTo(4);
        assertThat(rs.addresses()).containsOnly(expected1, expected2, expected3, expected4);
    }

    @Test
    public void shouldParseHostStringWithAddressesForMultipleShard() {
        sets = ReplicaSets.parse("shard1=myReplicaSet/host1:27017,[fe80::601:9bff:feab:ec01]:27017;shard2=otherReplicaset/1.2.3.4:27017,localhost:28017");
        assertThat(sets.replicaSetCount()).isEqualTo(2);
        assertThat(sets.hosts()).isEqualTo("shard1=myReplicaSet/[fe80::601:9bff:feab:ec01]:27017,host1:27017;shard2=otherReplicaset/1.2.3.4:27017,localhost:28017");

        rs = sets.all().get(0);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("myReplicaSet");
        assertThat(rs.shardName()).isEqualTo("shard1");
        ServerAddress expected1 = new ServerAddress("[fe80::601:9bff:feab:ec01]", 27017);
        ServerAddress expected2 = new ServerAddress("host1", 27017);
        assertThat(rs.addresses().size()).isEqualTo(2);
        assertThat(rs.addresses()).containsOnly(expected1, expected2);

        rs = sets.all().get(1);
        assertThat(rs.hasReplicaSetName()).isTrue();
        assertThat(rs.isStandaloneServer()).isFalse();
        assertThat(rs.replicaSetName()).isEqualTo("otherReplicaset");
        assertThat(rs.shardName()).isEqualTo("shard2");
        expected1 = new ServerAddress("1.2.3.4", 27017);
        expected2 = new ServerAddress("localhost", 28017);
        assertThat(rs.addresses().size()).isEqualTo(2);
        assertThat(rs.addresses()).containsOnly(expected1, expected2);
    }

    @Test
    public void shouldConsiderUnchangedSameInstance() {
        sets = ReplicaSets.parse("localhost:27017");
        assertThat(sets.haveChangedSince(sets)).isFalse();
    }

    @Test
    public void shouldConsiderUnchangedSimilarReplicaSets() {
        ReplicaSets sets1 = ReplicaSets.parse("localhost:27017");
        ReplicaSets sets2 = ReplicaSets.parse("localhost:27017");
        assertThat(sets1.haveChangedSince(sets2)).isFalse();

        sets1 = ReplicaSets.parse("shard1=myReplicaSet/host1:27017,[fe80::601:9bff:feab:ec01]:27017;shard2=otherReplicaset/1.2.3.4:27017,localhost:28017");
        sets2 = ReplicaSets.parse("shard1=myReplicaSet/host1:27017,[fe80::601:9bff:feab:ec01]:27017;shard2=otherReplicaset/1.2.3.4:27017,localhost:28017");
        assertThat(sets1.haveChangedSince(sets2)).isFalse();
    }

    @Test
    public void shouldConsiderChangedReplicaSetsWithOneReplicaSetContainingDifferentLocalServers() {
        ReplicaSets sets1 = ReplicaSets.parse("localhost:27017");
        ReplicaSets sets2 = ReplicaSets.parse("localhost:27017,host2:27017");
        assertThat(sets1.haveChangedSince(sets2)).isTrue();
    }

    @Test
    public void shouldConsiderUnchangedReplicaSetsWithAdditionalServerAddressInExistingReplicaSet() {
        ReplicaSets sets1 = ReplicaSets.parse("rs1/localhost:27017");
        ReplicaSets sets2 = ReplicaSets.parse("rs1/localhost:27017,host2:27017");
        assertThat(sets1.haveChangedSince(sets2)).isFalse();
    }

    @Test
    public void shouldConsiderChangedReplicaSetsWithAdditionalReplicaSet() {
        ReplicaSets sets1 = ReplicaSets.parse("rs1/localhost:27017;rs2/host2:17017");
        ReplicaSets sets2 = ReplicaSets.parse("rs1/localhost:27017");
        assertThat(sets1.haveChangedSince(sets2)).isTrue();
    }

    @Test
    public void shouldConsiderChangedReplicaSetsWithRemovedReplicaSet() {
        ReplicaSets sets1 = ReplicaSets.parse("rs1/localhost:27017");
        ReplicaSets sets2 = ReplicaSets.parse("rs1/localhost:27017;rs2/host2:17017");
        assertThat(sets1.haveChangedSince(sets2)).isTrue();
    }

    @Test
    public void shouldNotSubdivideOneReplicaSet() {
        sets = ReplicaSets.parse("rs1/host1:27017,host2:27017");
        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(1, divided::add);
        assertThat(divided.size()).isEqualTo(1);
        assertThat(divided.get(0)).isSameAs(sets);
    }

    @Test
    public void shouldNotSubdivideMultipleReplicaSetsIntoOneGroup() {
        sets = ReplicaSets.parse("rs1/host1:27017,host2:27017;rs2/host3:27017");
        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(1, divided::add);
        assertThat(divided.size()).isEqualTo(1);
        assertThat(divided.get(0)).isSameAs(sets);
    }

    @Test
    public void shouldSubdivideMultipleReplicaSetsWithIntoMultipleGroups() {
        sets = ReplicaSets.parse("rs1/host1:27017,host2:27017;rs2/host3:27017");
        List<ReplicaSets> divided = new ArrayList<>();
        sets.subdivide(2, divided::add);
        assertThat(divided.size()).isEqualTo(2);

        ReplicaSets dividedSet1 = divided.get(0);
        assertThat(dividedSet1.replicaSetCount()).isEqualTo(1);
        assertThat(dividedSet1.all().get(0)).isSameAs(sets.all().get(0));

        ReplicaSets dividedSet2 = divided.get(1);
        assertThat(dividedSet2.replicaSetCount()).isEqualTo(1);
        assertThat(dividedSet2.all().get(0)).isSameAs(sets.all().get(1));
    }

}
