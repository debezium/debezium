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
public class MongoUtilTest {

    private ServerAddress address;
    private List<ServerAddress> addresses = new ArrayList<>();

    @Test
    public void shouldParseIPv4ServerAddressWithoutPort() {
        address = MongoUtil.parseAddress("localhost");
        assertThat(address.getHost()).isEqualTo("localhost");
        assertThat(address.getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseIPv4ServerAddressWithoPort() {
        address = MongoUtil.parseAddress("localhost:28017");
        assertThat(address.getHost()).isEqualTo("localhost");
        assertThat(address.getPort()).isEqualTo(28017);
    }

    @Test
    public void shouldParseIPv6ServerAddressWithoutPort() {
        address = MongoUtil.parseAddress("[::1/128]");
        assertThat(address.getHost()).isEqualTo("::1/128"); // removes brackets
        assertThat(address.getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseIPv6ServerAddressWithPort() {
        address = MongoUtil.parseAddress("[::1/128]:28017");
        assertThat(address.getHost()).isEqualTo("::1/128"); // removes brackets
        assertThat(address.getPort()).isEqualTo(28017);
    }

    @Test
    public void shouldParseServerAddressesWithoutPort() {
        addresses = MongoUtil.parseAddresses("host1,host2,[::1/128],host4");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("host1");
        assertThat(addresses.get(0).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(2).getHost()).isEqualTo("::1/128");
        assertThat(addresses.get(2).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseServerAddressesWithPort() {
        addresses = MongoUtil.parseAddresses("host1:2111,host2:3111,[ff02::2:ff00:0/104]:4111,host4:5111");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("host1");
        assertThat(addresses.get(0).getPort()).isEqualTo(2111);
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(3111);
        assertThat(addresses.get(2).getHost()).isEqualTo("ff02::2:ff00:0/104");
        assertThat(addresses.get(2).getPort()).isEqualTo(4111);
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(5111);
    }

    @Test
    public void shouldParseServerAddressesWithReplicaSetNameAndWithoutPort() {
        addresses = MongoUtil.parseAddresses("replicaSetName/host1,host2,[::1/128],host4");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("host1");
        assertThat(addresses.get(0).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(2).getHost()).isEqualTo("::1/128");
        assertThat(addresses.get(2).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseServerAddressesWithReplicaSetNameAndWithPort() {
        addresses = MongoUtil.parseAddresses("replicaSetName/host1:2111,host2:3111,[ff02::2:ff00:0/104]:4111,host4:5111");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("host1");
        assertThat(addresses.get(0).getPort()).isEqualTo(2111);
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(3111);
        assertThat(addresses.get(2).getHost()).isEqualTo("ff02::2:ff00:0/104");
        assertThat(addresses.get(2).getPort()).isEqualTo(4111);
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(5111);
    }

    @Test
    public void shouldParseServerIPv6AddressesWithReplicaSetNameAndWithoutPort() {
        addresses = MongoUtil.parseAddresses("replicaSetName/[::1/128],host2,[ff02::2:ff00:0/104],host4");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("::1/128");
        assertThat(addresses.get(0).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(2).getHost()).isEqualTo("ff02::2:ff00:0/104");
        assertThat(addresses.get(2).getPort()).isEqualTo(ServerAddress.defaultPort());
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseServerIPv6AddressesWithReplicaSetNameAndWithPort() {
        addresses = MongoUtil.parseAddresses("replicaSetName/[::1/128]:2111,host2:3111,[ff02::2:ff00:0/104]:4111,host4:5111");
        assertThat(addresses.size()).isEqualTo(4);
        assertThat(addresses.get(0).getHost()).isEqualTo("::1/128");
        assertThat(addresses.get(0).getPort()).isEqualTo(2111);
        assertThat(addresses.get(1).getHost()).isEqualTo("host2");
        assertThat(addresses.get(1).getPort()).isEqualTo(3111);
        assertThat(addresses.get(2).getHost()).isEqualTo("ff02::2:ff00:0/104");
        assertThat(addresses.get(2).getPort()).isEqualTo(4111);
        assertThat(addresses.get(3).getHost()).isEqualTo("host4");
        assertThat(addresses.get(3).getPort()).isEqualTo(5111);
    }

    @Test
    public void shouldNotParseServerAddressesWithReplicaSetNameAndOpenBracket() {
        addresses = MongoUtil.parseAddresses("replicaSetName/[");
        assertThat(addresses.size()).isEqualTo(0);
    }

    @Test
    public void shouldNotParseServerAddressesWithReplicaSetNameAndNoAddress() {
        addresses = MongoUtil.parseAddresses("replicaSetName/");
        assertThat(addresses.size()).isEqualTo(1);
        assertThat(addresses.get(0).getHost()).isEqualTo(ServerAddress.defaultHost());
        assertThat(addresses.get(0).getPort()).isEqualTo(ServerAddress.defaultPort());
    }

    @Test
    public void shouldParseReplicaSetName() {
        assertThat(MongoUtil.replicaSetUsedIn("rs0/")).isEqualTo("rs0");
        assertThat(MongoUtil.replicaSetUsedIn("rs0/localhost")).isEqualTo("rs0");
        assertThat(MongoUtil.replicaSetUsedIn("rs0/[::1/128]")).isEqualTo("rs0");
    }

    @Test
    public void shouldNotParseReplicaSetName() {
        assertThat(MongoUtil.replicaSetUsedIn("")).isNull();
        assertThat(MongoUtil.replicaSetUsedIn("localhost")).isNull();
        assertThat(MongoUtil.replicaSetUsedIn("[::1/128]")).isNull();
    }

}
