/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.Test;

import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;
import io.debezium.util.Testing;

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

    @Test
    public void getChangeStreamTxnIdx_SupportsTransactionEvents() {
        // npx mongodb-resumetoken-decoder yields txnOpIndex for resumeTokens.
        // you can also test with ResumeTokenDecoder.java

        Map<String, Integer> testCases = Map.ofEntries(
                Map.entry("8266561E1B000000062B022C01002B026E5A100433C2A58AB57149A3871DA09C9D3A899646645F6964006466561E1B5E1565D6F9200E230004",
                        1),
                Map.entry("8265F49506000007662B022C01002B086E5A1004448A9E1A91724ECA875AAFA40773D05C46645F6964006465F49506238B3BBCC1CB20C00004",
                        4));

        for (Map.Entry<String, Integer> entry : testCases.entrySet()) {
            var doc = testChangestreamDocument(resumeTokenDocument(entry.getKey()), true);
            // add plus 1 to raw txnOpIndex for expected as it starts at 0 even for transactions
            assertThat(MongoUtil.getChangeStreamTxnIdx(doc)).isEqualTo(entry.getValue() + 1);
        }
    }

    @Test
    public void getChangeStreamTxnIdx_ReturnsZeroForNonTransactions() {
        // npx mongodb-resumetoken-decoder yields txnOpIndex for transactions
        // you can also test with ResumeTokenDecoder.java

        // non transaction resumeTokens start with 0 (same as the first event in a transaction)
        // however, we will test that getChangeStreamTxnIdx return 0 for all non transaction ChangestreamDocument
        // regardless of encoded txnOpIndex in the resumeToken
        Map<String, Integer> testCases = Map.ofEntries(
                Map.entry("8266561E1A000000062B022C0100296E5A1004479FFFE93A2F4CA0B70D131517A9885D463C5F6964003C696474657374000004",
                        0),
                Map.entry(
                        "8266561E1B000000062B022C01002B026E5A100433C2A58AB57149A3871DA09C9D3A899646645F6964006466561E1B5E1565D6F9200E230004",
                        1),
                Map.entry("8265F49506000007662B022C01002B086E5A1004448A9E1A91724ECA875AAFA40773D05C46645F6964006465F49506238B3BBCC1CB20C00004",
                        4));

        for (Map.Entry<String, Integer> entry : testCases.entrySet()) {
            var doc = testChangestreamDocument(resumeTokenDocument(entry.getKey()), false);
            assertThat(MongoUtil.getChangeStreamTxnIdx(doc)).isEqualTo(0);
        }
    }

    @Test
    public void getChangeStreamTxnIdx_ThrowsException() {
        // Testing.Print.enable();

        // npx mongodb-resumetoken-decoder yields txnOpIndex for transactions
        // you can also test with ResumeTokenDecoder.java

        // test different scenarios where MongoUtil.getChangeStreamTxnIdx should throw exceptions.
        Map<BsonDocument, String> invalidResumeTokens = Map.of(
                // throws exception with resumeTokenDocument having expected "_data" field but having unset txnOpIdx
                resumeTokenDocument("8266561E1B000000062"), "received unexpected exception",
                // throws exception with resumeTokenDocument having expected "_data" field but having invalid token
                resumeTokenDocument("8266561E1B000000062B022C"),
                "java.lang.RuntimeException: received unexpected exception 'java.lang.RuntimeException: unexpected end of input'",
                // throws exception with empty resumeTokenDocument
                new BsonDocument(), "java.lang.NullPointerException",
                // throws exception with resumeTokenDocument with unexpected field
                new BsonDocument("_foo", new BsonString("8266561E1B000000062")), "java.lang.NullPointerException",
                // throws exception with resumeTokenDocument having expected "_data" field but having invalid value type
                new BsonDocument("_data", new BsonInt64(48)), "org.bson.BsonInvalidOperationException: Value expected to be of type STRING is of unexpected type INT64");

        for (Map.Entry<BsonDocument, String> entry : invalidResumeTokens.entrySet()) {
            var invalidResumeToken = entry.getKey();
            var exceptionMessageSubstr = entry.getValue();

            var doc = testChangestreamDocument(invalidResumeToken, true);
            boolean caught = false;
            try {
                MongoUtil.getChangeStreamTxnIdx(doc);
            }
            catch (Exception e) {
                caught = true;
                Testing.print(e);
                assertThat(e).isInstanceOf(DebeziumException.class);
                assertThat(e.getMessage().contains(exceptionMessageSubstr))
                        .as(String.format("exception message '%s' does not have substr '%s'", e.getMessage(), exceptionMessageSubstr))
                        .isTrue();
            }
            finally {
                if (!caught) {
                    Assertions.fail("expected an exception to be thrown");
                }
            }
        }
    }

    private static ChangeStreamDocument<BsonDocument> testChangestreamDocument(BsonDocument resumeToken, boolean isTransactionEvent) {
        String operationTypeString = "insert";

        BsonInt64 txnNumber = null;
        BsonDocument lsid = null;
        if (isTransactionEvent) {
            txnNumber = new BsonInt64(Math.round(Math.random()));
            lsid = new BsonDocument("id", new BsonBinary(UUID.randomUUID()));
        }

        // todo add more fields as needed
        return new ChangeStreamDocument<BsonDocument>(
                operationTypeString,
                resumeToken,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                txnNumber,
                lsid,
                null,
                null);
    }

    private static BsonDocument resumeTokenDocument(String resumeToken) {
        var doc = new BsonDocument();
        return doc.append("_data", new BsonString(resumeToken));
    }

}
