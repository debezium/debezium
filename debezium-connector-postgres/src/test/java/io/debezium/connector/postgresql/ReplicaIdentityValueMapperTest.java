/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;

public class ReplicaIdentityValueMapperTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        String databaseName = "";

        Map<TableId, ServerInfo.ReplicaIdentity> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), ServerInfo.ReplicaIdentity.FULL);
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"), ServerInfo.ReplicaIdentity.DEFAULT);

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getReplicaIdentityMapper());
    }

    @Test
    public void shouldSetReplicaAutoSetIndexValue() {

        String databaseName = "";

        Map<TableId, ServerInfo.ReplicaIdentity> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), ServerInfo.ReplicaIdentity.FULL);
        ServerInfo.ReplicaIdentity replicaIdentityIndex = ServerInfo.ReplicaIdentity.INDEX;
        replicaIdentityIndex.setIndexName("idx_pk");
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"),
                replicaIdentityIndex);

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:INDEX idx_pk";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getReplicaIdentityMapper());
    }
}
