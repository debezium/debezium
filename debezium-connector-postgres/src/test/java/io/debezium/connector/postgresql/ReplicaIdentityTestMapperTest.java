/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.ReplicaIdentityInfo;
import io.debezium.relational.TableId;

public class ReplicaIdentityTestMapperTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        TableId tableId1 = new TableId("", "testSchema_1", "testTable_1");
        TableId tableId2 = new TableId("", "testSchema_2", "testTable_2");

        String replicaAutosetTypeField = "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(replicaAutosetTypeField);

        assertEquals(replicaIdentityMapper.findReplicaIdentity(tableId1).get().toString(), ReplicaIdentityInfo.ReplicaIdentity.FULL.toString());
        assertEquals(replicaIdentityMapper.findReplicaIdentity(tableId2).get().toString(), ReplicaIdentityInfo.ReplicaIdentity.DEFAULT.toString());
    }

    @Test
    public void shouldSetReplicaAutoSetIndexValue() {

        TableId tableId1 = new TableId("", "testSchema_1", "testTable_1");
        TableId tableId2 = new TableId("", "testSchema_2", "testTable_2");

        String replicaAutosetTypeField = "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:INDEX idx_pk";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(replicaAutosetTypeField);

        assertEquals(replicaIdentityMapper.findReplicaIdentity(tableId1).get().toString(), ReplicaIdentityInfo.ReplicaIdentity.FULL.toString());

        ReplicaIdentityInfo replicaIdentityIndex = new ReplicaIdentityInfo(ReplicaIdentityInfo.ReplicaIdentity.INDEX, "idx_pk");
        assertEquals(replicaIdentityMapper.findReplicaIdentity(tableId2).get().toString(), replicaIdentityIndex.toString());
    }
}
