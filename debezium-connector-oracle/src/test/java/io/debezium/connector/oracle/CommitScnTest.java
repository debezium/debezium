/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.doc.FixFor;

/**
 * Test the basic functionality of the {@link CommitScn} model.
 *
 * @author Chris Cranford
 */
public class CommitScnTest {

    @Test
    @FixFor("DBZ-5245")
    public void shouldParseCommitScnThatIsNull() throws Exception {
        // Test null with String-based valueOf
        CommitScn commitScn = CommitScn.valueOf((String) null);
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getCommitScnForAllRedoThreads()).isEmpty();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.NULL);
        assertThat(encodedCommitScn(commitScn)).isNull();

        // Test null with Long-based valueOf
        commitScn = CommitScn.valueOf((Long) null);
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getCommitScnForAllRedoThreads()).isEmpty();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.NULL);
        assertThat(encodedCommitScn(commitScn)).isNull();
    }

    @Test
    @FixFor("DBZ-5245")
    public void shouldParseCommitScnThatIsNumeric() throws Exception {
        CommitScn commitScn = CommitScn.valueOf(12345L);
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:");
    }

    @Test
    @FixFor({ "DBZ-5245", "DBZ-5439" })
    public void shouldParseCommitScnThatIsString() throws Exception {
        // Test parsing with only SCN value in the string
        CommitScn commitScn = CommitScn.valueOf("12345");
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:");

        // Test parsing with new multi-part SCN, single value
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1");
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:");

        // Test parsing with new multi-part SCN with transaction ids, single value
        commitScn = CommitScn.valueOf("12345:1:123456789-234567890");
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getRedoThreadCommitScn(1).getTxIds()).containsOnly("123456789", "234567890");
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:123456789-234567890");

        // Test parsing with new multi-part SCN, multi value
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1,678901:1253ef.123457ee0.abcd:0:2");
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(2);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1, 2);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(2)).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(2)).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(678901L));
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:,678901:2:");

        // Test parsing with new multi-part SCN with transaction ids, multi value
        commitScn = CommitScn.valueOf("12345:1:23456-78901,678901:2:12345-67890");
        assertThat(commitScn).isNotNull();
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(2);
        assertThat(commitScn.getCommitScnForAllRedoThreads().keySet()).containsOnly(1, 2);
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForAllRedoThreads().get(2)).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(12345L));
        assertThat(commitScn.getCommitScnForRedoThread(2)).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(678901L));
        assertThat(commitScn.getRedoThreadCommitScn(1).getTxIds()).containsOnly("23456", "78901");
        assertThat(commitScn.getRedoThreadCommitScn(2).getTxIds()).containsOnly("12345", "67890");
        assertThat(encodedCommitScn(commitScn)).isEqualTo("12345:1:23456-78901,678901:2:12345-67890");
    }

    @Test
    @FixFor({ "DBZ-5245", "DBZ-5439" })
    public void shouldSetCommitScnAcrossAllRedoThreads() throws Exception {
        // Test no redo thread data
        CommitScn commitScn = CommitScn.valueOf((String) null);
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.NULL);
        assertThat(commitScn.getCommitScnForAllRedoThreads()).isEmpty();

        // Test with a single commit scn
        commitScn = CommitScn.valueOf("12345");
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(23456L));

        // Test with a single redo record
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1");
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(23456L));

        // Test with a single redo record, with transaction ids
        commitScn = CommitScn.valueOf("12345:1:12345-67890");
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(1);
        assertThat(commitScn.getRedoThreadCommitScn(1).getTxIds()).containsOnly("12345", "67890");
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(23456L));

        // Test with a multi redo record
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1,678901:1253ef.123457ee0.abcd:0:2");
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(2);
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForRedoThread(2)).isEqualTo(Scn.valueOf(23456L));

        // Test with a multi redo record, with transaction ids
        commitScn = CommitScn.valueOf("12345:1:12345-67890,678901:2:23456-78901");
        commitScn.setCommitScnOnAllThreads(Scn.valueOf(23456L));
        assertThat(commitScn.getMaxCommittedScn()).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForAllRedoThreads()).hasSize(2);
        assertThat(commitScn.getCommitScnForRedoThread(1)).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getCommitScnForRedoThread(2)).isEqualTo(Scn.valueOf(23456L));
        assertThat(commitScn.getRedoThreadCommitScn(1).getTxIds()).containsOnly("12345", "67890");
        assertThat(commitScn.getRedoThreadCommitScn(2).getTxIds()).containsOnly("23456", "78901");
    }

    @Test
    @FixFor("DBZ-5245")
    public void shouldBeComparableWithScn() throws Exception {
        // Test no redo thread data
        CommitScn commitScn = CommitScn.valueOf((String) null);
        assertThat(commitScn.compareTo(Scn.NULL)).isEqualTo(0); // equal
        assertThat(commitScn.compareTo(Scn.valueOf(12345L))).isEqualTo(-1); // less-than

        // Test with a single commit scn
        commitScn = CommitScn.valueOf("12345");
        assertThat(commitScn.compareTo(Scn.NULL)).isEqualTo(1); // greater than null
        assertThat(commitScn.compareTo(Scn.valueOf(123L))).isEqualTo(1); // greater than 123
        assertThat(commitScn.compareTo(Scn.valueOf(12345L))).isEqualTo(0); // equal
        assertThat(commitScn.compareTo(Scn.valueOf(23456L))).isEqualTo(-1); // less-than

        // Test with a single redo record
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1");
        assertThat(commitScn.compareTo(Scn.NULL)).isEqualTo(1); // greater than null
        assertThat(commitScn.compareTo(Scn.valueOf(123L))).isEqualTo(1); // greater than 123
        assertThat(commitScn.compareTo(Scn.valueOf(12345L))).isEqualTo(0); // equal
        assertThat(commitScn.compareTo(Scn.valueOf(23456L))).isEqualTo(-1); // less-than

        // Test with a multi redo record
        commitScn = CommitScn.valueOf("12345:00241f.00093ff0.0010:0:1,345678:1253ef.123457ee0.abcd:0:2");
        assertThat(commitScn.compareTo(Scn.NULL)).isEqualTo(1); // both greater than null
        assertThat(commitScn.compareTo(Scn.valueOf(123L))).isEqualTo(1); // both greater than 123
        assertThat(commitScn.compareTo(Scn.valueOf(12345L))).isEqualTo(0); // thread 1 is equal to value
        assertThat(commitScn.compareTo(Scn.valueOf(23456L))).isEqualTo(-1); // thread 1 is less than value
        assertThat(commitScn.compareTo(Scn.valueOf(456789L))).isEqualTo(-1); // both less than 456789
    }

    @Test
    public void shouldCommitAlreadyBeenHandled() throws Exception {
        CommitScn commitScn = CommitScn.valueOf("12345:1:123456789-234567890");
        LogMinerEventRow row = mock(LogMinerEventRow.class);
        when(row.getThread()).thenReturn(1);

        // Test with Scn equals to one
        when(row.getScn()).thenReturn(new Scn(BigInteger.ONE));
        assertThat(commitScn.hasEventScnBeenHandled(row)).isEqualTo(true);

        // Test with Scn equals to zero and contains transactionId
        when(row.getScn()).thenReturn(new Scn(BigInteger.ZERO));
        when(row.getTransactionId()).thenReturn("123456789");
        assertThat(commitScn.hasEventScnBeenHandled(row)).isEqualTo(true);

        // Test with Scn equals to commit Scn and contains transactionId
        when(row.getScn()).thenReturn(new Scn(BigInteger.valueOf(12345L)));
        when(row.getTransactionId()).thenReturn("234567890");
        assertThat(commitScn.hasEventScnBeenHandled(row)).isEqualTo(true);
    }

    @Test
    public void shouldNotCommitAlreadyBeenHandled() throws Exception {
        CommitScn commitScn = CommitScn.valueOf("12345:1:123456789-234567890");
        LogMinerEventRow row = mock(LogMinerEventRow.class);
        when(row.getThread()).thenReturn(1);

        // Test with Scn bigger than commit Scn
        when(row.getScn()).thenReturn(new Scn(BigInteger.valueOf(12346L)));
        assertThat(commitScn.hasEventScnBeenHandled(row)).isEqualTo(false);

        // Test with Scn equals to commit Scn and not contains transactionId
        when(row.getScn()).thenReturn(new Scn(BigInteger.valueOf(12345L)));
        when(row.getTransactionId()).thenReturn("234567891");
        assertThat(commitScn.hasEventScnBeenHandled(row)).isEqualTo(false);
    }

    private static String encodedCommitScn(CommitScn value) {
        final Map<String, Object> offsets = value.store(new HashMap<>());
        return (String) offsets.get(SourceInfo.COMMIT_SCN_KEY);
    }
}
