/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.TransactionCommitConsumer.LobFragment;
import static io.debezium.connector.oracle.logminer.TransactionCommitConsumer.LobUnderConstruction;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.Random;

import org.junit.Test;

import oracle.sql.RAW;

public class LobUnderConstructionTest {
    @Test
    public void shouldBuildCorrectLobFromInitialValue() {
        LobUnderConstruction lob = LobUnderConstruction.fromInitialValue(null);
        assertThat(lob.isNull).isTrue();
        assertThat(lob.fragments).isEmpty();
        assertThat(lob.start).isEqualTo(0);
        assertThat(lob.end).isEqualTo(0);
        lob = LobUnderConstruction.fromInitialValue("EMPTY_BLOB()");
        assertThat(lob.isNull).isFalse();
        assertThat(lob.binary).isTrue();
        assertThat(lob.fragments).isEmpty();
        assertThat(lob.start).isEqualTo(0);
        assertThat(lob.end).isEqualTo(0);
        lob = LobUnderConstruction.fromInitialValue("EMPTY_CLOB()");
        assertThat(lob.isNull).isFalse();
        assertThat(lob.binary).isFalse();
        assertThat(lob.fragments).isEmpty();
        assertThat(lob.start).isEqualTo(0);
        assertThat(lob.end).isEqualTo(0);
        lob = LobUnderConstruction.fromInitialValue("0123456789");
        assertThat(lob.isNull).isFalse();
        assertThat(lob.binary).isFalse();
        assertThat(lob.start).isEqualTo(0);
        assertThat(lob.end).isEqualTo(10);
        lob = LobUnderConstruction.fromInitialValue("HEXTORAW('0123456789')");
        assertThat(lob.isNull).isFalse();
        assertThat(lob.binary).isTrue();
        assertThat(lob.start).isEqualTo(0);
        assertThat(lob.end).isEqualTo(5);
        LobUnderConstruction lob2 = LobUnderConstruction.fromInitialValue(lob);
        assertThat(lob2 == lob).isTrue();
    }

    @Test
    public void shouldMergeCorrectly() throws SQLException {
        // 0 fragments
        LobUnderConstruction lob = LobUnderConstruction.fromInitialValue(null);
        assertThat(lob.merge()).isEqualTo(null);

        lob = LobUnderConstruction.fromInitialValue("EMPTY_BLOB()");
        assertThat(lob.merge()).isEqualTo("EMPTY_BLOB()");

        lob = LobUnderConstruction.fromInitialValue("EMPTY_CLOB()");
        assertThat(lob.merge()).isEqualTo("EMPTY_CLOB()");

        // 1 fragment
        lob = LobUnderConstruction.fromInitialValue("0123456789");
        assertThat(lob.merge()).isEqualTo("0123456789");

        lob = LobUnderConstruction.fromInitialValue("EMPTY_CLOB()");
        lob.add(new LobFragment("0123456789"));
        assertThat(lob.merge()).isEqualTo("0123456789");

        lob = LobUnderConstruction.fromInitialValue("HEXTORAW('0123456789')");
        assertThat(lob.merge()).isInstanceOf(byte[].class);
        assertThat((byte[]) lob.merge()).isEqualTo(RAW.hexString2Bytes("0123456789"));

        lob = LobUnderConstruction.fromInitialValue("EMPTY_BLOB()");
        lob.add(new LobFragment("HEXTORAW('0123456789')"));
        assertThat(lob.merge()).isInstanceOf(byte[].class);
        assertThat((byte[]) lob.merge()).isEqualTo(RAW.hexString2Bytes("0123456789"));

        // multiple fragments with holes
        lob = LobUnderConstruction.fromInitialValue("0123456789");
        LobFragment frag = new LobFragment("9876543210");
        frag.offset = 11; // a gap of size one
        lob.add(frag);
        assertThat(lob.merge()).isEqualTo("0123456789 9876543210");

        lob = LobUnderConstruction.fromInitialValue("HEXTORAW('0123456789')");
        frag = new LobFragment("HEXTORAW('9876543210')");
        frag.offset = 6; // a gap of size one
        lob.add(frag);
        assertThat((byte[]) lob.merge()).isEqualTo(RAW.hexString2Bytes("0123456789009876543210"));
    }

    private String randomString(int length) {
        int lower = 'a';
        int upper = 'z';
        return new Random().ints(lower, upper + 1)
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private String encodeBytes(byte[] bytes, int offset, int length) {
        StringBuilder builder = new StringBuilder("HEXTORAW('");
        for (int i = offset; i < offset + length; i++) {
            builder.append(String.format("%02x", bytes[i]));
        }
        builder.append("')");
        return builder.toString();
    }

    @Test
    public void shouldHandleNonLinearInsertionOrderWithOverwritesForBlob() {
        byte[] contents = new byte[1024];
        new Random().nextBytes(contents);

        // back to front
        LobUnderConstruction lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 16; i++) {
            int offset = 1024 - 16 * i - 16;
            LobFragment frag = new LobFragment(encodeBytes(contents, offset, 16));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // front-back-front-back
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 32; i++) {
            int frontOffset = 16 * i;
            LobFragment frontFrag = new LobFragment(encodeBytes(contents, frontOffset, 16));
            frontFrag.offset = frontOffset;
            int backOffset = 1024 - 16 * i - 16;
            LobFragment backFrag = new LobFragment(encodeBytes(contents, backOffset, 16));
            backFrag.offset = backOffset;
            lob.add(frontFrag);
            lob.add(backFrag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // make gaps, fill gaps
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 32; i++) {
            int offset = 32 * i; // even slots
            LobFragment frag = new LobFragment(encodeBytes(contents, offset, 16));
            frag.offset = offset;
            lob.add(frag);
        }
        for (int i = 0; i < 1024 / 32; i++) {
            int offset = 32 * i + 16; // odd slots
            LobFragment frag = new LobFragment(encodeBytes(contents, offset, 16));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // all overlapping writes
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 16 - 1; i++) {
            int offset = 16 * i;
            LobFragment frag = new LobFragment(encodeBytes(contents, offset, 32));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // overwrite one large fragment back to front
        byte[] otherContents = new byte[1024];
        new Random().nextBytes(otherContents);
        lob = LobUnderConstruction.fromInitialValue(encodeBytes(otherContents, 0, otherContents.length));
        for (int i = 0; i < 1024 / 16; i++) {
            int offset = 1024 - 16 * i - 16;
            LobFragment frag = new LobFragment(encodeBytes(contents, offset, 16));
            frag.offset = offset;
            lob.add(frag);
            // the number of fragments should never be greater than 10, because after every 10
            // off-sequence insertions we coalesce the fragments to improve performance
            assertThat(lob.fragments.size()).isLessThan(11);
        }
        assertThat(lob.merge()).isEqualTo(contents);
    }

    @Test
    public void shouldHandleNonLinearInsertionOrderWithOverwritesForClob() {
        String contents = randomString(1024);

        // back to front
        LobUnderConstruction lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 16; i++) {
            int offset = 1024 - 16 * i - 16;
            LobFragment frag = new LobFragment(contents.substring(offset, offset + 16));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // front-back-front-back
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 32; i++) {
            int frontOffset = 16 * i;
            LobFragment frontFrag = new LobFragment(contents.substring(frontOffset, frontOffset + 16));
            frontFrag.offset = frontOffset;
            int backOffset = 1024 - 16 * i - 16;
            LobFragment backFrag = new LobFragment(contents.substring(backOffset, backOffset + 16));
            backFrag.offset = backOffset;
            lob.add(frontFrag);
            lob.add(backFrag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // make gaps, fill gaps
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 32; i++) {
            int offset = 32 * i; // even slots
            LobFragment frag = new LobFragment(contents.substring(offset, offset + 16));
            frag.offset = offset;
            lob.add(frag);
        }
        for (int i = 0; i < 1024 / 32; i++) {
            int offset = 32 * i + 16; // odd slots
            LobFragment frag = new LobFragment(contents.substring(offset, offset + 16));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // all overlapping writes
        lob = new LobUnderConstruction();
        for (int i = 0; i < 1024 / 16 - 1; i++) {
            int offset = 16 * i;
            LobFragment frag = new LobFragment(contents.substring(offset, offset + 32));
            frag.offset = offset;
            lob.add(frag);
        }
        assertThat(lob.merge()).isEqualTo(contents);

        // overwrite one large fragment back to front
        lob = LobUnderConstruction.fromInitialValue(randomString(1024));
        for (int i = 0; i < 1024 / 16; i++) {
            int offset = 1024 - 16 * i - 16;
            LobFragment frag = new LobFragment(contents.substring(offset, offset + 16));
            frag.offset = offset;
            lob.add(frag);
            // the number of fragments should never be greater than 10, because after every 10
            // off-sequence insertions we coalesce the fragments to improve performance
            assertThat(lob.fragments.size()).isLessThan(11);
        }
        assertThat(lob.merge()).isEqualTo(contents);
    }
}
