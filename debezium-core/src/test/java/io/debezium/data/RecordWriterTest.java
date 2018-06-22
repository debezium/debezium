/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import io.debezium.data.SchemaUtil.RecordWriter;
import io.debezium.doc.FixFor;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Test for {@link SchemaUtil.RecordWriter}.
 * 
 * @author Andreas Bergmeier
 * 
 */
public class RecordWriterTest {

    @Test
    @FixFor("DBZ-759")
    public void correctlySerializesByteArray() {
        final RecordWriter recordWriter = new RecordWriter();
        recordWriter.append(new byte[]{1, 3, 5, 7});
        assertThat(recordWriter.toString()).isEqualTo("\"[1, 3, 5, 7]\"");
    }

    @Test
    @FixFor("DBZ-759")
    public void correctlySerializesByteBuffer() {
        final RecordWriter recordWriter = new RecordWriter();
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(new byte[]{11, 13, 17});
        recordWriter.append(buffer);
        assertThat(recordWriter.toString()).isEqualTo("\"[11, 13, 17]\"");
    }
}
