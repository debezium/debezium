package io.debezium.util;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.data.Envelope;

public class TestCalculator {

    private static final int COUNT = 100_000;

    private static final Schema recordSchema = SchemaBuilder.struct().field("id", SchemaBuilder.int8()).build();
    private static final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", SchemaBuilder.int32())
            .field("version", SchemaBuilder.string())
            .build();
    private static final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();
    private static final Struct before = new Struct(recordSchema);
    private static final Struct source = new Struct(sourceSchema);

    static {
        before.put("id", (byte) 1);
        source.put("lsn", 1234);
        source.put("version", "version!");
    }

    private static final Struct data = envelope.create(before, source, Instant.now());

    @Test
    public void oldFullStruct() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < COUNT; i++) {
            sum += ObjectSizeCalculator.getObjectSize(data);
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }

    @Test
    public void oldWithoutSchema() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < COUNT; i++) {
            sum += ObjectSizeCalculator.getObjectSize(data) - ObjectSizeCalculator.getObjectSize(data.schema());
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }

    @Test
    @Ignore
    public void newC() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < 250_000; i++) {
            sum += NewObjectSizeCalculator.getObjectSize(data);
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }

    @Test
    public void struct() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < COUNT; i++) {
            sum += ApproximateStructSizeCalculator.getStructSize(data);
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }
}
