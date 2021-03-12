package io.debezium.util;

import org.junit.Test;

public class TestCalculator {

    @Test
    public void old() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < 250_000; i++) {
            sum += ObjectSizeCalculator.getObjectSize(Runtime.getRuntime());
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }

    @Test
    public void newC() {
        long sum = 0;
        Stopwatch sw = Stopwatch.reusable();
        sw.start();
        for (int i = 0; i < 250_000; i++) {
            sum += NewObjectSizeCalculator.getObjectSize(Runtime.getRuntime());
        }
        sw.stop();
        System.out.println(sum + " " + sw.durations().statistics().getTotal());
    }
}
