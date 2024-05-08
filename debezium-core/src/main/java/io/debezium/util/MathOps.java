/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.Immutable;

/**
 * Utilities for performing math operations with mixed native and advanced numeric types.
 *
 * @author Randall Hauch
 */
@Immutable
public final class MathOps {

    public static Number add(Number first, Number second) {
        if (second == null) {
            return first;
        }
        else if (first == null) {
            return second;
        }
        if (first instanceof Short) {
            return add((Short) first, second);
        }
        if (first instanceof Integer) {
            return add((Integer) first, second);
        }
        if (first instanceof Long) {
            return add((Long) first, second);
        }
        if (first instanceof Float) {
            return add((Float) first, second);
        }
        if (first instanceof Double) {
            return add((Double) first, second);
        }
        if (first instanceof BigInteger) {
            return add((BigInteger) first, second);
        }
        if (first instanceof BigDecimal) {
            return add((BigDecimal) first, second);
        }
        if (first instanceof AtomicLong) {
            return add((AtomicLong) first, second);
        }
        if (first instanceof AtomicInteger) {
            return add((AtomicInteger) first, second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Short first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Short first, short second) {
        int sum = first.shortValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        return Integer.valueOf(sum);
    }

    public static Number add(Short first, int second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Short first, long second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Short first, float second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Short first, double second) {
        double sum = first.doubleValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Short first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(Short first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(Short first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(Short first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(Short first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(Short first, BigDecimal second) {
        return second.add(BigDecimal.valueOf(first.longValue()));
    }

    public static Number add(Short first, BigInteger second) {
        return second.add(BigInteger.valueOf(first.longValue()));
    }

    public static Number add(Short first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(Short first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(Integer first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Integer first, short second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Integer first, int second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Integer first, long second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Integer first, float second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Integer first, double second) {
        double sum = first.doubleValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Integer first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(Integer first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(Integer first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(Integer first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(Integer first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(Integer first, BigDecimal second) {
        return second.add(BigDecimal.valueOf(first.longValue()));
    }

    public static Number add(Integer first, BigInteger second) {
        return second.add(BigInteger.valueOf(first.longValue()));
    }

    public static Number add(Integer first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(Integer first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(Long first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Long first, short second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Long first, int second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Long first, long second) {
        long sum = first.longValue() + second;
        if (Short.MAX_VALUE >= sum && Short.MIN_VALUE <= sum) {
            return Short.valueOf((short) sum);
        }
        if (Integer.MAX_VALUE >= sum && Integer.MIN_VALUE <= sum) {
            return Integer.valueOf((int) sum);
        }
        return Long.valueOf(sum);
    }

    public static Number add(Long first, float second) {
        double sum = first.doubleValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Long first, double second) {
        double sum = first.doubleValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Long first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(Long first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(Long first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(Long first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(Long first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(Long first, BigDecimal second) {
        return second.add(BigDecimal.valueOf(first.longValue()));
    }

    public static Number add(Long first, BigInteger second) {
        return second.add(BigInteger.valueOf(first.longValue()));
    }

    public static Number add(Long first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(Long first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(Float first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Float first, short second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Float first, int second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Float first, long second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Float first, float second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Float first, double second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Float first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(Float first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(Float first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(Float first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(Float first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(Float first, BigDecimal second) {
        return second.add(BigDecimal.valueOf(first.longValue()));
    }

    public static Number add(Float first, BigInteger second) {
        return second.add(BigInteger.valueOf(first.longValue()));
    }

    public static Number add(Float first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(Float first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(Double first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(Double first, short second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Double first, int second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Double first, long second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Double first, float second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Double first, double second) {
        double sum = first.floatValue() + second;
        if (Float.MAX_VALUE >= sum && Float.MIN_VALUE <= sum) {
            return Float.valueOf((float) sum);
        }
        return Double.valueOf(sum);
    }

    public static Number add(Double first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(Double first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(Double first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(Double first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(Double first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(Double first, BigDecimal second) {
        return second.add(BigDecimal.valueOf(first.longValue()));
    }

    public static Number add(Double first, BigInteger second) {
        return second.add(BigInteger.valueOf(first.longValue()));
    }

    public static Number add(Double first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(Double first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(BigInteger first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(BigInteger first, short second) {
        return first.add(BigInteger.valueOf(second));
    }

    public static Number add(BigInteger first, int second) {
        return first.add(BigInteger.valueOf(second));
    }

    public static Number add(BigInteger first, long second) {
        return first.add(BigInteger.valueOf(second));
    }

    public static Number add(BigInteger first, float second) {
        return new BigDecimal(first).add(BigDecimal.valueOf(second));
    }

    public static Number add(BigInteger first, double second) {
        return new BigDecimal(first).add(BigDecimal.valueOf(second));
    }

    public static Number add(BigInteger first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(BigInteger first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(BigInteger first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(BigInteger first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(BigInteger first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(BigInteger first, BigDecimal second) {
        return second.add(new BigDecimal(first));
    }

    public static Number add(BigInteger first, BigInteger second) {
        return second.add(second);
    }

    public static Number add(BigInteger first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(BigInteger first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(BigDecimal first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(BigDecimal first, short second) {
        return first.add(BigDecimal.valueOf(second));
    }

    public static Number add(BigDecimal first, int second) {
        return first.add(BigDecimal.valueOf(second));
    }

    public static Number add(BigDecimal first, long second) {
        return first.add(BigDecimal.valueOf(second));
    }

    public static Number add(BigDecimal first, float second) {
        return first.add(BigDecimal.valueOf(second));
    }

    public static Number add(BigDecimal first, double second) {
        return first.add(BigDecimal.valueOf(second));
    }

    public static Number add(BigDecimal first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(BigDecimal first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(BigDecimal first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(BigDecimal first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(BigDecimal first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(BigDecimal first, BigDecimal second) {
        return second.add(first);
    }

    public static Number add(BigDecimal first, BigInteger second) {
        return second.add(second);
    }

    public static Number add(BigDecimal first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(BigDecimal first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(AtomicInteger first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(AtomicInteger first, short second) {
        return add(Integer.valueOf(first.intValue()), second);
    }

    public static Number add(AtomicInteger first, int second) {
        return add(Integer.valueOf(first.intValue()), second);
    }

    public static Number add(AtomicInteger first, long second) {
        return add(Integer.valueOf(first.intValue()), second);
    }

    public static Number add(AtomicInteger first, float second) {
        return add(Integer.valueOf(first.intValue()), second);
    }

    public static Number add(AtomicInteger first, double second) {
        return add(Integer.valueOf(first.intValue()), second);
    }

    public static Number add(AtomicInteger first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(AtomicInteger first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(AtomicInteger first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(AtomicInteger first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(AtomicInteger first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(AtomicInteger first, BigDecimal second) {
        return add(second, first);
    }

    public static Number add(AtomicInteger first, BigInteger second) {
        return add(second, first);
    }

    public static Number add(AtomicInteger first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(AtomicInteger first, AtomicLong second) {
        return add(first, second.longValue());
    }

    public static Number add(AtomicLong first, Number second) {
        if (second instanceof Short) {
            return add(first, (Short) second);
        }
        if (second instanceof Integer) {
            return add(first, (Integer) second);
        }
        if (second instanceof Long) {
            return add(first, (Long) second);
        }
        if (second instanceof Float) {
            return add(first, (Float) second);
        }
        if (second instanceof Double) {
            return add(first, (Double) second);
        }
        if (second instanceof BigInteger) {
            return add(first, (BigInteger) second);
        }
        if (second instanceof BigDecimal) {
            return add(first, (BigDecimal) second);
        }
        if (second instanceof AtomicInteger) {
            return add(first, (AtomicInteger) second);
        }
        if (second instanceof AtomicLong) {
            return add(first, (AtomicLong) second);
        }
        throw new IllegalArgumentException();
    }

    public static Number add(AtomicLong first, short second) {
        return add(Long.valueOf(first.longValue()), second);
    }

    public static Number add(AtomicLong first, int second) {
        return add(Long.valueOf(first.longValue()), second);
    }

    public static Number add(AtomicLong first, long second) {
        return add(Long.valueOf(first.longValue()), second);
    }

    public static Number add(AtomicLong first, float second) {
        return add(Long.valueOf(first.longValue()), second);
    }

    public static Number add(AtomicLong first, double second) {
        return add(Long.valueOf(first.longValue()), second);
    }

    public static Number add(AtomicLong first, Short second) {
        return add(first, second.shortValue());
    }

    public static Number add(AtomicLong first, Integer second) {
        return add(first, second.intValue());
    }

    public static Number add(AtomicLong first, Long second) {
        return add(first, second.longValue());
    }

    public static Number add(AtomicLong first, Float second) {
        return add(first, second.floatValue());
    }

    public static Number add(AtomicLong first, Double second) {
        return add(first, second.doubleValue());
    }

    public static Number add(AtomicLong first, BigDecimal second) {
        return add(second, first);
    }

    public static Number add(AtomicLong first, BigInteger second) {
        return add(second, first);
    }

    public static Number add(AtomicLong first, AtomicInteger second) {
        return add(first, second.intValue());
    }

    public static Number add(AtomicLong first, AtomicLong second) {
        return add(first, second.longValue());
    }

    private MathOps() {
    }

}
