/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Extension of plain a {@link BigDecimal} type that adds support for new features
 * like special values handling - NaN, infinity; 
 *
 * @author Jiri Pechanec
 *
 */
public class DebeziumDecimal {
    /**
     * Special values for floating-point and numeric types
     */
    private static enum SpecialValue {
        NaN,
        PositiveInfinity,
        NegativeInfinity;
    }

    public static final String VALUE_FIELD = "value";

    public static DebeziumDecimal ZERO = new DebeziumDecimal(BigDecimal.ZERO);
    public static DebeziumDecimal NOT_A_NUMBER = new DebeziumDecimal(SpecialValue.NaN);
    public static DebeziumDecimal POSITIVE_INF = new DebeziumDecimal(SpecialValue.PositiveInfinity);
    public static DebeziumDecimal NEGATIVE_INF = new DebeziumDecimal(SpecialValue.NegativeInfinity);

    private final BigDecimal decimalValue;
    private final SpecialValue specialValue;

    public DebeziumDecimal(BigDecimal value) {
        this.decimalValue = value;
        this.specialValue = null;
    }

    private DebeziumDecimal(SpecialValue specialValue) {
        this.specialValue = specialValue;
        this.decimalValue = null;
    }

    /**
     *
     * @return the plain decimal value if available
     */
    public Optional<BigDecimal> getDecimalValue() {
        return Optional.ofNullable(decimalValue);
    }

    /**
    *
    * @param consumer - executed when this instance contains a plain {@link Decimal}
    */
   public void forDecimalValue(Consumer<BigDecimal> consumer) {
       if (decimalValue != null) {
           consumer.accept(decimalValue);
       }
   }

   /**
    * Factory method for creating instances from numbers in string format
    *
    * @param decimal - a string containing valid decimal number
    * @return {@link DebeziumDecimal} containing converted {@link BigDecimal}
    */
   public static DebeziumDecimal valueOf(String decimal) {
       return new DebeziumDecimal(new BigDecimal(decimal));
   }

   /**
    * @return value converted into double including special values
    */
   public double toDouble() {
       if (specialValue != null) {
           switch (specialValue) {
           case NaN:
               return Double.NaN;
           case PositiveInfinity:
               return Double.POSITIVE_INFINITY;
           case NegativeInfinity:
               return Double.NEGATIVE_INFINITY;
           }
       }
       return decimalValue.doubleValue();
   }

   /**
    * Returns a {@link SchemaBuilder} for a base schema builder for Debezium Decimal types. You can use the resulting SchemaBuilder
    * to set additional schema settings such as required/optional, default value, and documentation.
    *
    * @return the schema builder
    */
   static SchemaBuilder builder() {
       return SchemaBuilder.struct()
               .field(VALUE_FIELD, Schema.BYTES_SCHEMA);
   }

   /**
     * Converts a value from its logical format (BigDecimal) to its encoded format - a struct containing
     * either the special value or the binary representation of the number
     *
     * @param struct the strut to put data in
     * @return the encoded value
     */
    Struct fromLogical(Struct struct) {
        if (decimalValue != null) {
            struct.put(VALUE_FIELD, decimalValue.unscaledValue().toByteArray());
        }
        return struct;
    }

    /**
     * Decodes a part of the encoded value
     *
     * @param value the encoded value
     * @return the decoded value
     */
    static DebeziumDecimal toLogical(Struct value, Function<BigInteger, BigDecimal> valueProducer) {
        final BigInteger unscaledValue = new BigInteger((byte[])value.getBytes(VALUE_FIELD));
        return new DebeziumDecimal(valueProducer.apply(unscaledValue));
    }

    /**
     * Converts a value from its logical format (BigDecimal/special) to its string representation
     *
     * @param struct the strut to put data in
     * @return the encoded value
     */
    public String toString() {
        return decimalValue != null ? decimalValue.toString() : specialValue.name();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((decimalValue == null) ? 0 : decimalValue.hashCode());
        result = prime * result + ((specialValue == null) ? 0 : specialValue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DebeziumDecimal other = (DebeziumDecimal) obj;
        if (decimalValue == null) {
            if (other.decimalValue != null)
                return false;
        }
        else if (!decimalValue.equals(other.decimalValue))
            return false;
        if (specialValue != other.specialValue)
            return false;
        return true;
    }
}
