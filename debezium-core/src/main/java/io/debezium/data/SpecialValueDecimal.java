/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.data;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Extension of plain a {@link BigDecimal} type that adds support for new features
 * like special values handling - NaN, infinity;
 *
 * @author Jiri Pechanec
 *
 */
public class SpecialValueDecimal {

    /**
     * Special values for floating-point and numeric types
     */
    private static enum SpecialValue {
        NAN,
        POSITIVE_INFINITY,
        NEGATIVE_INFINITY;
    }

    public static SpecialValueDecimal ZERO = new SpecialValueDecimal(BigDecimal.ZERO);
    public static SpecialValueDecimal NOT_A_NUMBER = new SpecialValueDecimal(SpecialValue.NAN);
    public static SpecialValueDecimal POSITIVE_INF = new SpecialValueDecimal(SpecialValue.POSITIVE_INFINITY);
    public static SpecialValueDecimal NEGATIVE_INF = new SpecialValueDecimal(SpecialValue.NEGATIVE_INFINITY);

    private final BigDecimal decimalValue;
    private final SpecialValue specialValue;

    public SpecialValueDecimal(BigDecimal value) {
        this.decimalValue = value;
        this.specialValue = null;
    }

    private SpecialValueDecimal(SpecialValue specialValue) {
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
    * Factory method for creating instances from numbers in string format
    *
    * @param decimal a string containing valid decimal number
    * @return {@link SpecialValueDecimal} containing converted {@link BigDecimal}
    */
   public static SpecialValueDecimal valueOf(String decimal) {
       return new SpecialValueDecimal(new BigDecimal(decimal));
   }

   /**
    * @return value converted into double including special values
    */
   public double toDouble() {
       if (specialValue != null) {
           switch (specialValue) {
           case NAN:
               return Double.NaN;
           case POSITIVE_INFINITY:
               return Double.POSITIVE_INFINITY;
           case NEGATIVE_INFINITY:
               return Double.NEGATIVE_INFINITY;
           }
       }
       return decimalValue.doubleValue();
   }

    /**
     * Converts a value from its logical format (BigDecimal/special) to its string representation
     *
     * @param struct the strut to put data in
     * @return the encoded value
     */
    @Override
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
        SpecialValueDecimal other = (SpecialValueDecimal) obj;
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
