/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.math.BigDecimal;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.util.PGtokenizer;

final class PostgresMoney {

    private PostgresMoney() {
    }

    static BigDecimal parse(String value) {
        if (value == null) {
            return null;
        }
        try {
            return new BigDecimal(removeCurrencySymbol(value));
        }
        catch (final Exception e) {
            throw new ConnectException("Failed to parse money value: " + value, e);
        }
    }

    /**
     * Remove any parentheses, currency symbol, and thousands separators.
     */
    private static String removeCurrencySymbol(String currency) {
        final boolean negative = currency.charAt(0) == '(' || currency.charAt(0) == '-';

        String value = currency.charAt(0) == '-'
                ? "(" + currency.substring(1) + ")"
                : currency;

        value = PGtokenizer.removePara(value);

        if (!value.isEmpty() && !Character.isDigit(value.charAt(0)) && value.charAt(0) != '.') {
            value = value.substring(1);
        }

        value = value.replace(",", "");

        return negative ? "-" + value : value;
    }
}
