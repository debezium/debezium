/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.relational.ddl;

import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DataTypeBuilder {
    private StringBuilder prefix = new StringBuilder();
    private StringBuilder suffix = new StringBuilder();
    private String parameters;
    private int jdbcType = Types.NULL;
    private long length = -1;
    private int scale = -1;
    private int arrayDimsLength = 0;
    private final int[] arrayDims = new int[40];
    private static final Pattern SIGNED_UNSIGNED_ZEROFILL_PATTERN = Pattern
            .compile("(.*)\\s+(SIGNED UNSIGNED ZEROFILL|SIGNED UNSIGNED|SIGNED ZEROFILL)", Pattern.CASE_INSENSITIVE);

    public void addToName(String str) {
        if (length == -1) {
            // Length hasn't been set yet, so add to the prefix ...
            if (prefix.length() != 0) {
                prefix.append(' ');
            }
            prefix.append(str);
        }
        else {
            // Length has already been set, so add as a suffix ...
            if (suffix.length() != 0) {
                suffix.append(' ');
            }
            suffix.append(str);
        }
    }

    public DataTypeBuilder jdbcType(int jdbcType) {
        this.jdbcType = jdbcType;
        return this;
    }

    public DataTypeBuilder parameters(String parameters) {
        this.parameters = parameters;
        return this;
    }

    public DataTypeBuilder length(long length) {
        this.length = length;
        return this;
    }

    public DataTypeBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public DataTypeBuilder addArrayDimension(int dimension) {
        arrayDims[arrayDimsLength++] = dimension;
        return this;
    }

    public DataTypeBuilder reset() {
        length = -1;
        scale = -1;
        arrayDimsLength = 0;
        prefix.setLength(0);
        suffix.setLength(0);
        return this;
    }

    public DataType create() {
        StringBuilder name = new StringBuilder(this.prefix);
        StringBuilder expression = new StringBuilder(this.prefix);
        if (length != -1) {
            expression.append('(');
            expression.append(this.length);
            if (scale != -1) {
                expression.append(',');
                expression.append(this.scale);
            }
            expression.append(')');
        }
        else if (parameters != null) {
            expression.append('(');
            expression.append(parameters);
            expression.append(')');
        }
        if (arrayDimsLength != 0) {
            for (int i = 0; i != arrayDimsLength; ++i) {
                expression.append('[');
                expression.append(this.arrayDims[i]);
                expression.append(']');
            }
        }
        if (suffix.length() != 0) {
            expression.append(' ');
            expression.append(suffix);
            name.append(' ');
            name.append(suffix);
        }
        return new DataType(
                adjustSignedUnsignedZerofill(expression),
                adjustSignedUnsignedZerofill(name),
                jdbcType,
                length,
                scale,
                arrayDims,
                arrayDimsLength);
    }

    /**
     * This method will adjust the suffix names of numeric data type.
     * In connector streaming phase, the ddl parser maybe meet the invalid definition of suffix names with numeric data type,
     * will adjust to appropriate values.
     * e.g. replace "SIGNED UNSIGNED ZEROFILL" or "SIGNED ZEROFILL" to "UNSIGNED ZEROFILL", "SIGNED UNSIGNED" to "UNSIGNED"
     * and adjust to "UNSIGNED ZEROFILL" if "zerofill" appears alone.
     */
    private String adjustSignedUnsignedZerofill(StringBuilder origin) {
        Matcher matcher = SIGNED_UNSIGNED_ZEROFILL_PATTERN.matcher(origin.toString());
        if (matcher.matches()) {
            String suffix = matcher.group(2).toUpperCase();
            switch (suffix) {
                case "SIGNED UNSIGNED ZEROFILL":
                case "SIGNED ZEROFILL":
                    return matcher.replaceFirst("$1 UNSIGNED ZEROFILL");
                case "SIGNED UNSIGNED":
                    return matcher.replaceFirst("$1 UNSIGNED");
                default:
                    return origin.toString();
            }
        }
        if (origin.toString().toUpperCase().contains("ZEROFILL")
                && !origin.toString().toUpperCase().contains("UNSIGNED")) {
            return origin.toString().toUpperCase().replaceFirst("ZEROFILL", "UNSIGNED ZEROFILL");
        }
        return origin.toString();
    }
}
