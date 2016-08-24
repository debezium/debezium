/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.ddl.DataTypeGrammarParser.DataTypePattern;
import io.debezium.text.TokenStream;

/**
 * @author Randall Hauch
 *
 */
public class DataTypeGrammarParserTest {

    private final int TYPE = 1984; // this test doesn't really care which JDBC type is used

    private DataTypeGrammarParser parser;
    private DataTypePattern pattern;
    private DataType type;

    @Before
    public void beforeEach() {
        parser = new DataTypeGrammarParser();
    }

    @Test
    public void shouldParseGrammarWithOneRequiredToken() {
        pattern = parser.parse(TYPE, "BOOLEAN");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("BOOLEAN"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BOOLEAN");
        assertThat(type.expression()).isEqualTo("BOOLEAN");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("boolean"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BOOLEAN"); // matches the grammar
        assertThat(type.expression()).isEqualTo("BOOLEAN"); // matches the grammar
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithTwoRequiredTokens() {
        pattern = parser.parse(TYPE, "UNSIGNED INTEGER");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("UNSIGNED INTEGER"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("UNSIGNED INTEGER");
        assertThat(type.expression()).isEqualTo("UNSIGNED INTEGER");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("unsigned integer"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("UNSIGNED INTEGER"); // matches the grammar
        assertThat(type.expression()).isEqualTo("UNSIGNED INTEGER"); // matches the grammar
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOptionalTokens() {
        pattern = parser.parse(TYPE, "[UNSIGNED] INTEGER");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("UNSIGNED INTEGER"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("UNSIGNED INTEGER");
        assertThat(type.expression()).isEqualTo("UNSIGNED INTEGER");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("unsigned integer"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("UNSIGNED INTEGER"); // matches the grammar
        assertThat(type.expression()).isEqualTo("UNSIGNED INTEGER"); // matches the grammar
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("INTEGER"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER");
        assertThat(type.expression()).isEqualTo("INTEGER");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("integer"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER"); // matches the grammar
        assertThat(type.expression()).isEqualTo("INTEGER"); // matches the grammar
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOptionalTokensAfterLength() {
        pattern = parser.parse(TYPE, "INTEGER[(L)] [UNSIGNED]");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("INTEGER"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER");
        assertThat(type.expression()).isEqualTo("INTEGER");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("INTEGER(3)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER");
        assertThat(type.expression()).isEqualTo("INTEGER(3)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("INTEGER UNSIGNED"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER UNSIGNED");
        assertThat(type.expression()).isEqualTo("INTEGER UNSIGNED");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("INTEGER(3) UNSIGNED"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("INTEGER UNSIGNED");
        assertThat(type.expression()).isEqualTo("INTEGER(3) UNSIGNED");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOneRequiredTokenAndLiteralLength() {
        pattern = parser.parse(TYPE, "BIT(3)");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("BIT(3)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BIT");
        assertThat(type.expression()).isEqualTo("BIT(3)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOneRequiredTokenAndSetOfLiteralLengths() {
        pattern = parser.parse(TYPE, "BIT(3 | 4 | 5)");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("BIT(3)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BIT");
        assertThat(type.expression()).isEqualTo("BIT(3)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("BIT(4)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(4);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BIT");
        assertThat(type.expression()).isEqualTo("BIT(4)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("BIT(5)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(5);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BIT");
        assertThat(type.expression()).isEqualTo("BIT(5)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("BIT(2)"));
        assertThat(type).isNull();
    }

    @Test
    public void shouldParseGrammarWithOneRequiredTokenAndLength() {
        pattern = parser.parse(TYPE, "BIT(L)");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("BIT(3)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("BIT");
        assertThat(type.expression()).isEqualTo("BIT(3)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOneRequiredTokenAndLengthAndScale() {
        pattern = parser.parse(TYPE, "NUMBER(L,S)");
        assertThat(pattern).isNotNull();
        type = pattern.match(text("NUMBER ( 3, 2)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(2);
        assertThat(type.name()).isEqualTo("NUMBER");
        assertThat(type.expression()).isEqualTo("NUMBER(3,2)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOneRequiredTokenAndLengthAndOptionalScale() {
        pattern = parser.parse(TYPE, "DECIMAL[(M[,D])]");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("DECIMAL"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("DECIMAL");
        assertThat(type.expression()).isEqualTo("DECIMAL");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("DECIMAL(5)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(5);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("DECIMAL");
        assertThat(type.expression()).isEqualTo("DECIMAL(5)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("DECIMAL(5,3)"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(5);
        assertThat(type.scale()).isEqualTo(3);
        assertThat(type.name()).isEqualTo("DECIMAL");
        assertThat(type.expression()).isEqualTo("DECIMAL(5,3)");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldNotFindMatchingDataTypeIfInvalidOrIncomplete() {
        pattern = parser.parse(TYPE, "DECIMAL(3)");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("DECIMAL(3"));
        assertThat(type).isNull();
    }

    @Test
    public void shouldParseGrammarWithVariableNames() {
        pattern = parser.parse(TYPE, "TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("TEXT(3) CHARACTER SET utf8 COLLATE utf8_bin"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(3);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("TEXT CHARACTER SET utf8 COLLATE utf8_bin");
        assertThat(type.expression()).isEqualTo("TEXT(3) CHARACTER SET utf8 COLLATE utf8_bin");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithComplexVariableNames() {
        pattern = parser.parse(TYPE, "CHAR[(L)] BINARY [CHARACTER SET charset_name] [COLLATE collation_name]");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("CHAR(60) BINARY CHARACTER SET utf8 COLLATE utf8_bin"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(60);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("CHAR BINARY CHARACTER SET utf8 COLLATE utf8_bin");
        assertThat(type.expression()).isEqualTo("CHAR(60) BINARY CHARACTER SET utf8 COLLATE utf8_bin");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
        
        type = pattern.match(text("char  (  60  )  binary  DEFAULT  ''  NOT  NULL"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(60);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("CHAR BINARY");
        assertThat(type.expression()).isEqualTo("CHAR(60) BINARY");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldParseGrammarWithOptionalTokenLists() {
        pattern = parser.parse(TYPE, "TEXT [CHARACTER SET]");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("TEXT CHARACTER SET"));
        assertThat(type.arrayDimensions()).isNull();
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("TEXT CHARACTER SET");
        assertThat(type.expression()).isEqualTo("TEXT CHARACTER SET");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    @Test
    public void shouldMatchTypeWithArrayDimensionsFollowingLengthAndScale() {
        pattern = parser.parse(TYPE, "DECIMAL[(M[,D])]{n}");
        assertThat(pattern).isNotNull();

        type = pattern.match(text("DECIMAL[3]"));
        assertThat(type.arrayDimensions()).containsOnly(3);
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.scale()).isEqualTo(-1);
        assertThat(type.name()).isEqualTo("DECIMAL");
        assertThat(type.expression()).isEqualTo("DECIMAL[3]");
        assertThat(type.jdbcType()).isEqualTo(TYPE);

        type = pattern.match(text("DECIMAL(5,1)[3][6]"));
        assertThat(type.arrayDimensions()).containsOnly(3, 6);
        assertThat(type.length()).isEqualTo(5);
        assertThat(type.scale()).isEqualTo(1);
        assertThat(type.name()).isEqualTo("DECIMAL");
        assertThat(type.expression()).isEqualTo("DECIMAL(5,1)[3][6]");
        assertThat(type.jdbcType()).isEqualTo(TYPE);
    }

    protected TokenStream text(String content) {
        return new TokenStream(content, new DdlTokenizer(true), false).start();
    }

}
