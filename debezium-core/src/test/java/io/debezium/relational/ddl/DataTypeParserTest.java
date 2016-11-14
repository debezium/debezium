/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.ddl;

import java.sql.Types;

import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.text.TokenStream;

public class DataTypeParserTest {

    private DataTypeParser parser;

    @Before
    public void beforeEach() {
        parser = new DataTypeParser();
        parser.register(Types.BOOLEAN, "BOOL");
        parser.register(Types.BOOLEAN, "BOOLEAN");
        parser.register(Types.BOOLEAN, "BIT[(1)]");
        parser.register(Types.BOOLEAN, "TINYINT(1)");
        parser.register(Types.BIT, "BIT(M)");
        parser.register(Types.INTEGER, "TINYINT[(M)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.INTEGER, "SMALLINT[(M)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.INTEGER, "MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.INTEGER, "INT[(M)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.INTEGER, "INTEGER[(M)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.BIGINT, "BIGINT[(M)] [UNSIGNED] [ZEROFILL]");
        
        parser.register(Types.DECIMAL, "DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DECIMAL, "DEC[(M[,D])] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DECIMAL, "NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DECIMAL, "FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]");

        parser.register(Types.FLOAT, "FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DOUBLE, "DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DOUBLE, "DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL]");
        parser.register(Types.DOUBLE, "REAL[(M,D)] [UNSIGNED] [ZEROFILL]");

        parser.register(Types.CHAR, "ENUM(...) [CHARSET charset_name] [COLLATE collation_name]");

        parser.register(Types.VARCHAR, "VARCHAR");
    }

    @Test
    public void shouldDetermineBooleanTypes() {
        assertType("BIT(1)","BIT",Types.BOOLEAN, 1);
        assertType("BIT","BIT",Types.BOOLEAN);
        assertType("BOOLEAN","BOOLEAN",Types.BOOLEAN);
        assertType("BOOL","BOOL",Types.BOOLEAN);
        assertType("TINYINT(1)","TINYINT",Types.BOOLEAN, 1);
        assertType("BIT(-3)","BIT",Types.BOOLEAN);  // matches "BIT" thru "BIT[(1)]", leaving "(-3)" on stream
        assertNoType("BOOLE");
    }
    
    @Test
    public void shouldDetermineBitTypes() {
        assertType("BIT(2)","BIT",Types.BIT, 2);
        assertType("BIT(3)","BIT",Types.BIT, 3);
    }
    
    @Test
    public void shouldDetermineIntegerTypes() {
        assertType("TINYINT","TINYINT",Types.INTEGER);
        assertType("TINYINT(2)","TINYINT",Types.INTEGER, 2);
        assertType("TINYINT(10)","TINYINT",Types.INTEGER, 10);
        
        assertType("SMALLINT","SMALLINT",Types.INTEGER);
        assertType("SMALLINT(2)","SMALLINT",Types.INTEGER, 2);
        assertType("SMALLINT(10)","SMALLINT",Types.INTEGER, 10);
        
        assertType("MEDIUMINT","MEDIUMINT",Types.INTEGER);
        assertType("MEDIUMINT(2)","MEDIUMINT",Types.INTEGER, 2);
        assertType("MEDIUMINT(10)","MEDIUMINT",Types.INTEGER, 10);
        
        assertType("INT","INT",Types.INTEGER);
        assertType("INT(2)","INT",Types.INTEGER, 2);
        assertType("INT(10)","INT",Types.INTEGER, 10);
        
        assertType("INTEGER","INTEGER",Types.INTEGER);
        assertType("INTEGER(2)","INTEGER",Types.INTEGER, 2);
        assertType("INTEGER(10)","INTEGER",Types.INTEGER, 10);
        assertType("INTEGER(-2)","INTEGER",Types.INTEGER);  // leaves "(-2)" on stream
    }
    
    @Test
    public void shouldDetermineBitIntegerTypes() {
        assertType("BIGINT","BIGINT",Types.BIGINT);
        assertType("BIGINT(2)","BIGINT",Types.BIGINT, 2);
        assertType("BIGINT(10)","BIGINT",Types.BIGINT, 10);
    }
    
    @Test
    public void shouldDetermineDecimalTypes() {
        assertType("DECIMAL","DECIMAL",Types.DECIMAL);
        assertType("DECIMAL(2)","DECIMAL",Types.DECIMAL, 2);
        assertType("DECIMAL(10)","DECIMAL",Types.DECIMAL, 10);
        assertType("DECIMAL(2,1)","DECIMAL",Types.DECIMAL, 2, 1);
        assertType("DECIMAL(10,5)","DECIMAL",Types.DECIMAL, 10, 5);
        assertType("DECIMAL(10,5) ZEROFILL","DECIMAL ZEROFILL",Types.DECIMAL, 10, 5);
        assertType("DECIMAL(10,5) UNSIGNED ZEROFILL","DECIMAL UNSIGNED ZEROFILL",Types.DECIMAL, 10, 5);
    }
    
    @Test
    public void shouldDetermineTypeWithWildcard() {
        assertType("ENUM('a','b','c')","ENUM",Types.CHAR);
        assertEnumType("ENUM('a','multi','multi with () paren', 'other') followed by",
                       "ENUM('a','multi','multi with () paren', 'other')");
    }
    
    protected void assertType( String content, String typeName, int jdbcType ) {
        assertType(content,typeName,jdbcType,-1,-1,null);
    }
    
    protected void assertType( String content, String typeName, int jdbcType, long length ) {
        assertType(content,typeName,jdbcType,length,-1,null);
    }
    
    protected void assertType( String content, String typeName, int jdbcType, long precision, int scale ) {
        assertType(content,typeName,jdbcType,precision,scale,null);
    }
    
    protected void assertType( String content, String typeName, int jdbcType, long length, int scale, int[] arrayDims ) {
        DataType type = parser.parse(text(content), null);
        assertThat(type).isNotNull();
        assertThat(type.jdbcType()).isEqualTo(jdbcType);
        assertThat(type.name()).isEqualTo(typeName);
        assertThat(type.length()).isEqualTo(length);
        assertThat(type.scale()).isEqualTo(scale);
        assertThat(type.arrayDimensions()).isEqualTo(arrayDims);
    }
    
    protected void assertEnumType( String content, String expression ) {
        DataType type = parser.parse(text(content), null);
        assertThat(type).isNotNull();
        assertThat(type.jdbcType()).isEqualTo(Types.CHAR);
        assertThat(type.name()).isEqualTo("ENUM");
        assertThat(type.length()).isEqualTo(-1);
        assertThat(type.expression()).isEqualTo(expression);
    }
    
    protected void assertNoType( String content ) {
        DataType type = parser.parse(text(content), null);
        assertThat(type).isNull();
    }
    
    protected TokenStream text(String content) {
        return new TokenStream(content, new DdlTokenizer(true), false).start();
    }

}
