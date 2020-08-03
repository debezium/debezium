package io.debezium.jdbc;

import io.debezium.data.Bits;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Xml;
import io.debezium.relational.Column;
import io.debezium.time.*;
import org.apache.kafka.connect.data.Field;
import org.slf4j.LoggerFactory;
import java.sql.Types;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.apache.kafka.connect.data.SchemaBuilder;

public enum ProxyConverter {

    NULL(new int[]{Types.NULL}, (column, conf) -> { LoggerFactory.getLogger(ProxyConverter.class.getName()).warn("Unexpected JDBC type: NULL");return null; }, null),
    BIT(new int[]{Types.BIT}, (column, conf) -> column.length() > 1 ? Bits.builder(column.length()) : null, (column, field) -> (data, conf) -> ConverterHelper.convertBits(column, field, conf.byteOrder)),
    BOOL(new int[]{Types.BOOLEAN}, (column, conf) -> SchemaBuilder.bool(), (column, field) -> (conf, obj) -> ConverterHelper.convertBoolean(column, field, obj)),
    BINARY(new int[]{Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY}, (column, conf) -> conf.binaryMode.getSchema(), (column, field) -> (data, conf) -> ConverterHelper.convertBinary(column, field, data, conf.binaryMode)),
    TINYINT(new int[]{Types.TINYINT}, (column, conf) -> SchemaBuilder.int8(), (column, field) -> (data, conf) -> ConverterHelper.convertTinyInt(column, field, data)),
    SMALLINT(new int[]{Types.TINYINT}, (column, conf) -> SchemaBuilder.int16(), (column, field) -> (data, conf) -> ConverterHelper.convertSmallInt(column, field, data)),
    INTEGER(new int[]{Types.INTEGER}, (column, conf) -> SchemaBuilder.int32(), (column, field) -> (data, conf) -> ConverterHelper.convertInteger(column, field, data)),
    BIGINT(new int[]{Types.BIGINT}, (column, conf) -> SchemaBuilder.int64(), (column, field) -> (data, conf) -> ConverterHelper.convertBigInt(column, field, data)),
    REAL(new int[]{Types.REAL}, (column, conf) -> SchemaBuilder.float32(), (column, field) -> (data, conf) -> ConverterHelper.convertReal(column, field, data)),
    DOUBLE(new int[]{Types.DOUBLE}, (column, conf) -> SchemaBuilder.float64(), (column, field) -> (data, conf) -> ConverterHelper.convertDouble(column, field, data)),
    FLOAT(new int[]{Types.FLOAT}, (column, conf) -> SchemaBuilder.float64(), (column, field) -> (data, conf) -> ConverterHelper.convertFloat(column, field, data)),
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    NUMERIC(new int[]{Types.NUMERIC}, (column, conf) -> SpecialValueDecimal.builder(conf.decimalMode, column.length(), column.scale().get()), (column, field) -> (data, conf) -> ConverterHelper.convertNumeric(column, field, data, conf.decimalMode)),
    DECIMAL(new int[]{Types.DECIMAL}, (column, conf) -> SpecialValueDecimal.builder(conf.decimalMode, column.length(), column.scale().get()), (column, field) -> (data, conf) -> ConverterHelper.convertDecimal(column, field, data, conf.decimalMode)),
    STRING(new int[]{Types.CHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.NCLOB, Types.VARCHAR, Types.LONGVARCHAR, Types.CLOB, Types.DATALINK},
            (column, conf) -> SchemaBuilder.string(), (column, field) -> (data, conf) -> ConverterHelper.convertString(column, field, data)),
    XML(new int[]{Types.SQLXML}, (column, conf) -> Xml.builder(), (column, field) -> (data, conf) -> ConverterHelper.convertString(column, field, data)), // does convertString have to handle XMLtype then; just split it here in stead?
    DATE(new int[]{Types.DATE},  ColumnBuilderHelper::dateBuilder, (column, field) -> (data, conf) -> ConverterHelper.convertDate(column, field, conf, data)),
    TIME(new int[]{Types.TIME},  ColumnBuilderHelper::timeBuilder, (column, field) -> (data, conf) -> ConverterHelper.convertTime(column, field, conf, data)),
    TIMESTAMP(new int[]{Types.TIMESTAMP},  ColumnBuilderHelper::timestampBuilder, (column, field) -> (data, conf) -> ConverterHelper.convertTimestamp(column,field,conf,data)),
    TIME_WITH_TIMEZONE(new int[]{Types.TIME_WITH_TIMEZONE}, (column, conf) -> ZonedTime.builder(), (column, field) -> (data, conf) -> ConverterHelper.convertTimeWithZone(column,field,conf,data)),
    TIMESTAMP_WITH_TIMEZONE(new int[]{Types.TIMESTAMP_WITH_TIMEZONE}, (column, conf) -> ZonedTimestamp.builder(), (column, field) -> (data, conf) -> ConverterHelper.convertTimestampWithZone(column,field,conf,data)),
    ROWID(new int[]{Types.ROWID}, (column, conf) -> SchemaBuilder.bytes(), (column, field) -> (data, conf) -> ConverterHelper.convertRowId(column, field, data)),
    DEFAULT(new int[]{Types.DISTINCT, Types.ARRAY, Types.JAVA_OBJECT, Types.OTHER, Types.REF, Types.REF_CURSOR, Types.STRUCT}, null, null)
    ;


    int[] type;
    BiFunction<Column, JdbcValueConverters.ValueConverterConfiguration, SchemaBuilder> schemaBuilder;
    BiFunction<Column, Field, BiFunction<Object, JdbcValueConverters.ValueConverterConfiguration, Object>> converter;


    ProxyConverter(int[] type,
                   BiFunction<Column, JdbcValueConverters.ValueConverterConfiguration, SchemaBuilder> schemaBuilder,
                   BiFunction<Column, Field, BiFunction<Object, JdbcValueConverters.ValueConverterConfiguration, Object>> converter) {
        this.type = type;
        this.schemaBuilder = schemaBuilder;
        this.converter = converter;
    }

    /**
     * TODO
     * time stuff should be handled by a common final static class. for the scope, or the application... I guess it's per DB-properties file
     * I don't have the insight to handle instantiation as is
     */



    public static ProxyConverter fromJdbcType(int jdbcType) {
        for (ProxyConverter pc : ProxyConverter.values() ) {
            for (int type : pc.type) {
                if (type == jdbcType) return pc;
            }
        }
        return DEFAULT;
    }



}