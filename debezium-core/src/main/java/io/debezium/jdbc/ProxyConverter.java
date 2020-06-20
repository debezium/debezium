package io.debezium.jdbc;

import io.debezium.data.Bits;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Xml;
import io.debezium.relational.Column;
import io.debezium.time.*;
import org.slf4j.LoggerFactory;
import java.sql.Types;
import java.util.function.Function;

import org.apache.kafka.connect.data.SchemaBuilder;

enum ProxyConverter {

    NULL(new int[]{Types.NULL}, (column) -> { LoggerFactory.getLogger(ProxyConverter.class.getName()).warn("Unexpected JDBC type: NULL");return null; }, null),
    BIT(new int[]{Types.BIT}, column -> column.length() > 1 ? Bits.builder(column.length()) : null,(column, field, data) -> Converter.convertBits(column, field)),
    BOOL(new int[]{Types.BOOLEAN}, column -> SchemaBuilder.bool(), Converter::convertBoolean),
    BINARY(new int[]{Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY}, column -> ConverterHelper.binaryMode.getSchema(), (column, field, data) -> Converter.convertBinary(column, field, data, ConverterHelper.binaryMode)),
    TINYINT(new int[]{Types.TINYINT}, column -> SchemaBuilder.int8(), Converter::convertTinyInt),
    SMALLINT(new int[]{Types.TINYINT}, column -> SchemaBuilder.int16(), Converter::convertSmallInt),
    INTEGER(new int[]{Types.INTEGER}, column -> SchemaBuilder.int32(), Converter::convertInteger),
    BIGINT(new int[]{Types.BIGINT}, column -> SchemaBuilder.int64(), Converter::convertBigInt),
    REAL(new int[]{Types.REAL}, column -> SchemaBuilder.float32(), Converter::convertReal),
    DOUBLE(new int[]{Types.DOUBLE}, column -> SchemaBuilder.float64(), Converter:: convertDouble),
    FLOAT(new int[]{Types.FLOAT}, column -> SchemaBuilder.float64(), Converter:: convertFloat),
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    NUMERIC(new int[]{Types.NUMERIC}, column -> SpecialValueDecimal.builder(ConverterHelper.decimalMode, column.length(), column.scale().get()), Converter::convertNumeric),
    DECIMAL(new int[]{Types.DECIMAL}, column -> SpecialValueDecimal.builder(ConverterHelper.decimalMode, column.length(), column.scale().get()), Converter::convertDecimal),
    STRING(new int[]{Types.CHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.NCLOB, Types.VARCHAR, Types.LONGVARCHAR, Types.CLOB, Types.DATALINK},
            column -> SchemaBuilder.string(), Converter::convertString),
    XML(new int[]{Types.SQLXML}, column -> Xml.builder(), Converter::convertString), // does convertString have to handle XMLtype then; just split it here in stead?
    DATE(new int[]{Types.DATE},  ConverterHelper::dateBuilder, Converter::convertDate),
    TIME(new int[]{Types.TIME},  ConverterHelper::timeBuilder, Converter::convertTime),
    TIMESTAMP(new int[]{Types.TIMESTAMP},  ConverterHelper::timestampBuilder, Converter::convertTimestamp),
    TIME_WITH_TIMEZONE(new int[]{Types.TIME_WITH_TIMEZONE}, column -> ZonedTime.builder(), Converter::convertTimeWithZone),
    TIMESTAMP_WITH_TIMEZONE(new int[]{Types.TIMESTAMP_WITH_TIMEZONE}, column -> ZonedTimestamp.builder(), Converter::convertTimestampWithZone),
    ROWID(new int[]{Types.ROWID}, column -> SchemaBuilder.bytes(), Converter::convertRowId),
    DEFAULT(new int[]{Types.DISTINCT, Types.ARRAY, Types.JAVA_OBJECT, Types.OTHER, Types.REF, Types.REF_CURSOR, Types.STRUCT}, null, null)
    ;


    int[] type;
    Function<Column, SchemaBuilder> schemaBuilder;
    ConverterMethod converter;


    ProxyConverter(int[] type, Function<Column, SchemaBuilder> schemaBuilder, ConverterMethod converter) {
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