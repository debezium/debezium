package io.debezium.connector.postgresql.transforms.yugabytedb;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.transforms.ExtractNewRecordState;

/**
 * Custom extractor for YugabyteDB to be used when replica identity is CHANGE; this will be used
 * to transform records from the format {@code fieldName:{value:"someValue",set:true}}
 * to {@code fieldName:"someValue"} and omit the records from the message which are not updated
 * in the given change event.
 * @param <R>
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBExtractNewRecordState<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
  private static final Logger LOGGER = LoggerFactory.getLogger(YBExtractNewRecordState.class);

  @Override
  public R apply(final R record) {
    final R ret = super.apply(record);
    if (ret == null || (ret.value() != null && !(ret.value() instanceof Struct))) {
      return ret;
    }

    Pair<Schema, Struct> p = getUpdatedValueAndSchema((Struct) ret.key());
    Schema updatedSchemaForKey = p.getFirst();
    Struct updatedValueForKey = p.getSecond();

    Schema updatedSchemaForValue = null;
    Struct updatedValueForValue = null;
    if (ret.value() != null) {
      Pair<Schema, Struct> val = getUpdatedValueAndSchema((Struct) ret.value());
      updatedSchemaForValue = val.getFirst();
      updatedValueForValue = val.getSecond();
    }

    return ret.newRecord(ret.topic(), ret.kafkaPartition(), updatedSchemaForKey, updatedValueForKey, updatedSchemaForValue, updatedValueForValue, ret.timestamp());
  }

  @Override
  public void close() {
    super.close();
  }

  private boolean isSimplifiableField(Field field) {
    if (field.schema().type() != Type.STRUCT) {
      return false;
    }

    return field.schema().fields().size() == 2
             && (Objects.equals(field.schema().fields().get(0).name(), "value")
                   && Objects.equals(field.schema().fields().get(1).name(), "set"));
  }

  private Schema makeUpdatedSchema(Schema schema, Struct value) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      if (isSimplifiableField(field)) {
        if (value.get(field.name()) != null) {
          builder.field(field.name(), field.schema().field("value").schema());
        }
      }
      else {
        builder.field(field.name(), field.schema());
      }
    }

    return builder.build();
  }

  private Pair<Schema, Struct> getUpdatedValueAndSchema(Struct obj) {
    final Struct value = obj;
    Schema updatedSchema = makeUpdatedSchema(value.schema(), value);

    LOGGER.debug("Updated schema as json: " + io.debezium.data.SchemaUtil.asString(value.schema()));

    final Struct updatedValue = new Struct(updatedSchema);

    for (Field field : value.schema().fields()) {
      if (isSimplifiableField(field)) {
        Struct fieldValue = (Struct) value.get(field);
        if (fieldValue != null) {
          updatedValue.put(field.name(), fieldValue.get("value"));
        }
      }
      else {
        updatedValue.put(field.name(), value.get(field));
      }
    }

    return new Pair<>(updatedSchema, updatedValue);
  }
}

class SchemaUtil {

  public static SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder builder) {
    builder.name(source.name());
    builder.version(source.version());
    builder.doc(source.doc());

    final Map<String, String> params = source.parameters();
    if (params != null) {
      builder.parameters(params);
    }

    return builder;
  }

}

