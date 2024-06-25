package io.debezium.connector.postgresql;

import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import static org.assertj.core.api.Assertions.assertThat;

public class YBVerifyRecord extends VerifyRecord {
  public static void hasValidKey(SourceRecord record, String pkField, int pk) {
    Struct key = (Struct) record.key();
    assertThat(key.getStruct(pkField).get("value")).isEqualTo(pk);
  }

  public static void isValidRead(SourceRecord record, String pkField, int pk) {
    hasValidKey(record, pkField, pk);
    isValidRead(record);
  }

  public static void isValidInsert(SourceRecord record, String pkField, int pk) {
    hasValidKey(record, pkField, pk);
    isValidInsert(record, true);
  }

  public static void isValidUpdate(SourceRecord record, String pkField, int pk) {
    hasValidKey(record, pkField, pk);
    isValidUpdate(record, true);
  }
}
