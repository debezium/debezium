package io.debezium.outbox.quarkus;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class DebeziumCustomCodec implements MessageCodec<ExportedEvent<?, ?>, ExportedEvent<?, ?>> {
    private final String name;

    public DebeziumCustomCodec() {
        this.name = "DebeziumCustomCodec";
    }

    public DebeziumCustomCodec(String name) {
        this.name = name;
    }

    @Override
    public void encodeToWire(Buffer buffer, ExportedEvent<?, ?> exportedEvent) {
        throw new UnsupportedOperationException("LocalEventBusCodec cannot only be used for local delivery");
    }

    @Override
    public ExportedEvent<?, ?> decodeFromWire(int i, Buffer buffer) {
        throw new UnsupportedOperationException("LocalEventBusCodec cannot only be used for local delivery");
    }

    @Override
    public ExportedEvent<?, ?> transform(ExportedEvent<?, ?> exportedEvent) {
        return exportedEvent;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

    // @Override
    // public void encodeToWire(Buffer buffer, CustomMessage customMessage) {
    // // Easiest ways is using JSON object
    // JsonObject jsonToEncode = new JsonObject();
    // jsonToEncode.put("statusCode", customMessage.getStatusCode());
    // jsonToEncode.put("resultCode", customMessage.getResultCode());
    // jsonToEncode.put("summary", customMessage.getSummary());
    //
    // // Encode object to string
    // String jsonToStr = jsonToEncode.encode();
    //
    // // Length of JSON: is NOT characters count
    // int length = jsonToStr.getBytes().length;
    //
    // // Write data into given buffer
    // buffer.appendInt(length);
    // buffer.appendString(jsonToStr);
    // }
    //
    // @Override
    // public CustomMessage decodeFromWire(int position, Buffer buffer) {
    // // My custom message starting from this *position* of buffer
    // int _pos = position;
    //
    // // Length of JSON
    // int length = buffer.getInt(_pos);
    //
    // // Get JSON string by it`s length
    // // Jump 4 because getInt() == 4 bytes
    // String jsonStr = buffer.getString(_pos+=4, _pos+=length);
    // JsonObject contentJson = new JsonObject(jsonStr);
    //
    // // Get fields
    // int statusCode = contentJson.getInteger("statusCode");
    // String resultCode = contentJson.getString("resultCode");
    // String summary = contentJson.getString("summary");
    //
    // // We can finally create custom message object
    // return new CustomMessage(statusCode, resultCode, summary);
    // }
    //
    // @Override
    // public CustomMessage transform(CustomMessage customMessage) {
    // // If a message is sent *locally* across the event bus.
    // // This example sends message just as is
    // return customMessage;
    // }
    //
    // @Override
    // public String name() {
    // // Each codec must have a unique name.
    // // This is used to identify a codec when sending a message and for unregistering codecs.
    // return this.getClass().getSimpleName();
    // }
    //
    // @Override
    // public byte systemCodecID() {
    // // Always -1
    // return -1;
    // }
}
