/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import io.debezium.util.HexConverter;

/**
 * Utilities for working with MongoDB <a href="https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-stream-resume">resume tokens</a>.
 * <p>
 * Adaptation of <a href="https://github.com/mongodb/mongo-kafka/blob/master/src/main/java/com/mongodb/kafka/connect/util/ResumeTokenUtils.java">ResumeTokenUtils</a>
 */
public final class ResumeTokens {

    public static BsonTimestamp getTimestamp(BsonDocument resumeToken) {
        BsonValue data = getData(resumeToken);
        byte[] dataBytes = getDataBytes(data);
        ByteBuffer dataBuffer = ByteBuffer.wrap(dataBytes).order(ByteOrder.BIG_ENDIAN);

        // Cast to an int then remove the sign bit to get the unsigned value
        int canonicalType = ((int) dataBuffer.get()) & 0xff;
        if (canonicalType != 130) {
            throw new IllegalArgumentException("Expected canonical type equal to 130, but found " + canonicalType);
        }

        long timestampAsLong = dataBuffer.asLongBuffer().get();
        return new BsonTimestamp(timestampAsLong);
    }

    public static String toBase64(BsonDocument resumeToken) {
        var out = new BasicOutputBuffer();
        var writer = new BsonBinaryWriter(out);
        var codec = new BsonDocumentCodec();
        var context = EncoderContext.builder().build();

        codec.encode(writer, resumeToken, context);
        return Base64.getEncoder().encodeToString(out.toByteArray());
    }

    public static BsonDocument fromBase64(String data) {
        var bytes = Base64.getDecoder().decode(data);
        var reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        var codec = new BsonDocumentCodec();
        var context = DecoderContext.builder().build();

        return codec.decode(reader, context);
    }

    public static BsonValue getData(BsonDocument resumeToken) {
        if (!resumeToken.containsKey("_data")) {
            throw new IllegalArgumentException("Expected _data field in resume token");
        }

        return resumeToken.get("_data");
    }

    public static BsonDocument fromData(String data) {
        return (data == null) ? null : new BsonDocument("_data", new BsonString(data));
    }

    private static byte[] getDataBytes(BsonValue data) {
        // From: https://www.mongodb.com/docs/v4.2/changeStreams/#resume-tokens :
        // > MongoDB Version Feature Compatibility Version Resume Token _data Type
        // > MongoDB 4.2 and later “4.2” or “4.0” Hex-encoded string (v1)
        // > MongoDB 4.0.7 and later “4.0” or “3.6” Hex-encoded string (v1)
        // > MongoDB 4.0.6 and earlier “4.0” Hex-encoded string (v0)
        // > MongoDB 4.0.6 and earlier “3.6” BinData
        // > MongoDB 3.6 “3.6” BinData
        //
        if (data.isString()) {
            String hexString = data.asString().getValue();
            return HexConverter.convertFromHex(hexString);
        }
        else if (data.isBinary()) {
            return data.asBinary().getData();
        }
        else {
            throw new IllegalArgumentException(
                    "Expected binary or string for _data field in resume token but found " + data.getBsonType());
        }
    }

    private ResumeTokens() {
        throw new AssertionError(getClass().getName() + " should not be instantiated");
    }
}
