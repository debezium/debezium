/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.util;

import org.bson.Document;

/**
 * ResumeTokenDecoder for decoding Mongo <a href="https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-stream-resume-token">Change Stream</a>
 * resume tokens.
 * It is based on keystringdecoder.ts and resumetokendecoder.ts in the mongodb-resumetoken-decoder github repo
 * Sources:
 *     <a href="https://github.com/mongodb-js/mongodb-resumetoken-decoder/tree/5a7e79fbaf14ad3d935657e5cfbf968c76b98422">mongodb-resumetoken-decoder source</a>
 *     <a href="https://www.npmjs.com/package/mongodb-resumetoken-decoder?activeTab=readme">mongodb-resumetoken-decoder npm package</a>
 *     <a href="https://git.corp.stripe.com/gist/yuewang/f4df6dad99a069eb12f2bfa678d27f99">original java code generated based on t/js code</a>
 * */
public class ResumeTokenDecoder {
    public final static String DEFAULT_DECODER_VERSION = "v1";

    // Decode resumeToken using the specified Decoder version
    // this method tries its best to not throw exceptions and to parse partial tokens, returning a document with null fields if necessary
    // it is up to the caller to validate that the desired field exists.
    // Convenience methods like txnOpIndexFromTokenHex do validate and extract the desired field
    public static Document tokenHexToBson(String version, String resumeToken) {
        return KeyStringDecoder.tokenStringToBson(version, KeyStringDecoder.hexToByteArray(resumeToken));
    }

    // Decode resumeToken using the default decoder version
    // this method tries its best to not throw exceptions and to parse partial tokens, returning a document with null fields if necessary
    // it is up to the caller to validate that the desired field exists.
    // Convenience methods like txnOpIndexFromTokenHex do validate and extract the desired field
    public static Document tokenHexToBson(String resumeToken) {
        return KeyStringDecoder.tokenStringToBson(DEFAULT_DECODER_VERSION, KeyStringDecoder.hexToByteArray(resumeToken));
    }

    // Get just txnOpIndex from Token. Throws an exception if it doesn't exist or is null
    // TODO (tosinva): support partial parsing of token. This will require update to KeyStringDecoder
    public static int txnOpIndexFromTokenHex(String resumeToken) throws RuntimeException {
        try {
            Document document = KeyStringDecoder.tokenStringToBson(DEFAULT_DECODER_VERSION, KeyStringDecoder.hexToByteArray(resumeToken));
            // check if key exists, as missing key defaults to 0.
            if (document.containsKey(KeyStringDecoder.TXN_OP_INDEX_KEY) && document.get(KeyStringDecoder.TXN_OP_INDEX_KEY) != null) {
                return document.getInteger(KeyStringDecoder.TXN_OP_INDEX_KEY);
            }
            throw new RuntimeException(String.format("decoded token does not have '%s' field or its value is null", KeyStringDecoder.TXN_OP_INDEX_KEY));
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("received unexpected exception '%s'. Resume token: '%s'", e, resumeToken), e);
        }
    }
}
