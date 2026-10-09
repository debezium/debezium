/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import org.bson.BsonDocument;

/**
 * All storable BSON types, including deprecated types and representative boundary values.
 */
final class MongoBsonTypeTestData {

    private MongoBsonTypeTestData() {
    }

    static BsonDocument values() {
        return BsonDocument.parse("""
                {"doubleValue":1.25,"stringValue":"Seoul 서울 🙂","documentValue":{"n":1},
                 "arrayValue":[1,2],"binaryValue":{"$binary":{"base64":"/wE=","subType":"00"}},
                 "undefinedValue":{"$undefined":true},"objectIdValue":{"$oid":"507f1f77bcf86cd799439011"},
                 "booleanValue":true,"dateValue":{"$date":{"$numberLong":"1783078553473"}},"nullValue":null,
                 "regexValue":{"$regularExpression":{"pattern":"a.*","options":"im"}},
                 "dbPointerValue":{"$dbPointer":{"$ref":"db.collection","$id":{"$oid":"507f1f77bcf86cd799439011"}}},
                 "javascriptValue":{"$code":"return 1;"},"symbolValue":{"$symbol":"symbol"},
                 "javascriptWithScopeValue":{"$code":"return x;","$scope":{"x":1}},
                 "int32Value":{"$numberInt":"42"},"timestampValue":{"$timestamp":{"t":1783078553,"i":7}},
                 "int64Value":{"$numberLong":"42"},
                 "decimalValue":{"$numberDecimal":"12345678901234567890.12345678901234"},
                 "minKeyValue":{"$minKey":1},"maxKeyValue":{"$maxKey":1},
                 "int32Min":{"$numberInt":"-2147483648"},"int32Max":{"$numberInt":"2147483647"},
                 "int64Min":{"$numberLong":"-9223372036854775808"},"int64Max":{"$numberLong":"9223372036854775807"},
                 "negativeZero":{"$numberDouble":"-0.0"},"nan":{"$numberDouble":"NaN"},
                 "positiveInfinity":{"$numberDouble":"Infinity"},"negativeInfinity":{"$numberDouble":"-Infinity"},
                 "decimalNan":{"$numberDecimal":"NaN"},"decimalInfinity":{"$numberDecimal":"Infinity"},
                 "negativeDate":{"$date":{"$numberLong":"-1"}},"unsignedTimestamp":{"$timestamp":{"t":4294967295,"i":4294967295}},
                 "oldBinary":{"$binary":{"base64":"/wE=","subType":"02"}},
                 "oldUuid":{"$binary":{"base64":"ABEiM0RVZneImaq7zN3u/w==","subType":"03"}},
                 "uuid":{"$binary":{"base64":"ABEiM0RVZneImaq7zN3u/w==","subType":"04"}},
                 "customBinary":{"$binary":{"base64":"/wE=","subType":"80"}},
                 "emptyDocument":{},"emptyArray":[],"mixedArray":[1,"two",null,{"x":true}]}
                """);
    }
}
