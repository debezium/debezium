/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;

/**
 * KeyStringDecoder for decoding Mongo internal keystrings
 * It is based on keystringdecoder.ts and resumetokendecoder.ts in the mongodb-resumetoken-decoder github repo
 * Sources:
 *     <a href="https://github.com/mongodb-js/mongodb-resumetoken-decoder/tree/5a7e79fbaf14ad3d935657e5cfbf968c76b98422">mongodb-resumetoken-decoder source</a>
 *     <a href="https://www.npmjs.com/package/mongodb-resumetoken-decoder?activeTab=readme">mongodb-resumetoken-decoder npm package</a>
 *     <a href="https://git.corp.stripe.com/stripe-internal/mongo/blob/stripe-v4.4/src/mongo/db/storage/key_string.cpp">mongo/key_string.cpp</a>
 *     <a href="https://git.corp.stripe.com/gist/yuewang/f4df6dad99a069eb12f2bfa678d27f99">original java code generated based on t/js code</a>
 * */
public class KeyStringDecoder {
    public static final int kLess = 1;
    public static final int kGreater = 254;
    public static final int kEnd = 4;

    public static final int CType_kMinKey = 10;
    public static final int CType_kMaxKey = 240;
    public static final int CType_kNullish = 20;
    public static final int CType_kUndefined = 15;
    public static final int CType_kBoolTrue = 111;
    public static final int CType_kBoolFalse = 110;
    public static final int CType_kDate = 120;
    public static final int CType_kTimestamp = 130;
    public static final int CType_kOID = 100;
    public static final int CType_kStringLike = 60;
    public static final int CType_kCode = 160;
    public static final int CType_kCodeWithScope = 170;
    public static final int CType_kBinData = 90;
    public static final int CType_kRegEx = 140;
    public static final int CType_kDBRef = 150;
    public static final int CType_kObject = 70;
    public static final int CType_kArray = 80;
    public static final int CType_kNumeric = 30;

    public static final int CType_kNumericNaN = CType_kNumeric;
    public static final int CType_kNumericNegativeLargeMagnitude = CType_kNumeric + 1; // <= -2**63 including -Inf
    public static final int CType_kNumericNegative8ByteInt = CType_kNumeric + 2;
    public static final int CType_kNumericNegative7ByteInt = CType_kNumeric + 3;
    public static final int CType_kNumericNegative6ByteInt = CType_kNumeric + 4;
    public static final int CType_kNumericNegative5ByteInt = CType_kNumeric + 5;
    public static final int CType_kNumericNegative4ByteInt = CType_kNumeric + 6;
    public static final int CType_kNumericNegative3ByteInt = CType_kNumeric + 7;
    public static final int CType_kNumericNegative2ByteInt = CType_kNumeric + 8;
    public static final int CType_kNumericNegative1ByteInt = CType_kNumeric + 9;
    public static final int CType_kNumericNegativeSmallMagnitude = CType_kNumeric + 10; // between 0 and -1 exclusive
    public static final int CType_kNumericZero = CType_kNumeric + 11;
    public static final int CType_kNumericPositiveSmallMagnitude = CType_kNumeric + 12; // between 0 and 1 exclusive
    public static final int CType_kNumericPositive1ByteInt = CType_kNumeric + 13;
    public static final int CType_kNumericPositive2ByteInt = CType_kNumeric + 14;
    public static final int CType_kNumericPositive3ByteInt = CType_kNumeric + 15;
    public static final int CType_kNumericPositive4ByteInt = CType_kNumeric + 16;
    public static final int CType_kNumericPositive5ByteInt = CType_kNumeric + 17;
    public static final int CType_kNumericPositive6ByteInt = CType_kNumeric + 18;
    public static final int CType_kNumericPositive7ByteInt = CType_kNumeric + 19;
    public static final int CType_kNumericPositive8ByteInt = CType_kNumeric + 20;
    public static final int CType_kNumericPositiveLargeMagnitude = CType_kNumeric + 21; // >= 2**63 including +Inf

    // custom: introduce constant for txnOpIndex
    public static final String TXN_OP_INDEX_KEY = "txnOpIndex";

    public static int numBytesForInt(int ctype) {
        if (ctype >= CType_kNumericPositive1ByteInt) {
            return ctype - CType_kNumericPositive1ByteInt + 1;
        }
        return CType_kNumericNegative1ByteInt - ctype + 1;
    }

    static class BufferConsumer {
        byte[] buf;
        int index = 0;

        // custom: lower 32 bit mask
        static final long MASK_32_BIT = 0xFFFFFFFFL; // or '((1L << 32) - 1)'

        public BufferConsumer(byte[] buf) {
            this.buf = buf;
        }

        public int peekUint8() {
            if (index >= buf.length) {
                return -1;
            }
            return buf[index] & 0xff;
        }

        public int readUint8() {
            if (index >= buf.length) {
                throw new RuntimeException("unexpected end of input");
            }
            return buf[index++] & 0xff;
        }

        public long readUint32BE() {
            return (readUint8() << 24) |
                    (readUint8() << 16) |
                    (readUint8() << 8) |
                    (readUint8());
        }

        public long readUint64BE() {
            long high = readUint32BE();
            // custom: need to mask lower 32 bits for low before combining due to long being signed
            long low = readUint32BE() & MASK_32_BIT;
            return (high << 32) | low;
        }

        public byte[] readBytes(long n) {
            if (index + n > buf.length) {
                throw new RuntimeException("unexpected end of input");
            }
            byte[] ret = new byte[(int) n];
            System.arraycopy(buf, index, ret, 0, (int) n);
            index += n;
            return ret;
        }

        public String readCString() {
            int end = index;
            while (end < buf.length && buf[end] != 0) {
                end++;
            }
            String str = new String(buf, index, end - index, StandardCharsets.UTF_8);
            index = end + 1; // move past the null terminator
            return str;
        }

        public String readCStringWithNuls() {
            StringBuilder sb = new StringBuilder();
            while (true) {
                String str = readCString();
                if (str.isEmpty()) {
                    break;
                }
                sb.append(str).append('\0');
            }
            return sb.toString();
        }
    }

    public static byte[] hexToByteArray(String hexString) {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }

    public static String uint8ArrayToHex(byte[] arr) {
        StringBuilder sb = new StringBuilder();
        for (byte b : arr) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static Object readValue(int ctype, String version, BufferConsumer buf) {
        boolean isNegative = false;
        switch (ctype) {
            case CType_kMinKey:
                return new Object(); // Placeholder for MinKey
            case CType_kMaxKey:
                return new Object(); // Placeholder for MaxKey
            case CType_kNullish:
                return null;
            case CType_kUndefined:
                return null; // Can be considered as undefined
            case CType_kBoolTrue:
                return true;
            case CType_kBoolFalse:
                return false;
            case CType_kDate:
                return new Date(buf.readUint64BE() ^ (1L << 63));
            case CType_kTimestamp:
                long t = buf.readUint32BE();
                long i = buf.readUint32BE();
                return Map.of("t", t, "i", i);
            case CType_kOID:
                return uint8ArrayToHex(buf.readBytes(12));
            case CType_kStringLike:
                return buf.readCStringWithNuls();
            case CType_kCode:
                return buf.readCStringWithNuls(); // Placeholder for Code
            case CType_kCodeWithScope:
                String code = buf.readCStringWithNuls();
                Map<String, Object> scope = (Map<String, Object>) keystringToBsonPartial(version, buf, "named");
                return Map.of("code", code, "scope", scope);
            case CType_kBinData:
                long size = buf.readUint8();
                if (size == 0xff) {
                    size = buf.readUint32BE();
                }
                byte subtype = (byte) buf.readUint8();
                byte[] data = buf.readBytes(size);
                return Map.of("type", (int) subtype, "data", data);
            case CType_kRegEx:
                String pattern = buf.readCString();
                String flags = buf.readCString();
                return Map.of("pattern", pattern, "flags", flags);
            case CType_kDBRef:
                long nsSize = buf.readUint32BE();
                String ns = new String(buf.readBytes(nsSize), StandardCharsets.UTF_8);
                String refId = uint8ArrayToHex(buf.readBytes(12));
                return Map.of("$ref", ns, "$id", refId);
            case CType_kObject:
                return keystringToBsonPartial(version, buf, "named");
            case CType_kArray:
                List<Object> arr = new ArrayList<>();
                while (buf.peekUint8() != 0) {
                    arr.add(keystringToBsonPartial(version, buf, "single"));
                }
                buf.readUint8(); // consume 0-byte
                return arr;
            case CType_kNumericNaN:
                return Double.NaN;
            case CType_kNumericZero:
                return 0;
            case CType_kNumericNegativeLargeMagnitude:
                isNegative = true;
            case CType_kNumericPositiveLargeMagnitude: {
                long encoded = buf.readUint64BE();
                if (isNegative) {
                    // comment: the right-hand operand to the and isn't a true 64 bit mask and is unnecessary given
                    // that encoded is already a long
                    // see here: https://docs.oracle.com/javase/specs/jls/se14/html/jls-15.html#jls-15.19
                    encoded = ~encoded; // custom: don't attempt to mask with ((1L << 64) - 1);
                }
                if (version.equals("v0")) {
                    return Double.longBitsToDouble(encoded);
                }
                else if ((encoded & (1L << 63)) == 0) { // In range of (finite) doubles
                    boolean hasDecimalContinuation = (encoded & 1) != 0;
                    encoded >>= 1; // remove decimal continuation marker
                    encoded |= (1L << 62); // implied leading exponent bit
                    double bin = Double.longBitsToDouble(encoded);
                    if (isNegative) {
                        bin = -bin;
                    }
                    if (hasDecimalContinuation) {
                        buf.readUint64BE(); // consume decimal continuation
                    }
                    return bin;
                }
                else if (encoded == 0xffff_ffff_ffff_ffffL) { // Infinity
                    return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                }
                else {
                    buf.readUint64BE(); // low bits
                    return isNegative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                }
            }
            case CType_kNumericNegativeSmallMagnitude:
                isNegative = true;
            case CType_kNumericPositiveSmallMagnitude: {
                long encoded = buf.readUint64BE();
                if (isNegative) {
                    // comment: the right-hand operand to the and isn't a true 64 bit mask and is unnecessary given
                    // that encoded is already a long
                    // see here: https://docs.oracle.com/javase/specs/jls/se14/html/jls-15.html#jls-15.19
                    encoded = ~encoded; // custom: don't attempt to mask with ((1L << 64) - 1);
                }
                switch ((int) (encoded >> 62)) {
                    case 0x0: {
                        // Teeny tiny decimal, smaller magnitude than 2**(-1074)
                        buf.readUint64BE(); // consume 64 bits
                        return 0;
                    }
                    case 0x1:
                    case 0x2: {
                        boolean hasDecimalContinuation = (encoded & 1) != 0;
                        encoded -= 1L << 62;
                        encoded >>= 1;
                        double scaledBin = Double.longBitsToDouble(encoded);
                        double bin = scaledBin * Math.pow(2, -256);
                        if (hasDecimalContinuation) {
                            buf.readUint64BE(); // consume decimal continuation
                        }
                        return isNegative ? -bin : bin;
                    }
                    case 0x3: {
                        encoded >>= 2;
                        double bin = Double.longBitsToDouble(encoded);
                        return isNegative ? -bin : bin;
                    }
                    default:
                        throw new RuntimeException("unreachable");
                }
            }
            case CType_kNumericNegative8ByteInt:
            case CType_kNumericNegative7ByteInt:
            case CType_kNumericNegative6ByteInt:
            case CType_kNumericNegative5ByteInt:
            case CType_kNumericNegative4ByteInt:
            case CType_kNumericNegative3ByteInt:
            case CType_kNumericNegative2ByteInt:
            case CType_kNumericNegative1ByteInt:
                isNegative = true;
            case CType_kNumericPositive1ByteInt:
            case CType_kNumericPositive2ByteInt:
            case CType_kNumericPositive3ByteInt:
            case CType_kNumericPositive4ByteInt:
            case CType_kNumericPositive5ByteInt:
            case CType_kNumericPositive6ByteInt:
            case CType_kNumericPositive7ByteInt:
            case CType_kNumericPositive8ByteInt: {
                long encodedIntegerPart = 0;
                int intBytesRemaining = numBytesForInt(ctype);
                while (intBytesRemaining-- > 0) {
                    byte byteVal = (byte) buf.readUint8();
                    if (isNegative) {
                        byteVal = (byte) ~byteVal;
                    }
                    encodedIntegerPart = (encodedIntegerPart << 8) | (byteVal & 0xFFL);
                }
                boolean haveFractionalPart = (encodedIntegerPart & 1) != 0;
                long integerPart = encodedIntegerPart >> 1;
                if (!haveFractionalPart) {
                    if (isNegative) {
                        integerPart = -integerPart;
                    }
                    if (integerPart >= Integer.MIN_VALUE && integerPart <= Integer.MAX_VALUE) {
                        return (int) integerPart;
                    }
                    return integerPart;
                }
                long fracBytes;
                long encodedFraction;
                if (version.equals("v0")) {
                    fracBytes = 8 - numBytesForInt(ctype);
                    encodedFraction = integerPart;
                    for (int fracBytesRemaining = (int) fracBytes; fracBytesRemaining > 0; fracBytesRemaining--) {
                        byte byteVal = (byte) buf.readUint8();
                        if (isNegative) {
                            byteVal = (byte) ~byteVal;
                        }
                        encodedFraction = (encodedFraction << 8) | (byteVal & 0xFFL);
                    }
                    double bin = Double.longBitsToDouble(encodedFraction & ~3L);
                    long dcm = fracBytes != 0 ? (encodedFraction & 3) : 3;
                    if (dcm != 0 && dcm != 2) {
                        buf.readUint64BE(); // consume 64 bits
                    }
                    return isNegative ? -bin : bin;
                }
                else { // "v1" case
                    fracBytes = 8 - numBytesForInt(ctype);
                    encodedFraction = integerPart;
                    for (int fracBytesRemaining = (int) fracBytes; fracBytesRemaining > 0; fracBytesRemaining--) {
                        byte byteVal = (byte) buf.readUint8();
                        if (isNegative) {
                            byteVal = (byte) ~byteVal;
                        }
                        encodedFraction = (encodedFraction << 8) | (byteVal & 0xFFL);
                    }
                    double bin = Double.longBitsToDouble(encodedFraction & ~3L) * Math.pow(2, -8 * fracBytes);
                    long dcm = fracBytes != 0 ? (encodedFraction & 3) : 3;
                    if (dcm != 0 && dcm != 2) {
                        buf.readUint64BE(); // consume 64 bits
                    }
                    return isNegative ? -bin : bin;
                }
            }
            default:
                throw new RuntimeException("Unknown keystring ctype " + ctype);
        }
    }

    private static String createUUID(byte[] data) {
        long msb = 0;
        long lsb = 0;
        assert data.length == 16 : "data must be 16 bytes in length";
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (data[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (data[i] & 0xff);
        }
        return new UUID(msb, lsb).toString();
    }

    private static Object keystringToBsonPartial(String version, BufferConsumer buf, String mode) {
        Map<String, Object> contents = new HashMap<>();
        List<Object> contentsList = new ArrayList<>();
        while (buf.peekUint8() != -1) {
            int ctype = buf.readUint8();
            if (ctype == kLess || ctype == kGreater) {
                ctype = buf.readUint8();
            }
            if (ctype == kEnd) {
                break;
            }
            if ("named".equals(mode)) {
                if (ctype == 0) {
                    break;
                }
                String key = buf.readCString();
                ctype = buf.readUint8(); // again ctype, but more accurate this time
                contents.put(key, readValue(ctype, version, buf));
            }
            else if ("single".equals(mode)) {
                return readValue(ctype, version, buf);
            }
            else {
                contentsList.add(readValue(ctype, version, buf));
            }
        }
        return "named".equals(mode) ? contents : contentsList;
    }

    public static Document tokenStringToBson(String version, byte[] buf) {
        List<Object> obj = (List<Object>) keystringToBsonPartial(version, new BufferConsumer(buf), "toplevel");
        Document bsonDoc = new Document();

        try {
            // comment: improve null and type handling
            bsonDoc.append("timestamp", getOrNull(obj, 0));
            Object versionOrNull = getOrNull(obj, 1);
            bsonDoc.append("version", versionOrNull);
            bsonDoc.append("tokenType", getOrNull(obj, 2));
            bsonDoc.append(TXN_OP_INDEX_KEY, getOrNull(obj, 3));
            bsonDoc.append("fromInvalidate", getOrNull(obj, 4));
            bsonDoc.append("uuid", uuidFromObjMap(getOrNull(obj, 5)));
            if ((versionOrNull instanceof Integer)) {
                bsonDoc.append((int) versionOrNull == 2 ? "eventIdentifier" : "documentKey", getOrNull(obj, 6));
            }
        }
        catch (Exception ignored) {
            // comment: try to add as many fields as we have. It's possible in some cases, some fields are missing e.g. empty documentKey
        }

        return bsonDoc;
    }

    private static String uuidFromObjMap(Object obj) {
        String uuid = null;
        if (obj instanceof Map) {
            Map<String, Object> tmp = (Map<String, Object>) obj;
            byte[] data = (byte[]) (tmp.get("data"));
            uuid = createUUID(data);
        }
        return uuid;
    }

    // comment: support getting an item from the list if it exists or returns null
    static Object getOrNull(List<Object> obj, int index) {
        if (obj.size() >= index + 1) {
            return obj.get(index);
        }
        return null;
    }
}
