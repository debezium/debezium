/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.util;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;

import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Unit test for {@code ResumeTokenDecoder }.
 */
public class ResumeTokenDecoderTest {
    private final static String RESUME_TOKENS_DATA_PATH = "resume_tokens.json";

    // todo: test tokens gotten from transactions
    // todo: use @TestFactory from jupiter
    @Test
    public void testResumeTokensParsedAsExpected() {
        var testCases = loadTestData();

        // assert that we have tests
        assertThat(testCases).isNotEmpty();

        // go through different test cases
        for (ResumeTokenTestCase tCase : testCases) {
            if (tCase.checkOnlyTxnOpIndex) {
                int actualTxnOpIdx = ResumeTokenDecoder.txnOpIndexFromTokenHex(tCase.inputToken);
                int expectedTxnOpIdx = tCase.expectedDocument.getInteger(KeyStringDecoder.TXN_OP_INDEX_KEY);
                assertThat(actualTxnOpIdx).isEqualTo(expectedTxnOpIdx).as(String.format("check txnOp test case %s", tCase.testName));
                continue;
            }

            Document actual = ResumeTokenDecoder.tokenHexToBson(tCase.inputToken);
            assertThat(normalizeDoc(actual)).isEqualTo(normalizeDoc(tCase.expectedDocument)).as(String.format("check test case %s", tCase.testName));
        }

        System.out.printf("successfully ran %s tests from file\n", testCases.size());
    }

    @Test
    public void testResumeTokensManually() {
        // test docs are equal
        String resumeToken = "8265F49506000007662B022C0100296E5A1004448A9E1A91724ECA875AAFA40773D05C46645F6964006465F49506238B3BBCC1CB20C00004";
        Document actualDoc = ResumeTokenDecoder.tokenHexToBson(resumeToken);
        Document expectedDoc = new Document()
                .append("timestamp", new Document()
                        .append("t", 1710527750)
                        .append("i", 1894))
                .append("version", 1)
                .append("tokenType", 128)
                .append("txnOpIndex", 0)
                .append("fromInvalidate", false)
                .append("uuid", "448a9e1a-9172-4eca-875a-afa40773d05c")
                .append("documentKey", new Document()
                        .append("_id", "65f49506238b3bbcc1cb20c0"));

        assertThat(normalizeDoc(actualDoc)).isEqualTo(normalizeDoc(expectedDoc));

        // test that equality check fails on diff txnOpIndex
        expectedDoc.remove("txnOpIndex");
        expectedDoc.append("txnOpIndex", 101);
        assertThat(normalizeDoc(actualDoc)).isNotEqualTo(normalizeDoc(expectedDoc));
    }

    private static Document normalizeDoc(Document doc) {
        var jsonSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
        return Document.parse(doc.toJson(jsonSettings));
    }

    /**
     * based on  AbstractMongoConnectorIT.loadTestDocuments(String pathOnClasspath)
     * */
    static List<ResumeTokenTestCase> loadTestData() {
        final List<ResumeTokenTestCase> documents = new ArrayList<>();

        try (InputStream stream = Testing.Files.readResourceAsStream(RESUME_TOKENS_DATA_PATH)) {
            assertThat(stream).isNotNull();
            var docArr = BsonArray.parse(IoUtil.read(stream));
            for (BsonValue docAsBson : docArr.getValues()) {
                Document doc = Document.parse(docAsBson.asDocument().toJson());
                documents.add(ResumeTokenTestCase.fromDocument(doc));
            }
        }
        catch (IOException e) {
            fail("Unable to find or read file '" + "resume_tokens.json" + "': " + e.getMessage());
        }
        assertThat(documents).isNotEmpty();
        return documents;
    }
}

class ResumeTokenTestCase {
    private final static String TEST_NAME_KEY = "test_name";
    private final static String INPUT_TOKEN_KEY = "token";
    private final static String EXPECTED_DOCUMENT_KEY = "document";
    private final static String CHECK_ONLY_TXN_INDEX_KEY = "checkOnlyTxnOpIndex";

    public String testName;
    public boolean checkOnlyTxnOpIndex;

    public String inputToken;
    public Document expectedDocument;

    public static ResumeTokenTestCase fromDocument(Document testCase) {
        var tCase = new ResumeTokenTestCase();
        tCase.inputToken = testCase.getString(INPUT_TOKEN_KEY);
        tCase.expectedDocument = testCase.get(EXPECTED_DOCUMENT_KEY, Document.class);
        tCase.testName = testCase.getString(TEST_NAME_KEY).isEmpty() ? "resume_token_test_" + RandomStringUtils.randomAlphanumeric(10)
                : testCase.getString(TEST_NAME_KEY);
        tCase.checkOnlyTxnOpIndex = testCase.getBoolean(CHECK_ONLY_TXN_INDEX_KEY, false);
        return tCase;
    }
}
