/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.junit.Test;

/**
 * @author Randall Hauch
 * @author Horia Chiorean
 */
public class StringsTest {

    public void compareSeparatedLines( Object... lines ) {
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(content);
        for (Object line : lines) {
            stream.println(line);
        }
        List<String> actualLines = Strings.splitLines(content.toString());
        assertArrayEquals(lines, actualLines.toArray());
    }

    @Test
    public void splitLinesShouldWorkCorrectly() {
        compareSeparatedLines("Line 1", "Line 2", "Line 3", "Line 4");
    }

    @Test
    public void setLengthShouldTruncateStringsThatAreTooLong() {
        assertEquals("This is the st", Strings.setLength("This is the string", 14, ' '));
    }

    @Test
    public void setLengthShouldAppendCharacterForStringsThatAreTooShort() {
        assertEquals("This      ", Strings.setLength("This", 10, ' '));
    }

    @Test
    public void setLengthShouldNotRemoveLeadingWhitespace() {
        assertEquals(" This     ", Strings.setLength(" This", 10, ' '));
        assertEquals("\tThis     ", Strings.setLength("\tThis", 10, ' '));
    }

    @Test
    public void setLengthShouldAppendCharacterForEmptyStrings() {
        assertEquals("          ", Strings.setLength("", 10, ' '));
    }

    @Test
    public void setLengthShouldAppendCharacterForNullStrings() {
        assertEquals("          ", Strings.setLength(null, 10, ' '));
    }

    @Test
    public void setLengthShouldReturnStringsThatAreTheDesiredLength() {
        assertEquals("This is the string", Strings.setLength("This is the string", 18, ' '));
    }

    @Test
    public void justifyLeftShouldTruncateStringsThatAreTooLong() {
        assertEquals("This is the st", Strings.justifyLeft("This is the string", 14, ' '));
    }

    @Test
    public void justifyLeftShouldAppendCharacterForStringsThatAreTooShort() {
        assertEquals("This      ", Strings.justifyLeft("This", 10, ' '));
    }

    @Test
    public void justifyLeftShouldRemoveLeadingWhitespace() {
        assertEquals("This      ", Strings.justifyLeft(" This", 10, ' '));
        assertEquals("This      ", Strings.justifyLeft("\tThis", 10, ' '));
    }

    @Test
    public void justifyLeftShouldAppendCharacterForEmptyStrings() {
        assertEquals("          ", Strings.justifyLeft("", 10, ' '));
    }

    @Test
    public void justifyLeftShouldAppendCharacterForNullStrings() {
        assertEquals("          ", Strings.justifyLeft(null, 10, ' '));
    }

    @Test
    public void justifyLeftShouldReturnStringsThatAreTheDesiredLength() {
        assertEquals("This is the string", Strings.justifyLeft("This is the string", 18, ' '));
    }

    @Test
    public void justifyRightShouldTruncateStringsThatAreTooLong() {
        assertEquals(" is the string", Strings.justifyRight("This is the string", 14, ' '));
    }

    @Test
    public void justifyRightShouldPrependCharacterForStringsThatAreTooShort() {
        assertEquals("      This", Strings.justifyRight("This", 10, ' '));
    }

    @Test
    public void justifyRightShouldPrependCharacterForEmptyStrings() {
        assertEquals("          ", Strings.justifyRight("", 10, ' '));
    }

    @Test
    public void justifyRightShouldPrependCharacterForNullStrings() {
        assertEquals("          ", Strings.justifyRight(null, 10, ' '));
    }

    @Test
    public void justifyRightShouldReturnStringsThatAreTheDesiredLength() {
        assertEquals("This is the string", Strings.justifyRight("This is the string", 18, ' '));
    }

    @Test
    public void justifyCenterShouldTruncateStringsThatAreTooLong() {
        assertEquals("This is the st", Strings.justifyCenter("This is the string", 14, ' '));
    }

    @Test
    public void justifyCenterShouldPrependAndAppendSameNumberOfCharacterForStringsThatAreTooShortButOfAnEvenLength() {
        assertEquals("   This   ", Strings.justifyCenter("This", 10, ' '));
    }

    @Test
    public void justifyCenterShouldPrependOneMoreCharacterThanAppendingForStringsThatAreTooShortButOfAnOddLength() {
        assertEquals("   Thing  ", Strings.justifyCenter("Thing", 10, ' '));
    }

    @Test
    public void justifyCenterShouldPrependCharacterForEmptyStrings() {
        assertEquals("          ", Strings.justifyCenter("", 10, ' '));
    }

    @Test
    public void justifyCenterShouldPrependCharacterForNullStrings() {
        assertEquals("          ", Strings.justifyCenter(null, 10, ' '));
    }

    @Test
    public void justifyCenterShouldReturnStringsThatAreTheDesiredLength() {
        assertEquals("This is the string", Strings.justifyCenter("This is the string", 18, ' '));
    }
}