/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.re2j.Pattern;

import io.debezium.doc.FixFor;
import io.debezium.text.ParsingException;

/**
 * @author Randall Hauch
 * @author Horia Chiorean
 */
public class StringsTest {

    public void compareSeparatedLines(Object... lines) {
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

    @Test
    public void replaceVariablesShouldReplaceVariableThatHaveSameCase() {
        assertReplacement("some ${v1} text", vars("v1", "abc"), "some abc text");
        assertReplacement("some ${v1} text", vars("v1", "abc", "V1", "ABC"), "some abc text");
        assertReplacement("some ${v1:other} text", vars("V1", "ABC"), "some other text");
    }

    @Test
    public void replaceVariablesShouldNotReplaceVariableThatHasNoDefaultAndIsNotFound() {
        assertReplacement("some ${varName} text", vars("v1", "value1"), "some ${varName} text");
        assertReplacement("some${varName}text", vars("v1", "value1"), "some${varName}text");
        assertReplacement("${varName}", vars("v1", "value1"), "${varName}");
    }

    @Test
    public void replaceVariablesShouldReplaceVariablesWithNoDefault() {
        assertReplacement("${varName}", vars("varName", "replaced"), "replaced");
        assertReplacement("some${varName}text", vars("varName", "replaced"), "somereplacedtext");
        assertReplacement("some ${varName} text", vars("varName", "replaced"), "some replaced text");
        assertReplacement("some ${var1,var2,var3:other} text", vars("var1", "replaced"), "some replaced text");
        assertReplacement("some ${var1,var2,var3:other} text", vars("v1", "replaced", "var2", "new"), "some new text");
        assertReplacement("some ${var1,var2,var3:other} text", vars("v1", "replaced", "var3", "new"), "some new text");
    }

    @Test
    public void replaceVariablesShouldReplaceVariablesWithDefaultWhenNoReplacementIsFound() {
        assertReplacement("some${varName:other}text", vars("v1", "replaced"), "someothertext");
        assertReplacement("some ${varName:other} text", vars("v1", "replaced"), "some other text");
        assertReplacement("some ${var1,var2,var3:other} text", vars("var10", "replaced"), "some other text");
        assertReplacement("some ${var1,var2,var3:other} text", vars("var10", "replaced", "var11", "new"), "some other text");
    }

    @Test
    public void replaceVariablesShouldReplaceMultipleVariables() {
        assertReplacement("${v1}${v1}", vars("v1", "first", "v2", "second"), "firstfirst");
        assertReplacement("${v1}${v2}", vars("v1", "first", "v2", "second"), "firstsecond");
        assertReplacement("some${v1}text${v2}end", vars("v1", "first", "v2", "second"), "somefirsttextsecondend");
        assertReplacement("some ${v1} text ${v2} end", vars("v1", "first", "v2", "second"), "some first text second end");
        assertReplacement("some ${v1:other} text", vars("vx1", "replaced"), "some other text");
        assertReplacement("some ${v1,v2,v3:other} text ${v1,v2,v3:other}", vars("var10", "replaced", "v2", "s"), "some s text s");
        assertReplacement("some ${v1,v2:other}${v2,v3:other} text", vars("v1", "1", "v2", "2"), "some 12 text");
    }

    @Test
    public void isNullOrEmptyReturnsTrueForNull() {
        assertThat(Strings.isNullOrEmpty(null)).isTrue();
    }

    @Test
    public void isNullOrEmptyReturnsTrueForEmptyString() {
        assertThat(Strings.isNullOrEmpty("")).isTrue();
    }

    @Test
    public void isNullOrEmptyReturnsFalseForFalseForNonEmptyString() {
        assertThat(Strings.isNullOrEmpty("hello")).isFalse();
    }

    @Test
    public void isNullOrEmptyReturnsFalseForFalseForBlankString() {
        assertThat(Strings.isNullOrEmpty("     ")).isFalse();
    }

    @Test
    public void isNumericShouldReturnFalseForNull() {
        assertThat(Strings.isNumeric(null)).isFalse();
    }

    @Test
    public void isNumericShouldReturnFalseForEmptyString() {
        assertThat(Strings.isNumeric("")).isFalse();
    }

    @Test
    public void isNumericShouldReturnFalseForBlankString() {
        assertThat(Strings.isNumeric("     ")).isFalse();
    }

    @Test
    public void isNumericShouldReturnTrueForNumericString() {
        assertThat(Strings.isNumeric("123")).isTrue();
    }

    public void unquoteIdentifierPartShouldReturnNullForNull() {
        assertThat(Strings.unquoteIdentifierPart(null)).isNull();
    }

    @Test
    public void unquoteIdentifierPartShouldReturnSameValueForUnquotedString() {
        assertThat(Strings.unquoteIdentifierPart("table")).isEqualTo("table");
    }

    @Test
    public void unquoteIdentifierPartShouldReturnEmptyStringForEmptyQuotedString() {
        assertThat(Strings.unquoteIdentifierPart("''")).isEqualTo("");
    }

    @Test
    public void unquoteIdentifierPartShouldReturnUnquotedString() {
        assertThat(Strings.unquoteIdentifierPart("'Table'")).isEqualTo("Table");
    }

    @Test
    public void unquoteIdentifierPartShouldUnescapeEscapedQuote() {
        assertThat(Strings.unquoteIdentifierPart("'Tab''le'")).isEqualTo("Tab'le");
    }

    @Test
    public void unquoteIdentifierPartShouldSupportDoubleQuotes() {
        assertThat(Strings.unquoteIdentifierPart("\"Tab\"\"le\"")).isEqualTo("Tab\"le");
    }

    @Test
    public void unquoteIdentifierPartShouldSupportBackTicks() {
        assertThat(Strings.unquoteIdentifierPart("`Tab``le`")).isEqualTo("Tab`le");
    }

    @Test
    public void hexStringToByteArrayShouldReturnCorrectByteArray() {
        assertThat(Strings.hexStringToByteArray(null)).isNull();
        assertThat(Strings.hexStringToByteArray("00")).isEqualTo(new byte[]{ 0 });
        assertThat(Strings.hexStringToByteArray("010203")).isEqualTo(new byte[]{ 1, 2, 3 });
        assertThat(Strings.hexStringToByteArray("CAFEBABE")).isEqualTo(new byte[]{ -54, -2, -70, -66 });
    }

    @Test
    public void regexSplit() {
        assertRegexSet("a,b", "a", "b");
        assertRegexSet("a\\,b", "a,b");
        assertRegexSet("a,b,", "a", "b");
        assertRegexSet("a,b\\,", "a", "b,");
        assertRegexSet("a\\\\\\,b", "a\\\\,b");
        assertRegexSet(
                "DROP TEMPORARY TABLE IF EXISTS .+ /\\\\* generated by server \\\\*/,"
                        + "INSERT INTO mysql.rds_heartbeat2\\(.*\\,.*\\) values \\(.*\\,.*\\) ON DUPLICATE KEY UPDATE value = .*",
                "DROP TEMPORARY TABLE IF EXISTS .+ /\\\\* generated by server \\\\*/",
                "INSERT INTO mysql.rds_heartbeat2\\(.*,.*\\) values \\(.*,.*\\) ON DUPLICATE KEY UPDATE value = .*");
        assertRegexList("a,b", "a", "b");
        assertRegexList("a\\,b", "a,b");
        assertRegexList("a,b,", "a", "b");
        assertRegexList("a,b\\,", "a", "b,");
        assertRegexList("a\\\\\\,b", "a\\\\,b");
        assertRegexList("DROP TEMPORARY TABLE IF EXISTS .+ /\\\\* generated by server \\\\*/,"
                + "INSERT INTO mysql.rds_heartbeat2\\(.*\\,.*\\) values \\(.*\\,.*\\) ON DUPLICATE KEY UPDATE value = .*",
                "DROP TEMPORARY TABLE IF EXISTS .+ /\\\\* generated by server \\\\*/",
                "INSERT INTO mysql.rds_heartbeat2\\(.*,.*\\) values \\(.*,.*\\) ON DUPLICATE KEY UPDATE value = .*");
    }

    @Test
    public void joinShouldExcludeFirstNullableElement() {
        assertThat(Strings.join(",", Arrays.asList(null, "b", "c"))).isEqualTo("b,c");
    }

    @Test
    public void joinShouldExcludeSecondNullableElement() {
        assertThat(Strings.join(",", Arrays.asList("a", null, "c"))).isEqualTo("a,c");
    }

    @Test
    public void joinShouldExcludeAllNullableElements() {
        assertThat(Strings.join(",", Arrays.asList(null, null, null))).isEqualTo("");
    }

    @Test
    public void joinWithConversionShouldConvertAllElements() {
        assertThat(Strings.join(",", Arrays.asList("a", "b", "c"), s -> "_" + s)).isEqualTo("_a,_b,_c");
    }

    @Test(expected = ParsingException.class)
    public void regexSplitWrongEscape() {
        Strings.setOfRegex("a,b\\,c\\");
    }

    @Test
    @FixFor("DBZ-1164")
    public void asDurationShouldConvertValue() {
        assertThat(Strings.asDuration(null)).isNull();
        assertThat(Strings.asDuration("24:00:00")).isEqualTo(Duration.parse("PT24H"));
        assertThat(Strings.asDuration("18:02:54.123")).isEqualTo(Duration.parse("PT18H2M54.123S"));
        assertThat(Strings.asDuration("18:02:54.123456789")).isEqualTo(Duration.parse("PT18H2M54.123456789S"));
        assertThat(Strings.asDuration("24:00:01")).isEqualTo(Duration.parse("PT24H1S"));
    }

    @Test
    public void startsWithIgnoreCase() {
        assertThat(Strings.startsWithIgnoreCase("INSERT INTO", "insert")).isTrue();
        assertThat(Strings.startsWithIgnoreCase("INSERT INTO", "INSERT")).isTrue();
        assertThat(Strings.startsWithIgnoreCase("insert INTO", "INSERT")).isTrue();
        assertThat(Strings.startsWithIgnoreCase("INSERT INTO", "update")).isFalse();
    }

    @Test
    @FixFor("DBZ-1340")
    public void getBegin() {
        assertThat(Strings.getBegin(null, 10)).isNull();
        assertThat(Strings.getBegin("", 10)).isEqualTo("");
        assertThat(Strings.getBegin("INSERT ", 7)).isEqualTo("INSERT ");
        assertThat(Strings.getBegin("INSERT INTO", 7)).isEqualTo("INSERT ");
        assertThat(Strings.getBegin("UPDATE mytable", 7)).isEqualTo("UPDATE ");
        assertThat(Strings.getBegin("delete from ", 7).toUpperCase()).isEqualTo("DELETE ");
    }

    @Test
    public void isBlankShouldReturnTrueForNull() {
        assertThat(Strings.isNullOrBlank(null)).isTrue();
    }

    @Test
    public void isBlankShouldReturnTrueForEmptyString() {
        assertThat(Strings.isNullOrBlank("")).isTrue();
    }

    @Test
    public void isBlankShouldReturnTrueForStringWithOnlyWhitespace() {
        assertThat(Strings.isNullOrBlank("  \t ")).isTrue();
    }

    @Test
    public void isBlankShouldReturnFalseForStringWithNonWhitespaceCharacters() {
        assertThat(Strings.isNullOrBlank("not blank")).isFalse();
    }

    @Test
    public void durationToString() {
        assertThat(Strings.duration(0)).isEqualTo("00:00:00.0");
        assertThat(Strings.duration(1)).isEqualTo("00:00:00.001");
        assertThat(Strings.duration(10)).isEqualTo("00:00:00.01");
        assertThat(Strings.duration(100)).isEqualTo("00:00:00.1");
        assertThat(Strings.duration(1_000)).isEqualTo("00:00:01.0");
        assertThat(Strings.duration(60_000)).isEqualTo("00:01:00.0");
        assertThat(Strings.duration(61_010)).isEqualTo("00:01:01.01");
        assertThat(Strings.duration(3_600_000)).isEqualTo("01:00:00.0");
        assertThat(Strings.duration(36_000_000)).isEqualTo("10:00:00.0");
        assertThat(Strings.duration(540_000_000)).isEqualTo("150:00:00.0");
        assertThat(Strings.duration(541_934_321)).isEqualTo("150:32:14.321");
    }

    protected void assertReplacement(String before, Map<String, String> replacements, String after) {
        String result = Strings.replaceVariables(before, replacements::get);
        assertThat(result).isEqualTo(after);
    }

    protected Map<String, String> vars(String var1, String val1) {
        return Collect.hashMapOf(var1, val1);
    }

    protected Map<String, String> vars(String var1, String val1, String var2, String val2) {
        return Collect.hashMapOf(var1, val1, var2, val2);
    }

    protected void assertRegexSet(String patterns, String... matches) {
        Set<Pattern> regexSet = Strings.setOfRegex(patterns);

        assertThat(regexSet.stream()
                .map(Pattern::pattern)
                .collect(Collectors.toSet())).containsOnly(matches);
    }

    protected void assertRegexList(String patterns, String... matches) {
        List<Pattern> regexList = Strings.listOfRegex(patterns, Pattern.CASE_INSENSITIVE);
        assertThat(regexList.stream()
                .map(Pattern::pattern)
                .collect(Collectors.toList())).isEqualTo(Arrays.asList((Object[]) matches));
    }
}
