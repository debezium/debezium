/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.text;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class XmlCharactersTest {

    @Test
    public void shouldNotAllowColonInNcName() {
        assertThat(XmlCharacters.isValidNcNameStart(':')).isFalse();
    }

    @Test
    public void shouldNotAllowDigitAsFirstCharacterInName() {
        assertThat(XmlCharacters.isValidNameStart('0')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('1')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('2')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('3')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('4')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('5')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('6')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('7')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('8')).isFalse();
        assertThat(XmlCharacters.isValidNameStart('9')).isFalse();
    }

    @Test
    public void shouldAllowLettersAsFirstCharacterInName() {
        for (char c = 'a'; c <= 'z'; ++c) {
            assertThat(XmlCharacters.isValidNameStart(c)).isTrue();
        }
        for (char c = 'A'; c <= 'Z'; ++c) {
            assertThat(XmlCharacters.isValidNameStart(c)).isTrue();
        }
    }

    @Test
    public void shouldNotAllowDigitAsFirstCharacterInNcName() {
        assertThat(XmlCharacters.isValidNcNameStart('0')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('1')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('2')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('3')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('4')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('5')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('6')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('7')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('8')).isFalse();
        assertThat(XmlCharacters.isValidNcNameStart('9')).isFalse();
    }
}
