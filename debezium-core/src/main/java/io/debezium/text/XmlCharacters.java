/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.text;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import io.debezium.annotation.Immutable;

/**
 * A utility class for determining the validity of various XML names, per the <a href="http://www.w3.org/TR/REC-xml/">XML 1.0
 * Specification</a>.
 *
 * @author Randall Hauch
 */
@Immutable
public class XmlCharacters {

    private static final int NUMBER_OF_CHARACTERS = 1 << 16; // 65536 or 0x10000

    /**
     * This implementation uses an array that captures for each character the XML classifications. An array is used because it is
     * a fast way of looking up each character.
     */
    private static final char[] MASKS = new char[NUMBER_OF_CHARACTERS];

    private static final int VALID_CHARACTER = 1;
    private static final int CONTENT_CHARACTER = 1 << 1;
    private static final int SPACE_CHARACTER = 1 << 2;
    private static final int NAME_START_CHARACTER = 1 << 3;
    private static final int NAME_CHARACTER = 1 << 4;
    private static final int NCNAME_START_CHARACTER = 1 << 5;
    private static final int NCNAME_CHARACTER = 1 << 6;
    private static final int PUBID_CHARACTER = 1 << 7;

    static {

        // ----------------
        // Valid Characters
        // ----------------
        // [2] Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
        // See http://www.w3.org/TR/REC-xml/#charsets
        MASKS[0x9] |= VALID_CHARACTER | CONTENT_CHARACTER;
        MASKS[0xA] |= VALID_CHARACTER | CONTENT_CHARACTER;
        MASKS[0xD] |= VALID_CHARACTER | CONTENT_CHARACTER;
        for (int i = 0x20; i <= 0xD7FF; ++i) {
            MASKS[i] |= VALID_CHARACTER | CONTENT_CHARACTER;
        }
        for (int i = 0xE000; i <= 0xFFFD; ++i) {
            MASKS[i] |= VALID_CHARACTER | CONTENT_CHARACTER;
        }
        // Last range is bigger than our character array, so we'll handle in the 'isValid' method ...
        // for ( int i=0x10000; i<=0x10FFFF; ++i ) MASKS[i] = VALID_CHARACTER_MASK | CONTENT_CHARACTER;

        // Remove the other characters that are not allowed in XML content:
        // '<', '&', '\n', '\r', ']'
        MASKS['<'] &= ~(CONTENT_CHARACTER);
        MASKS['&'] &= ~(CONTENT_CHARACTER);
        MASKS['\n'] &= ~(CONTENT_CHARACTER);
        MASKS['\r'] &= ~(CONTENT_CHARACTER);
        MASKS[']'] &= ~(CONTENT_CHARACTER);

        // ---------------------
        // Whitespace Characters
        // ---------------------
        // [3] S ::= (#x20 | #x9 | #xD | #xA)+
        // See http://www.w3.org/TR/REC-xml/#sec-common-syn
        MASKS[0x20] |= SPACE_CHARACTER;
        MASKS[0x9] |= SPACE_CHARACTER;
        MASKS[0xA] |= SPACE_CHARACTER;
        MASKS[0xD] |= SPACE_CHARACTER;

        // ---------------------
        // Name Start Characters
        // ---------------------
        // [4] NameStartChar ::= ":" | [A-Z] | "_" | [a-z] | [#xC0-#xD6] | [#xD8-#xF6] | [#xF8-#x2FF] |
        // [#x370-#x37D] | [#x37F-#x1FFF] | [#x200C-#x200D] | [#x2070-#x218F] |
        // [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] | [#xFDF0-#xFFFD] |
        // [#x10000-#xEFFFF]
        // See http://www.w3.org/TR/REC-xml/#sec-common-syn
        //
        // Note that all these start characters AND characters are valid for NAME and NCNAME
        int nameStartMask = NAME_START_CHARACTER | NCNAME_START_CHARACTER | NAME_CHARACTER | NCNAME_CHARACTER;
        MASKS[':'] |= nameStartMask;
        MASKS['_'] |= nameStartMask;
        for (int i = 'A'; i <= 'Z'; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 'a'; i <= 'z'; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0xC0; i <= 0xD6; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0xD8; i <= 0xF6; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0xF8; i <= 0x2FF; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x370; i <= 0x37D; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x37F; i <= 0x1FFF; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x200C; i <= 0x200D; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x2070; i <= 0x218F; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x2C00; i <= 0x2FEF; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x3001; i <= 0xD7FF; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0xF900; i <= 0xFDCF; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0xFDF0; i <= 0xFFFD; ++i) {
            MASKS[i] |= nameStartMask;
        }
        // Last range is bigger than our character array ...
        // for (int i = 0x10000; i <= 0xEFFFF; ++i) MASKS[i] |= nameStartMask;

        // ---------------
        // Name Characters
        // ---------------
        // [4a] NameChar ::= NameStartChar | "-" | "." | [0-9] | #xB7 | [#x0300-#x036F] | [#x203F-#x2040]
        // See http://www.w3.org/TR/REC-xml/#sec-common-syn
        //
        // Note that all these characters are valid for NAME and NCNAME
        int nameMask = NAME_CHARACTER | NCNAME_CHARACTER;
        MASKS['-'] |= nameMask;
        MASKS['.'] |= nameMask;
        MASKS[0xB7] |= nameMask;
        for (int i = '0'; i <= '9'; ++i) {
            MASKS[i] |= nameMask;
        }
        for (int i = 0x0300; i <= 0x036F; ++i) {
            MASKS[i] |= nameStartMask;
        }
        for (int i = 0x203F; i <= 0x2040; ++i) {
            MASKS[i] |= nameStartMask;
        }

        // --------
        // NC Names
        // --------
        // [4] NCName ::= NCNameStartChar NCNameChar*
        // which is just an XML Name, minus the ":"
        // See http://www.w3.org/TR/REC-xml-names/#ns-decl
        // So, remove the NCNAME_CHARACTER and NCNAME_START_CHARACTER masks from ':' ...
        MASKS[':'] &= ~(NCNAME_START_CHARACTER | NCNAME_CHARACTER);

        // --------------------
        // Public ID characters
        // --------------------
        // [13] PubidChar ::= #x20 | #xD | #xA | [a-zA-Z0-9] | [-'()+,./:=?;!*#@$_%]
        MASKS[0x20] |= PUBID_CHARACTER;
        MASKS[0xA] |= PUBID_CHARACTER;
        MASKS[0xD] |= PUBID_CHARACTER;
        for (int i = 'A'; i <= 'Z'; ++i) {
            MASKS[i] |= PUBID_CHARACTER;
        }
        for (int i = 'a'; i <= 'z'; ++i) {
            MASKS[i] |= PUBID_CHARACTER;
        }
        for (int i = '0'; i <= '9'; ++i) {
            MASKS[i] |= PUBID_CHARACTER;
        }
        MASKS['-'] |= PUBID_CHARACTER;
        MASKS['\''] |= PUBID_CHARACTER;
        MASKS['('] |= PUBID_CHARACTER;
        MASKS[')'] |= PUBID_CHARACTER;
        MASKS['+'] |= PUBID_CHARACTER;
        MASKS[','] |= PUBID_CHARACTER;
        MASKS['.'] |= PUBID_CHARACTER;
        MASKS['/'] |= PUBID_CHARACTER;
        MASKS[':'] |= PUBID_CHARACTER;
        MASKS['='] |= PUBID_CHARACTER;
        MASKS['?'] |= PUBID_CHARACTER;
        MASKS[';'] |= PUBID_CHARACTER;
        MASKS['!'] |= PUBID_CHARACTER;
        MASKS['*'] |= PUBID_CHARACTER;
        MASKS['#'] |= PUBID_CHARACTER;
        MASKS['@'] |= PUBID_CHARACTER;
        MASKS['$'] |= PUBID_CHARACTER;
        MASKS['_'] |= PUBID_CHARACTER;
        MASKS['%'] |= PUBID_CHARACTER;

    }

    private XmlCharacters() {
    }

    /**
     * Determine whether the supplied character is a valid first character in an XML Name. The first character in an XML name is
     * more restrictive than the {@link #isValidName(int) remaining characters}.
     *
     * @param c the character
     * @return true if the character is valid for an XML Name's first character
     */
    public static boolean isValidNameStart(int c) {
        return c < NUMBER_OF_CHARACTERS && (MASKS[c] & NAME_START_CHARACTER) != 0;
    }

    /**
     * Determine whether the supplied character is a valid first character in an XML NCName. The first character in an XML NCName
     * is more restrictive than the {@link #isValidName(int) remaining characters}.
     *
     * @param c the character
     * @return true if the character is valid for an XML NCName's first character
     */
    public static boolean isValidNcNameStart(int c) {
        return c < NUMBER_OF_CHARACTERS && (MASKS[c] & NCNAME_START_CHARACTER) != 0;
    }

    /**
     * Determine whether the supplied character is a valid non-first character in an XML Name. The {@link #isValidNameStart(int)
     * first character} in an XML name is more restrictive than the remaining characters.
     *
     * @param c the character
     * @return true if the character is valid character in an XML Name
     */
    public static boolean isValidName(int c) {
        return c < NUMBER_OF_CHARACTERS && (MASKS[c] & NAME_CHARACTER) != 0;
    }

    /**
     * Determine whether the supplied character is a valid non-first character in an XML NCName. The
     * {@link #isValidNcNameStart(int) first character} in an XML NCName is more restrictive than the remaining characters.
     *
     * @param c the character
     * @return true if the character is valid character in an XML NCName
     */
    public static boolean isValidNcName(int c) {
        return c < NUMBER_OF_CHARACTERS && (MASKS[c] & NCNAME_CHARACTER) != 0;
    }

    /**
     * Determine whether the supplied character is a valid character in an XML Pubid.
     *
     * @param c the character
     * @return true if the character is valid character in an XML Pubid
     */
    public static boolean isValidPubid(int c) {
        return c < NUMBER_OF_CHARACTERS && (MASKS[c] & PUBID_CHARACTER) != 0;
    }

    /**
     * Determine whether the supplied character is a valid character in XML.
     *
     * @param c the character
     * @return true if the character is valid character in XML
     */
    public static boolean isValid(int c) {
        return (c < NUMBER_OF_CHARACTERS && (MASKS[c] & VALID_CHARACTER) != 0) || (0x10000 <= c && c <= 0x10FFFF);
    }

    /**
     * Determine whether the supplied character is a valid character in XML content
     *
     * @param c the character
     * @return true if the character is valid character in XML content
     */
    public static boolean isValidContent(int c) {
        return (c < NUMBER_OF_CHARACTERS && (MASKS[c] & CONTENT_CHARACTER) != 0) || (0x10000 <= c && c <= 0x10FFFF);
    }

    /**
     * Determine whether the supplied character is a valid whitespace character in XML
     *
     * @param c the character
     * @return true if the character is valid whitespace character in XML
     */
    public static boolean isValidSpace(int c) {
        return c <= 0x20 && (MASKS[c] & SPACE_CHARACTER) != 0;
    }

    /**
     * Determine if the supplied name is a valid XML Name.
     *
     * @param name the string being checked
     * @return true if the supplied name is indeed a valid XML Name, or false otherwise
     */
    public static boolean isValidName(String name) {
        if (name == null || name.length() == 0) {
            return false;
        }
        CharacterIterator iter = new StringCharacterIterator(name);
        char c = iter.first();
        if (!isValidNameStart(c)) {
            return false;
        }
        while (c != CharacterIterator.DONE) {
            if (!isValidName(c)) {
                return false;
            }
            c = iter.next();
        }
        return true;
    }

    /**
     * Determine if the supplied name is a valid XML NCName.
     *
     * @param name the string being checked
     * @return true if the supplied name is indeed a valid XML NCName, or false otherwise
     */
    public static boolean isValidNcName(String name) {
        if (name == null || name.length() == 0) {
            return false;
        }
        CharacterIterator iter = new StringCharacterIterator(name);
        char c = iter.first();
        if (!isValidNcNameStart(c)) {
            return false;
        }
        while (c != CharacterIterator.DONE) {
            if (!isValidNcName(c)) {
                return false;
            }
            c = iter.next();
        }
        return true;
    }
}
