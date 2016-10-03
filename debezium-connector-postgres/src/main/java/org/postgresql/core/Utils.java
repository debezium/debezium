/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

/**
 * Collection of utilities used by the protocol-level code.
 */
public class Utils {
  /**
   * Turn a bytearray into a printable form, representing each byte in hex.
   *
   * @param data the bytearray to stringize
   * @return a hex-encoded printable representation of <code>data</code>
   */
  public static String toHexString(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte element : data) {
      sb.append(Integer.toHexString((element >> 4) & 15));
      sb.append(Integer.toHexString(element & 15));
    }
    return sb.toString();
  }

  /**
   * Keep a local copy of the UTF-8 Charset so we can avoid synchronization overhead from looking up
   * the Charset by name as String.getBytes(String) requires.
   */
  private final static Charset utf8Charset = Charset.forName("UTF-8");

  /**
   * Encode a string as UTF-8.
   *
   * @param str the string to encode
   * @return the UTF-8 representation of <code>str</code>
   */
  public static byte[] encodeUTF8(String str) {
    // See org.postgresql.benchmark.encoding.UTF8Encoding#string_getBytes
    // for performance measurements.
    // In OracleJDK 6u65, 7u55, and 8u40 String.getBytes(Charset) is
    // 3 times faster than other JDK approaches.
    return str.getBytes(utf8Charset);
  }

  /**
   * Escape the given literal <tt>value</tt> and append it to the string buffer <tt>sbuf</tt>. If
   * <tt>sbuf</tt> is <tt>null</tt>, a new StringBuffer will be returned. The argument
   * <tt>standardConformingStrings</tt> defines whether the backend expects standard-conforming
   * string literals or allows backslash escape sequences.
   *
   * @param sbuf the string buffer to append to; or <tt>null</tt>
   * @param value the string value
   * @param standardConformingStrings if standard conforming strings should be used
   * @return the sbuf argument; or a new string buffer for sbuf == null
   * @throws SQLException if the string contains a <tt>\0</tt> character
   * @deprecated use {@link #escapeLiteral(StringBuilder, String, boolean)} instead
   */
  public static StringBuffer appendEscapedLiteral(StringBuffer sbuf, String value,
      boolean standardConformingStrings) throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuffer(value.length() * 11 / 10); // Add 10% for escaping.
    }
    doAppendEscapedLiteral(sbuf, value, standardConformingStrings);
    return sbuf;
  }

  /**
   * Escape the given literal <tt>value</tt> and append it to the string builder <tt>sbuf</tt>. If
   * <tt>sbuf</tt> is <tt>null</tt>, a new StringBuilder will be returned. The argument
   * <tt>standardConformingStrings</tt> defines whether the backend expects standard-conforming
   * string literals or allows backslash escape sequences.
   *
   * @param sbuf the string builder to append to; or <tt>null</tt>
   * @param value the string value
   * @param standardConformingStrings if standard conforming strings should be used
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a <tt>\0</tt> character
   */
  public static StringBuilder escapeLiteral(StringBuilder sbuf, String value,
      boolean standardConformingStrings) throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder(value.length() * 11 / 10); // Add 10% for escaping.
    }
    doAppendEscapedLiteral(sbuf, value, standardConformingStrings);
    return sbuf;
  }

  /**
   * Common part for {@link #appendEscapedLiteral(StringBuffer, String, boolean)} and
   * {@link #escapeLiteral(StringBuilder, String, boolean)}
   *
   * @param sbuf Either StringBuffer or StringBuilder as we do not expect any IOException to be
   *        thrown
   * @param value value to append
   * @param standardConformingStrings if standard conforming strings should be used
   */
  private static void doAppendEscapedLiteral(Appendable sbuf, String value,
      boolean standardConformingStrings) throws SQLException {
    try {
      if (standardConformingStrings) {
        // With standard_conforming_strings on, escape only single-quotes.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new PSQLException(GT.tr("Zero bytes may not occur in string parameters."),
                PSQLState.INVALID_PARAMETER_VALUE);
          }
          if (ch == '\'') {
            sbuf.append('\'');
          }
          sbuf.append(ch);
        }
      } else {
        // With standard_conforming_string off, escape backslashes and
        // single-quotes, but still escape single-quotes by doubling, to
        // avoid a security hazard if the reported value of
        // standard_conforming_strings is incorrect, or an error if
        // backslash_quote is off.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new PSQLException(GT.tr("Zero bytes may not occur in string parameters."),
                PSQLState.INVALID_PARAMETER_VALUE);
          }
          if (ch == '\\' || ch == '\'') {
            sbuf.append(ch);
          }
          sbuf.append(ch);
        }
      }
    } catch (IOException e) {
      throw new PSQLException(GT.tr("No IOException expected from StringBuffer or StringBuilder"),
          PSQLState.UNEXPECTED_ERROR, e);
    }
  }

  /**
   * Escape the given identifier <tt>value</tt> and append it to the string buffer <tt>sbuf</tt>. If
   * <tt>sbuf</tt> is <tt>null</tt>, a new StringBuffer will be returned. This method is different
   * from appendEscapedLiteral in that it includes the quoting required for the identifier while
   * appendEscapedLiteral does not.
   *
   * @param sbuf the string buffer to append to; or <tt>null</tt>
   * @param value the string value
   * @return the sbuf argument; or a new string buffer for sbuf == null
   * @throws SQLException if the string contains a <tt>\0</tt> character
   * @deprecated use {@link #escapeIdentifier(StringBuilder, String)} instead
   */
  public static StringBuffer appendEscapedIdentifier(StringBuffer sbuf, String value)
      throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuffer(2 + value.length() * 11 / 10); // Add 10% for escaping.
    }
    doAppendEscapedIdentifier(sbuf, value);
    return sbuf;
  }

  /**
   * Escape the given identifier <tt>value</tt> and append it to the string builder <tt>sbuf</tt>.
   * If <tt>sbuf</tt> is <tt>null</tt>, a new StringBuilder will be returned. This method is
   * different from appendEscapedLiteral in that it includes the quoting required for the identifier
   * while {@link #escapeLiteral(StringBuilder, String, boolean)} does not.
   *
   * @param sbuf the string builder to append to; or <tt>null</tt>
   * @param value the string value
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a <tt>\0</tt> character
   */
  public static StringBuilder escapeIdentifier(StringBuilder sbuf, String value)
      throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder(2 + value.length() * 11 / 10); // Add 10% for escaping.
    }
    doAppendEscapedIdentifier(sbuf, value);
    return sbuf;
  }

  /**
   * Common part for appendEscapedIdentifier
   *
   * @param sbuf Either StringBuffer or StringBuilder as we do not expect any IOException to be
   *        thrown.
   * @param value value to append
   */
  private static void doAppendEscapedIdentifier(Appendable sbuf, String value) throws SQLException {
    try {
      sbuf.append('"');

      for (int i = 0; i < value.length(); ++i) {
        char ch = value.charAt(i);
        if (ch == '\0') {
          throw new PSQLException(GT.tr("Zero bytes may not occur in identifiers."),
              PSQLState.INVALID_PARAMETER_VALUE);
        }
        if (ch == '"') {
          sbuf.append(ch);
        }
        sbuf.append(ch);
      }

      sbuf.append('"');
    } catch (IOException e) {
      throw new PSQLException(GT.tr("No IOException expected from StringBuffer or StringBuilder"),
          PSQLState.UNEXPECTED_ERROR, e);
    }
  }

  /**
   * Attempt to parse the server version string into an XXYYZZ form version number.
   *
   * Returns 0 if the version could not be parsed.
   *
   * Returns minor version 0 if the minor version could not be determined, e.g. devel or beta
   * releases.
   *
   * If a single major part like 90400 is passed, it's assumed to be a pre-parsed version and
   * returned verbatim. (Anything equal to or greater than 10000 is presumed to be this form).
   *
   * The yy or zz version parts may be larger than 99. A NumberFormatException is thrown if a
   * version part is out of range.
   *
   * @param serverVersion server vertion in a XXYYZZ form
   * @return server version in number form
   * @deprecated use specific {@link Version} instance
   */
  @Deprecated
  public static int parseServerVersionStr(String serverVersion) throws NumberFormatException {
    return ServerVersion.parseServerVersionStr(serverVersion);
  }
}
