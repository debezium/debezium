/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.shyiko.mysql.binlog.network.AuthenticationException;

@RunWith(Parameterized.class)
public class BinlogReaderSanitizeAuthenticationExceptionTest {

    @Parameterized.Parameters(name = "unsanitized: {0}, sanitized: {1}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(
                new Object[][]{
                        { new String(new byte[]{ 0, 1, 2 }), "" },
                        { new String(new byte[]{ 'a', 0 }), "a" },
                        { new String(new byte[]{ 'a', 0, 0, '1' }), "a1" },
                        { "An ascii string", "An ascii string" },
                        { "中文。" + new String(new byte[]{ 0, 0, 0 }), "中文。" },
                        { "Punctuations.Are,preserved;right?", "Punctuations.Are,preserved;right?" },
                        { "New line \n and tabs \t too ?", "New line \n and tabs \t too ?" },
                });
    }

    @Parameterized.Parameter(0)
    public String unsanitizedString;

    @Parameterized.Parameter(1)
    public String sanitizedString;

    @Test
    public void sanitizeAuthenticationException() {
        AuthenticationException e = new AuthenticationException(unsanitizedString);
        AuthenticationException sanitized = BinlogStreamingChangeEventSource.sanitizeAuthenticationException(e);
        assertEquals(sanitizedString, sanitized.getMessage());
        assertEquals(e.getErrorCode(), sanitized.getErrorCode());
        assertEquals(e.getSqlState(), sanitized.getSqlState());
        assertEquals(e.getCause(), sanitized.getCause());
        assertEquals(e.getStackTrace(), sanitized.getStackTrace());
    }
}