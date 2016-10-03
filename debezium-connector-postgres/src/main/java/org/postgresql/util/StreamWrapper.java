/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Wrapper around a length-limited InputStream.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public class StreamWrapper {

  private static final int MAX_MEMORY_BUFFER_BYTES = 51200;

  private static final String TEMP_FILE_PREFIX = "postgres-pgjdbc-stream";

  public StreamWrapper(byte[] data, int offset, int length) {
    this.stream = null;
    this.rawData = data;
    this.offset = offset;
    this.length = length;
  }

  public StreamWrapper(InputStream stream, int length) {
    this.stream = stream;
    this.rawData = null;
    this.offset = 0;
    this.length = length;
  }

  public StreamWrapper(InputStream stream) throws PSQLException {
    try {
      ByteArrayOutputStream memoryOutputStream = new ByteArrayOutputStream();
      final int memoryLength = copyStream(stream, memoryOutputStream, MAX_MEMORY_BUFFER_BYTES);
      byte[] rawData = memoryOutputStream.toByteArray();

      if (memoryLength == -1) {
        final int diskLength;
        final File tempFile = File.createTempFile(TEMP_FILE_PREFIX, null);
        FileOutputStream diskOutputStream = new FileOutputStream(tempFile);
        diskOutputStream.write(rawData);
        try {
          diskLength = copyStream(stream, diskOutputStream, Integer.MAX_VALUE - rawData.length);
          if (diskLength == -1) {
            throw new PSQLException(GT.tr("Object is too large to send over the protocol."),
                PSQLState.NUMERIC_CONSTANT_OUT_OF_RANGE);
          }
          diskOutputStream.flush();
        } finally {
          diskOutputStream.close();
        }
        this.offset = 0;
        this.length = rawData.length + diskLength;
        this.rawData = null;
        this.stream = new FileInputStream(tempFile) {
          /*
           * Usually, closing stream should be done by pgjdbc clients. Here it's an internally
           * managed stream so we need to auto-close it and be sure to delete the temporary file
           * when doing so. Auto-closing will be done when the first occurs: reaching EOF or Garbage
           * Collection
           */
          private boolean _closed = false;
          private int _position = 0;

          /**
           * Check if we should auto-close this stream
           */
          private void checkShouldClose(int readResult) throws IOException {
            if (readResult == -1) {
              close();
            } else {
              _position += readResult;
              if (_position >= length) {
                close();
              }
            }
          }

          public int read(byte[] b) throws IOException {
            if (_closed) {
              return -1;
            }
            int result = super.read(b);
            checkShouldClose(result);
            return result;
          }

          public int read(byte[] b, int off, int len) throws IOException {
            if (_closed) {
              return -1;
            }
            int result = super.read(b, off, len);
            checkShouldClose(result);
            return result;
          }

          public void close() throws IOException {
            if (!_closed) {
              super.close();
              tempFile.delete();
              _closed = true;
            }
          }

          protected void finalize() throws IOException {
            // forcibly close it because super.finalize() may keep the FD open, which may prevent
            // file deletion
            close();
            super.finalize();
          }
        };
      } else {
        this.rawData = rawData;
        this.stream = null;
        this.offset = 0;
        this.length = rawData.length;
      }
    } catch (IOException e) {
      throw new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
          PSQLState.IO_ERROR, e);
    }
  }

  public InputStream getStream() {
    if (stream != null) {
      return stream;
    }

    return new java.io.ByteArrayInputStream(rawData, offset, length);
  }

  public int getLength() {
    return length;
  }

  public int getOffset() {
    return offset;
  }

  public byte[] getBytes() {
    return rawData;
  }

  public String toString() {
    return "<stream of " + length + " bytes>";
  }

  private static int copyStream(InputStream inputStream, OutputStream outputStream, int limit)
      throws IOException {
    int totalLength = 0;
    byte[] buffer = new byte[2048];
    int readLength = inputStream.read(buffer);
    while (readLength > 0) {
      totalLength += readLength;
      outputStream.write(buffer, 0, readLength);
      if (totalLength >= limit) {
        return -1;
      }
      readLength = inputStream.read(buffer);
    }
    return totalLength;
  }

  private final InputStream stream;
  private final byte[] rawData;
  private final int offset;
  private final int length;
}
