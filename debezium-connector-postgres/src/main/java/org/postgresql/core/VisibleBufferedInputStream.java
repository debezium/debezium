/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2006-2014, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */

package org.postgresql.core;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A faster version of BufferedInputStream. Does no synchronisation and allows direct access to the
 * used byte[] buffer.
 *
 * @author Mikko Tiihonen
 */
public class VisibleBufferedInputStream extends InputStream {

  /**
   * If a direct read to byte array is called that would require a smaller read from the wrapped
   * stream that MINIMUM_READ then first fill the buffer and serve the bytes from there. Larger
   * reads are directly done to the provided byte array.
   */
  private static final int MINIMUM_READ = 1024;

  /**
   * In how large spans is the C string zero-byte scanned.
   */
  private static final int STRING_SCAN_SPAN = 1024;

  /**
   * The wrapped input stream.
   */
  private final InputStream wrapped;

  /**
   * The buffer.
   */
  private byte[] buffer;

  /**
   * Current read position in the buffer.
   */
  private int index;

  /**
   * How far is the buffer filled with valid data.
   */
  private int endIndex;

  /**
   * Creates a new buffer around the given stream.
   *
   * @param in The stream to buffer.
   * @param bufferSize The initial size of the buffer.
   */
  public VisibleBufferedInputStream(InputStream in, int bufferSize) {
    wrapped = in;
    buffer = new byte[bufferSize < MINIMUM_READ ? MINIMUM_READ : bufferSize];
  }

  /**
   * {@inheritDoc}
   */
  public int read() throws IOException {
    if (ensureBytes(1)) {
      return buffer[index++] & 0xFF;
    }
    return -1;
  }

  /**
   * Reads a byte from the buffer without advancing the index pointer.
   *
   * @return byte from the buffer without advancing the index pointer
   * @throws IOException if something wrong happens
   */
  public int peek() throws IOException {
    if (ensureBytes(1)) {
      return buffer[index] & 0xFF;
    }
    return -1;
  }

  /**
   * Reads byte from the buffer without any checks. This method never reads from the underlaying
   * stream. Before calling this method the {@link #ensureBytes} method must have been called.
   *
   * @return The next byte from the buffer.
   * @throws ArrayIndexOutOfBoundsException If ensureBytes was not called to make sure the buffer
   *         contains the byte.
   */
  public byte readRaw() {
    return buffer[index++];
  }

  /**
   * Ensures that the buffer contains at least n bytes. This method invalidates the buffer and index
   * fields.
   *
   * @param n The amount of bytes to ensure exists in buffer
   * @return true if required bytes are available and false if EOF
   * @throws IOException If reading of the wrapped stream failed.
   */
  public boolean ensureBytes(int n) throws IOException {
    int required = n - endIndex + index;
    while (required > 0) {
      if (!readMore(required)) {
        return false;
      }
      required = n - endIndex + index;
    }
    return true;
  }

  /**
   * Reads more bytes into the buffer.
   *
   * @param wanted How much should be at least read.
   * @return True if at least some bytes were read.
   * @throws IOException If reading of the wrapped stream failed.
   */
  private boolean readMore(int wanted) throws IOException {
    if (endIndex == index) {
      index = 0;
      endIndex = 0;
    }
    int canFit = buffer.length - endIndex;
    if (canFit < wanted) {
      // would the wanted bytes fit if we compacted the buffer
      // and still leave some slack
      if (index + canFit > wanted + MINIMUM_READ) {
        compact();
      } else {
        doubleBuffer();
      }
      canFit = buffer.length - endIndex;
    }
    int read = wrapped.read(buffer, endIndex, canFit);
    if (read < 0) {
      return false;
    }
    endIndex += read;
    return true;
  }

  /**
   * Doubles the size of the buffer.
   */
  private void doubleBuffer() {
    byte[] buf = new byte[buffer.length * 2];
    moveBufferTo(buf);
    buffer = buf;
  }

  /**
   * Compacts the unread bytes of the buffer to the beginning of the buffer.
   */
  private void compact() {
    moveBufferTo(buffer);
  }

  /**
   * Moves bytes from the buffer to the begining of the destination buffer. Also sets the index and
   * endIndex variables.
   *
   * @param dest The destination buffer.
   */
  private void moveBufferTo(byte[] dest) {
    int size = endIndex - index;
    System.arraycopy(buffer, index, dest, 0, size);
    index = 0;
    endIndex = size;
  }

  /**
   * {@inheritDoc}
   */
  public int read(byte to[], int off, int len) throws IOException {
    if ((off | len | (off + len) | (to.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    // if the read would go to wrapped stream, but would result
    // in a small read then try read to the buffer instead
    int avail = endIndex - index;
    if (len - avail < MINIMUM_READ) {
      ensureBytes(len);
      avail = endIndex - index;
    }

    // first copy from buffer
    if (avail > 0) {
      if (len <= avail) {
        System.arraycopy(buffer, index, to, off, len);
        index += len;
        return len;
      }
      System.arraycopy(buffer, index, to, off, avail);
      len -= avail;
      off += avail;
    }
    int read = avail;

    // good place to reset index because the buffer is fully drained
    index = 0;
    endIndex = 0;

    // then directly from wrapped stream
    do {
      int r = wrapped.read(to, off, len);
      if (r <= 0) {
        return (read == 0) ? r : read;
      }
      read += r;
      off += r;
      len -= r;
    } while (len > 0);

    return read;
  }

  /**
   * {@inheritDoc}
   */
  public long skip(long n) throws IOException {
    int avail = endIndex - index;
    if (avail >= n) {
      index += n;
      return n;
    }
    n -= avail;
    index = 0;
    endIndex = 0;
    return avail + wrapped.skip(n);
  }

  /**
   * {@inheritDoc}
   */
  public int available() throws IOException {
    int avail = endIndex - index;
    return avail > 0 ? avail : wrapped.available();
  }

  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
    wrapped.close();
  }

  /**
   * Returns direct handle to the used buffer. Use the {@link #ensureBytes} to prefill required
   * bytes the buffer and {@link #getIndex} to fetch the current position of the buffer.
   *
   * @return The underlaying buffer.
   */
  public byte[] getBuffer() {
    return buffer;
  }

  /**
   * Returns the current read position in the buffer.
   *
   * @return the current read position in the buffer.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Scans the length of the next null terminated string (C-style string) from the stream.
   *
   * @return The length of the next null terminated string.
   * @throws IOException If reading of stream fails.
   * @throws EOFException If the stream did not contain any null terminators.
   */
  public int scanCStringLength() throws IOException {
    int pos = index;
    while (true) {
      while (pos < endIndex) {
        if (buffer[pos++] == '\0') {
          return pos - index;
        }
      }
      if (!readMore(STRING_SCAN_SPAN)) {
        throw new EOFException();
      }
      pos = index;
    }
  }
}
