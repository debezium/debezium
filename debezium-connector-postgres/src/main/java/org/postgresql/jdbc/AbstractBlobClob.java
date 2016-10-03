/*-------------------------------------------------------------------------
*
* Copyright (c) 2005-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * This class holds all of the methods common to both Blobs and Clobs.
 *
 * @author <a href="mailto:mike@middlesoft.co.uk">Michael Barker</a>
 */
public abstract class AbstractBlobClob {
  protected BaseConnection conn;

  private LargeObject currentLo;
  private boolean currentLoIsWriteable;
  private boolean support64bit;


  /**
   * We create separate LargeObjects for methods that use streams so they won't interfere with each
   * other.
   */
  private ArrayList<LargeObject> subLOs;

  private final long oid;

  public AbstractBlobClob(BaseConnection conn, long oid) throws SQLException {
    this.conn = conn;
    this.oid = oid;
    this.currentLo = null;
    this.currentLoIsWriteable = false;

    support64bit = conn.haveMinimumServerVersion(90300);

    subLOs = new ArrayList<LargeObject>();
  }

  public synchronized void free() throws SQLException {
    if (currentLo != null) {
      currentLo.close();
      currentLo = null;
      currentLoIsWriteable = false;
    }
    for (LargeObject subLO : subLOs) {
      subLO.close();
    }
    subLOs = null;
  }

  /**
   * For Blobs this should be in bytes while for Clobs it should be in characters. Since we really
   * haven't figured out how to handle character sets for Clobs the current implementation uses
   * bytes for both Blobs and Clobs.
   *
   * @param len maximum length
   * @throws SQLException if operation fails
   */
  public synchronized void truncate(long len) throws SQLException {
    checkFreed();
    if (!conn.haveMinimumServerVersion(ServerVersion.v8_3)) {
      throw new PSQLException(
          GT.tr("Truncation of large objects is only implemented in 8.3 and later servers."),
          PSQLState.NOT_IMPLEMENTED);
    }

    if (len < 0) {
      throw new PSQLException(GT.tr("Cannot truncate LOB to a negative length."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    if (len > Integer.MAX_VALUE) {
      if (support64bit) {
        getLo(true).truncate64(len);
      } else {
        throw new PSQLException(GT.tr("PostgreSQL LOBs can only index to: {0}", Integer.MAX_VALUE),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    } else {
      getLo(true).truncate((int) len);
    }
  }

  public synchronized long length() throws SQLException {
    checkFreed();
    if (support64bit) {
      return getLo(false).size64();
    } else {
      return getLo(false).size();
    }
  }

  public synchronized byte[] getBytes(long pos, int length) throws SQLException {
    assertPosition(pos);
    getLo(false).seek((int) (pos - 1), LargeObject.SEEK_SET);
    return getLo(false).read(length);
  }


  public synchronized InputStream getBinaryStream() throws SQLException {
    checkFreed();
    LargeObject subLO = getLo(false).copy();
    addSubLO(subLO);
    subLO.seek(0, LargeObject.SEEK_SET);
    return subLO.getInputStream();
  }

  public synchronized OutputStream setBinaryStream(long pos) throws SQLException {
    assertPosition(pos);
    LargeObject subLO = getLo(true).copy();
    addSubLO(subLO);
    subLO.seek((int) (pos - 1));
    return subLO.getOutputStream();
  }

  /**
   * Iterate over the buffer looking for the specified pattern
   *
   * @param pattern A pattern of bytes to search the blob for
   * @param start The position to start reading from
   * @return position of the specified pattern
   * @throws SQLException if something wrong happens
   */
  public synchronized long position(byte[] pattern, long start) throws SQLException {
    assertPosition(start, pattern.length);

    int position = 1;
    int patternIdx = 0;
    long result = -1;
    int tmpPosition = 1;

    for (LOIterator i = new LOIterator(start - 1); i.hasNext(); position++) {
      byte b = i.next();
      if (b == pattern[patternIdx]) {
        if (patternIdx == 0) {
          tmpPosition = position;
        }
        patternIdx++;
        if (patternIdx == pattern.length) {
          result = tmpPosition;
          break;
        }
      } else {
        patternIdx = 0;
      }
    }

    return result;
  }

  /**
   * Iterates over a large object returning byte values. Will buffer the data from the large object.
   */
  private class LOIterator {
    private static final int BUFFER_SIZE = 8096;
    private byte buffer[] = new byte[BUFFER_SIZE];
    private int idx = BUFFER_SIZE;
    private int numBytes = BUFFER_SIZE;

    public LOIterator(long start) throws SQLException {
      getLo(false).seek((int) start);
    }

    public boolean hasNext() throws SQLException {
      boolean result;
      if (idx < numBytes) {
        result = true;
      } else {
        numBytes = getLo(false).read(buffer, 0, BUFFER_SIZE);
        idx = 0;
        result = (numBytes > 0);
      }
      return result;
    }

    private byte next() {
      return buffer[idx++];
    }
  }


  /**
   * This is simply passing the byte value of the pattern Blob
   *
   * @param pattern search pattern
   * @param start start position
   * @return position of given pattern
   * @throws SQLException if something goes wrong
   */
  public synchronized long position(Blob pattern, long start) throws SQLException {
    return position(pattern.getBytes(1, (int) pattern.length()), start);
  }

  /**
   * Throws an exception if the pos value exceeds the max value by which the large object API can
   * index.
   *
   * @param pos Position to write at.
   * @throws SQLException if something goes wrong
   */
  protected void assertPosition(long pos) throws SQLException {
    assertPosition(pos, 0);
  }

  /**
   * Throws an exception if the pos value exceeds the max value by which the large object API can
   * index.
   *
   * @param pos Position to write at.
   * @param len number of bytes to write.
   * @throws SQLException if something goes wrong
   */
  protected void assertPosition(long pos, long len) throws SQLException {
    checkFreed();
    if (pos < 1) {
      throw new PSQLException(GT.tr("LOB positioning offsets start at 1."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    if (pos + len - 1 > Integer.MAX_VALUE) {
      throw new PSQLException(GT.tr("PostgreSQL LOBs can only index to: {0}", Integer.MAX_VALUE),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  /**
   * Checks that this LOB hasn't been free()d already.
   *
   * @throws SQLException if LOB has been freed.
   */
  protected void checkFreed() throws SQLException {
    if (subLOs == null) {
      throw new PSQLException(GT.tr("free() was called on this LOB previously"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
  }

  protected synchronized LargeObject getLo(boolean forWrite) throws SQLException {


    if (this.currentLo != null) {
      if (forWrite && !currentLoIsWriteable) {
        // Reopen the stream in read-write, at the same pos.
        int currentPos = this.currentLo.tell();

        LargeObjectManager lom = conn.getLargeObjectAPI();
        LargeObject newLo = lom.open(oid, LargeObjectManager.READWRITE);
        this.subLOs.add(this.currentLo);
        this.currentLo = newLo;

        if (currentPos != 0) {
          this.currentLo.seek(currentPos);
        }
      }

      return this.currentLo;
    }
    LargeObjectManager lom = conn.getLargeObjectAPI();
    currentLo = lom.open(oid, forWrite ? LargeObjectManager.READWRITE : LargeObjectManager.READ);
    currentLoIsWriteable = forWrite;
    return currentLo;
  }

  protected void addSubLO(LargeObject subLO) {
    subLOs.add(subLO);
  }
}
