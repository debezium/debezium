/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.largeobject;

import org.postgresql.core.BaseConnection;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.fastpath.FastpathArg;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * This class provides the basic methods required to run the interface, plus a pair of methods that
 * provide InputStream and OutputStream classes for this object.
 *
 * <p>
 * Normally, client code would use the getAsciiStream, getBinaryStream, or getUnicodeStream methods
 * in ResultSet, or setAsciiStream, setBinaryStream, or setUnicodeStream methods in
 * PreparedStatement to access Large Objects.
 *
 * <p>
 * However, sometimes lower level access to Large Objects are required, that are not supported by
 * the JDBC specification.
 *
 * <p>
 * Refer to org.postgresql.largeobject.LargeObjectManager on how to gain access to a Large Object,
 * or how to create one.
 *
 * @see org.postgresql.largeobject.LargeObjectManager
 * @see java.sql.ResultSet#getAsciiStream
 * @see java.sql.ResultSet#getBinaryStream
 * @see java.sql.ResultSet#getUnicodeStream
 * @see java.sql.PreparedStatement#setAsciiStream
 * @see java.sql.PreparedStatement#setBinaryStream
 * @see java.sql.PreparedStatement#setUnicodeStream
 */
public class LargeObject {
  /**
   * Indicates a seek from the begining of a file
   */
  public static final int SEEK_SET = 0;

  /**
   * Indicates a seek from the current position
   */
  public static final int SEEK_CUR = 1;

  /**
   * Indicates a seek from the end of a file
   */
  public static final int SEEK_END = 2;

  private final Fastpath fp; // Fastpath API to use
  private final long oid; // OID of this object
  private final int mode; // read/write mode of this object
  private final int fd; // the descriptor of the open large object

  private BlobOutputStream os; // The current output stream

  private boolean closed = false; // true when we are closed

  private BaseConnection conn; // Only initialized when open a LOB with CommitOnClose
  private boolean commitOnClose; // Only initialized when open a LOB with CommitOnClose

  /**
   * This opens a large object.
   *
   * <p>
   * If the object does not exist, then an SQLException is thrown.
   *
   * @param fp FastPath API for the connection to use
   * @param oid of the Large Object to open
   * @param mode Mode of opening the large object
   * @param conn the connection to the database used to access this LOB
   * @param commitOnClose commit the transaction when this LOB will be closed (defined in
   *        LargeObjectManager)
   * @throws SQLException if a database-access error occurs.
   * @see org.postgresql.largeobject.LargeObjectManager
   */
  protected LargeObject(Fastpath fp, long oid, int mode, BaseConnection conn, boolean commitOnClose)
      throws SQLException {
    this.fp = fp;
    this.oid = oid;
    this.mode = mode;
    if (commitOnClose) {
      this.commitOnClose = true;
      this.conn = conn;
    } else {
      this.commitOnClose = false;
    }

    FastpathArg args[] = new FastpathArg[2];
    args[0] = Fastpath.createOIDArg(oid);
    args[1] = new FastpathArg(mode);
    this.fd = fp.getInteger("lo_open", args);
  }

  /**
   * This opens a large object.
   *
   * <p>
   * If the object does not exist, then an SQLException is thrown.
   *
   * @param fp FastPath API for the connection to use
   * @param oid of the Large Object to open
   * @param mode Mode of opening the large object (defined in LargeObjectManager)
   * @throws SQLException if a database-access error occurs.
   * @see org.postgresql.largeobject.LargeObjectManager
   */
  protected LargeObject(Fastpath fp, long oid, int mode) throws SQLException {
    this(fp, oid, mode, null, false);
  }

  public LargeObject copy() throws SQLException {
    return new LargeObject(fp, oid, mode);
  }

  /*
   * Release large object resources during garbage cleanup.
   *
   * This code used to call close() however that was problematic because the scope of the fd is a
   * transaction, thus if commit or rollback was called before garbage collection ran then the call
   * to close would error out with an invalid large object handle. So this method now does nothing
   * and lets the server handle cleanup when it ends the transaction.
   *
   * protected void finalize() throws SQLException { }
   */

  /**
   * @return the OID of this LargeObject
   * @deprecated As of 8.3, replaced by {@link #getLongOID()}
   */
  public int getOID() {
    return (int) oid;
  }

  /**
   * @return the OID of this LargeObject
   */
  public long getLongOID() {
    return oid;
  }

  /**
   * This method closes the object. You must not call methods in this object after this is called.
   *
   * @throws SQLException if a database-access error occurs.
   */
  public void close() throws SQLException {
    if (!closed) {
      // flush any open output streams
      if (os != null) {
        try {
          // we can't call os.close() otherwise we go into an infinite loop!
          os.flush();
        } catch (IOException ioe) {
          throw new PSQLException("Exception flushing output stream", PSQLState.DATA_ERROR, ioe);
        } finally {
          os = null;
        }
      }

      // finally close
      FastpathArg args[] = new FastpathArg[1];
      args[0] = new FastpathArg(fd);
      fp.fastpath("lo_close", args); // true here as we dont care!!
      closed = true;
      if (this.commitOnClose) {
        this.conn.commit();
      }
    }
  }

  /**
   * Reads some data from the object, and return as a byte[] array
   *
   * @param len number of bytes to read
   * @return byte[] array containing data read
   * @throws SQLException if a database-access error occurs.
   */
  public byte[] read(int len) throws SQLException {
    // This is the original method, where the entire block (len bytes)
    // is retrieved in one go.
    FastpathArg args[] = new FastpathArg[2];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(len);
    return fp.getData("loread", args);
  }

  /**
   * Reads some data from the object into an existing array
   *
   * @param buf destination array
   * @param off offset within array
   * @param len number of bytes to read
   * @return the number of bytes actually read
   * @throws SQLException if a database-access error occurs.
   */
  public int read(byte buf[], int off, int len) throws SQLException {
    byte b[] = read(len);
    if (b.length < len) {
      len = b.length;
    }
    System.arraycopy(b, 0, buf, off, len);
    return len;
  }

  /**
   * Writes an array to the object
   *
   * @param buf array to write
   * @throws SQLException if a database-access error occurs.
   */
  public void write(byte buf[]) throws SQLException {
    FastpathArg args[] = new FastpathArg[2];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(buf);
    fp.fastpath("lowrite", args);
  }

  /**
   * Writes some data from an array to the object
   *
   * @param buf destination array
   * @param off offset within array
   * @param len number of bytes to write
   * @throws SQLException if a database-access error occurs.
   */
  public void write(byte buf[], int off, int len) throws SQLException {
    FastpathArg args[] = new FastpathArg[2];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(buf, off, len);
    fp.fastpath("lowrite", args);
  }

  /**
   * Sets the current position within the object.
   *
   * <p>
   * This is similar to the fseek() call in the standard C library. It allows you to have random
   * access to the large object.
   *
   * @param pos position within object
   * @param ref Either SEEK_SET, SEEK_CUR or SEEK_END
   * @throws SQLException if a database-access error occurs.
   */
  public void seek(int pos, int ref) throws SQLException {
    FastpathArg args[] = new FastpathArg[3];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(pos);
    args[2] = new FastpathArg(ref);
    fp.fastpath("lo_lseek", args);
  }

  /**
   * Sets the current position within the object using 64-bit value (9.3+)
   *
   * @param pos position within object
   * @param ref Either SEEK_SET, SEEK_CUR or SEEK_END
   * @throws SQLException if a database-access error occurs.
   */
  public void seek64(long pos, int ref) throws SQLException {
    FastpathArg args[] = new FastpathArg[3];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(pos);
    args[2] = new FastpathArg(ref);
    fp.fastpath("lo_lseek64", args);
  }

  /**
   * Sets the current position within the object.
   *
   * <p>
   * This is similar to the fseek() call in the standard C library. It allows you to have random
   * access to the large object.
   *
   * @param pos position within object from begining
   * @throws SQLException if a database-access error occurs.
   */
  public void seek(int pos) throws SQLException {
    seek(pos, SEEK_SET);
  }

  /**
   * @return the current position within the object
   * @throws SQLException if a database-access error occurs.
   */
  public int tell() throws SQLException {
    FastpathArg args[] = new FastpathArg[1];
    args[0] = new FastpathArg(fd);
    return fp.getInteger("lo_tell", args);
  }

  /**
   * @return the current position within the object
   * @throws SQLException if a database-access error occurs.
   */
  public long tell64() throws SQLException {
    FastpathArg args[] = new FastpathArg[1];
    args[0] = new FastpathArg(fd);
    return fp.getLong("lo_tell64", args);
  }

  /**
   * This method is inefficient, as the only way to find out the size of the object is to seek to
   * the end, record the current position, then return to the original position.
   *
   * <p>
   * A better method will be found in the future.
   *
   * @return the size of the large object
   * @throws SQLException if a database-access error occurs.
   */
  public int size() throws SQLException {
    int cp = tell();
    seek(0, SEEK_END);
    int sz = tell();
    seek(cp, SEEK_SET);
    return sz;
  }

  /**
   * See #size() for information about efficiency.
   *
   * @return the size of the large object
   * @throws SQLException if a database-access error occurs.
   */
  public long size64() throws SQLException {
    long cp = tell64();
    seek64(0, SEEK_END);
    long sz = tell64();
    seek64(cp, SEEK_SET);
    return sz;
  }

  /**
   * Truncates the large object to the given length in bytes. If the number of bytes is larger than
   * the current large object length, the large object will be filled with zero bytes. This method
   * does not modify the current file offset.
   *
   * @param len given length in bytes
   * @throws SQLException if something goes wrong
   */
  public void truncate(int len) throws SQLException {
    FastpathArg args[] = new FastpathArg[2];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(len);
    fp.getInteger("lo_truncate", args);
  }

  /**
   * Truncates the large object to the given length in bytes. If the number of bytes is larger than
   * the current large object length, the large object will be filled with zero bytes. This method
   * does not modify the current file offset.
   *
   * @param len given length in bytes
   * @throws SQLException if something goes wrong
   */
  public void truncate64(long len) throws SQLException {
    FastpathArg args[] = new FastpathArg[2];
    args[0] = new FastpathArg(fd);
    args[1] = new FastpathArg(len);
    fp.getInteger("lo_truncate64", args);
  }

  /**
   * Returns an {@link InputStream} from this object.
   *
   * <p>
   * This {@link InputStream} can then be used in any method that requires an InputStream.
   *
   * @return {@link InputStream} from this object
   * @throws SQLException if a database-access error occurs.
   */
  public InputStream getInputStream() throws SQLException {
    return new BlobInputStream(this, 4096);
  }

  /**
   * Returns an {@link InputStream} from this object, that will limit the amount of data that is
   * visible
   *
   * @param limit maximum number of bytes the resulting stream will serve
   * @return {@link InputStream} from this object
   * @throws SQLException if a database-access error occurs.
   */
  public InputStream getInputStream(long limit) throws SQLException {
    return new BlobInputStream(this, 4096, limit);
  }

  /**
   * Returns an {@link OutputStream} to this object.
   *
   * <p>
   * This OutputStream can then be used in any method that requires an OutputStream.
   *
   * @return {@link OutputStream} from this object
   * @throws SQLException if a database-access error occurs.
   */
  public OutputStream getOutputStream() throws SQLException {
    if (os == null) {
      os = new BlobOutputStream(this, 4096);
    }
    return os;
  }
}
