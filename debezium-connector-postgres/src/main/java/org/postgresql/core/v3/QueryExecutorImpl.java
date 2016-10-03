/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
* Copyright (c) 2004, Open Cloud Limited.
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.core.v3;

import org.postgresql.PGProperty;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyOperation;
import org.postgresql.copy.CopyOut;
import org.postgresql.core.Encoding;
import org.postgresql.core.EncodingPredictor;
import org.postgresql.core.Field;
import org.postgresql.core.Logger;
import org.postgresql.core.NativeQuery;
import org.postgresql.core.Oid;
import org.postgresql.core.PGBindException;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Parser;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.QueryExecutorBase;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandler;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.ResultHandlerDelegate;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.SqlCommandType;
import org.postgresql.core.TransactionState;
import org.postgresql.core.Utils;
import org.postgresql.core.v3.replication.V3ReplicationProtocol;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.BatchResultHandler;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.PSQLWarning;
import org.postgresql.util.ServerErrorMessage;

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

/**
 * QueryExecutor implementation for the V3 protocol.
 */
public class QueryExecutorImpl extends QueryExecutorBase {
  /**
   * TimeZone of the current connection (TimeZone backend parameter)
   */
  private TimeZone timeZone;

  /**
   * application_name connection property
   */
  private String applicationName;

  /**
   * True if server uses integers for date and time fields. False if server uses double.
   */
  private boolean integerDateTimes;

  /**
   * Bit set that has a bit set for each oid which should be received using binary format.
   */
  private final Set<Integer> useBinaryReceiveForOids = new HashSet<Integer>();

  /**
   * Bit set that has a bit set for each oid which should be sent using binary format.
   */
  private final Set<Integer> useBinarySendForOids = new HashSet<Integer>();

  /**
   * This is a fake query object so processResults can distinguish "ReadyForQuery" messages
   * from Sync messages vs from simple execute (aka 'Q')
   */
  private final SimpleQuery sync = (SimpleQuery) createQuery("SYNC", false, true).query;

  private short deallocateEpoch;

  /**
   * This caches the latest observed {@code set search_path} query so the reset of prepared
   * statement cache can be skipped if using repeated calls for the same {@code set search_path}
   * value.
   */
  private String lastSetSearchPathQuery;

  private final ReplicationProtocol replicationProtocol;

  public QueryExecutorImpl(PGStream pgStream, String user, String database,
      int cancelSignalTimeout, Properties info, Logger logger) throws SQLException, IOException {
    super(logger, pgStream, user, database, cancelSignalTimeout, info);

    this.allowEncodingChanges = PGProperty.ALLOW_ENCODING_CHANGES.getBoolean(info);
    this.replicationProtocol = new V3ReplicationProtocol(this, pgStream, logger);
    readStartupMessages();
  }

  @Override
  public int getProtocolVersion() {
    return 3;
  }

  /**
   * Supplement to synchronization of public methods on current QueryExecutor.
   *
   * Necessary for keeping the connection intact between calls to public methods sharing a state
   * such as COPY subprotocol. waitOnLock() must be called at beginning of each connection access
   * point.
   *
   * Public methods sharing that state must then be synchronized among themselves. Normal method
   * synchronization typically suffices for that.
   *
   * See notes on related methods as well as currentCopy() below.
   */
  private Object lockedFor = null;

  /**
   * Obtain lock over this connection for given object, blocking to wait if necessary.
   *
   * @param obtainer object that gets the lock. Normally current thread.
   * @throws PSQLException when already holding the lock or getting interrupted.
   */
  private void lock(Object obtainer) throws PSQLException {
    if (lockedFor == obtainer) {
      throw new PSQLException(GT.tr("Tried to obtain lock while already holding it"),
          PSQLState.OBJECT_NOT_IN_STATE);

    }
    waitOnLock();
    lockedFor = obtainer;
  }

  /**
   * Release lock on this connection presumably held by given object.
   *
   * @param holder object that holds the lock. Normally current thread.
   * @throws PSQLException when this thread does not hold the lock
   */
  private void unlock(Object holder) throws PSQLException {
    if (lockedFor != holder) {
      throw new PSQLException(GT.tr("Tried to break lock on database connection"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
    lockedFor = null;
    this.notify();
  }

  /**
   * Wait until our lock is released. Execution of a single synchronized method can then continue
   * without further ado. Must be called at beginning of each synchronized public method.
   */
  private void waitOnLock() throws PSQLException {
    while (lockedFor != null) {
      try {
        this.wait();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new PSQLException(
            GT.tr("Interrupted while waiting to obtain lock on database connection"),
            PSQLState.OBJECT_NOT_IN_STATE, ie);
      }
    }
  }

  /**
   * @param holder object assumed to hold the lock
   * @return whether given object actually holds the lock
   */
  boolean hasLock(Object holder) {
    return lockedFor == holder;
  }

  //
  // Query parsing
  //

  public Query createSimpleQuery(String sql) throws SQLException {
    List<NativeQuery> queries = Parser.parseJdbcSql(sql,
        getStandardConformingStrings(), false, true,
        isReWriteBatchedInsertsEnabled());
    return wrap(queries);
  }

  @Override
  public Query wrap(List<NativeQuery> queries) {
    if (queries.isEmpty()) {
      // Empty query
      return EMPTY_QUERY;
    }
    if (queries.size() == 1) {
      NativeQuery firstQuery = queries.get(0);
      if (isReWriteBatchedInsertsEnabled()
          && firstQuery.getCommand().isBatchedReWriteCompatible()) {
        int valuesBraceOpenPosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceOpenPosition();
        int valuesBraceClosePosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceClosePosition();
        return new BatchedQuery(firstQuery, this, valuesBraceOpenPosition,
            valuesBraceClosePosition, isColumnSanitiserDisabled());
      } else {
        return new SimpleQuery(firstQuery, this, isColumnSanitiserDisabled());
      }
    }

    // Multiple statements.
    SimpleQuery[] subqueries = new SimpleQuery[queries.size()];
    int[] offsets = new int[subqueries.length];
    int offset = 0;
    for (int i = 0; i < queries.size(); ++i) {
      NativeQuery nativeQuery = queries.get(i);
      offsets[i] = offset;
      subqueries[i] = new SimpleQuery(nativeQuery, this, isColumnSanitiserDisabled());
      offset += nativeQuery.bindPositions.length;
    }

    return new CompositeQuery(subqueries, offsets);
  }

  //
  // Query execution
  //

  private int updateQueryMode(int flags) {
    switch (getPreferQueryMode()) {
      case SIMPLE:
        return flags | QUERY_EXECUTE_AS_SIMPLE;
      case EXTENDED:
        return flags & ~QUERY_EXECUTE_AS_SIMPLE;
      default:
        return flags;
    }
  }

  public synchronized void execute(Query query, ParameterList parameters, ResultHandler handler,
      int maxRows, int fetchSize, int flags) throws SQLException {
    waitOnLock();
    if (logger.logDebug()) {
      logger.debug("simple execute, handler=" + handler + ", maxRows=" + maxRows + ", fetchSize="
          + fetchSize + ", flags=" + flags);
    }

    if (parameters == null) {
      parameters = SimpleQuery.NO_PARAMETERS;
    }

    flags = updateQueryMode(flags);

    boolean describeOnly = (QUERY_DESCRIBE_ONLY & flags) != 0;

    ((V3ParameterList) parameters).convertFunctionOutParameters();

    // Check parameters are all set..
    if (!describeOnly) {
      ((V3ParameterList) parameters).checkAllParametersSet();
    }

    boolean autosave = false;
    try {
      try {
        handler = sendQueryPreamble(handler, flags);
        autosave = sendAutomaticSavepoint(query, flags);
        sendQuery(query, (V3ParameterList) parameters, maxRows, fetchSize, flags,
            handler, null);
        if ((flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0) {
          // Sync message is not required for 'Q' execution as 'Q' ends with ReadyForQuery message
          // on its own
        } else {
          sendSync();
        }
        processResults(handler, flags);
        estimatedReceiveBufferBytes = 0;
      } catch (PGBindException se) {
        // There are three causes of this error, an
        // invalid total Bind message length, a
        // BinaryStream that cannot provide the amount
        // of data claimed by the length arugment, and
        // a BinaryStream that throws an Exception
        // when reading.
        //
        // We simply do not send the Execute message
        // so we can just continue on as if nothing
        // has happened. Perhaps we need to
        // introduce an error here to force the
        // caller to rollback if there is a
        // transaction in progress?
        //
        sendSync();
        processResults(handler, flags);
        estimatedReceiveBufferBytes = 0;
        handler
            .handleError(new PSQLException(GT.tr("Unable to bind parameter values for statement."),
                PSQLState.INVALID_PARAMETER_VALUE, se.getIOException()));
      }
    } catch (IOException e) {
      abort();
      handler.handleError(
          new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
              PSQLState.CONNECTION_FAILURE, e));
    }

    try {
      handler.handleCompletion();
    } catch (SQLException e) {
      rollbackIfRequired(autosave, e);
    }
  }

  private boolean sendAutomaticSavepoint(Query query, int flags) throws IOException {
    if (((flags & QueryExecutor.QUERY_SUPPRESS_BEGIN) == 0
        || getTransactionState() == TransactionState.OPEN)
        && query != restoreToAutoSave
        && getAutoSave() != AutoSave.NEVER
        // If query has no resulting fields, it cannot fail with 'cached plan must not change result type'
        // thus no need to set a safepoint before such query
        && (getAutoSave() == AutoSave.ALWAYS
        // If CompositeQuery is observed, just assume it might fail and set the savepoint
        || !(query instanceof SimpleQuery)
        || ((SimpleQuery) query).getFields() != null)) {
      sendOneQuery(autoSaveQuery, SimpleQuery.NO_PARAMETERS, 1, 0,
          updateQueryMode(QUERY_NO_RESULTS | QUERY_NO_METADATA)
              // PostgreSQL does not support bind, exec, simple, sync message flow,
              // so we force autosavepoint to use simple if the main query is using simple
              | (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE));
      return true;
    }
    return false;
  }

  private void rollbackIfRequired(boolean autosave, SQLException e) throws SQLException {
    if (autosave
        && getTransactionState() == TransactionState.FAILED
        && (getAutoSave() == AutoSave.ALWAYS || willHealOnRetry(e))) {
      try {
        execute(restoreToAutoSave, SimpleQuery.NO_PARAMETERS, new ResultHandlerDelegate(null),
            1, 0, updateQueryMode(QUERY_NO_RESULTS | QUERY_NO_METADATA));
      } catch (SQLException e2) {
        // That's O(N), sorry
        e.setNextException(e2);
      }
    }
    throw e;
  }

  // Deadlock avoidance:
  //
  // It's possible for the send and receive streams to get "deadlocked" against each other since
  // we do not have a separate thread. The scenario is this: we have two streams:
  //
  // driver -> TCP buffering -> server
  // server -> TCP buffering -> driver
  //
  // The server behaviour is roughly:
  // while true:
  // read message
  // execute message
  // write results
  //
  // If the server -> driver stream has a full buffer, the write will block.
  // If the driver is still writing when this happens, and the driver -> server
  // stream also fills up, we deadlock: the driver is blocked on write() waiting
  // for the server to read some more data, and the server is blocked on write()
  // waiting for the driver to read some more data.
  //
  // To avoid this, we guess at how much response data we can request from the
  // server before the server -> driver stream's buffer is full (MAX_BUFFERED_RECV_BYTES).
  // This is the point where the server blocks on write and stops reading data. If we
  // reach this point, we force a Sync message and read pending data from the server
  // until ReadyForQuery, then go back to writing more queries unless we saw an error.
  //
  // This is not 100% reliable -- it's only done in the batch-query case and only
  // at a reasonably high level (per query, not per message), and it's only an estimate
  // -- so it might break. To do it correctly in all cases would seem to require a
  // separate send or receive thread as we can only do the Sync-and-read-results
  // operation at particular points, and also as we don't really know how much data
  // the server is sending.
  //
  // Our message size estimation is coarse, and disregards asynchronous
  // notifications, warnings/info/debug messages, etc, so the repsonse size may be
  // quite different from the 250 bytes assumed here even for queries that don't
  // return data.
  //
  // See github issue #194 and #195 .
  //
  // Assume 64k server->client buffering, which is extremely conservative. A typical
  // system will have 200kb or more of buffers for its receive buffers, and the sending
  // system will typically have the same on the send side, giving us 400kb or to work
  // with. (We could check Java's receive buffer size, but prefer to assume a very
  // conservative buffer instead, and we don't know how big the server's send
  // buffer is.)
  //
  private static final int MAX_BUFFERED_RECV_BYTES = 64000;
  private static final int NODATA_QUERY_RESPONSE_SIZE_BYTES = 250;

  public synchronized void execute(Query[] queries, ParameterList[] parameterLists,
      BatchResultHandler batchHandler, int maxRows, int fetchSize, int flags) throws SQLException {
    waitOnLock();
    if (logger.logDebug()) {
      logger.debug("batch execute " + queries.length + " queries, handler=" + batchHandler + ", maxRows="
          + maxRows + ", fetchSize=" + fetchSize + ", flags=" + flags);
    }

    flags = updateQueryMode(flags);

    boolean describeOnly = (QUERY_DESCRIBE_ONLY & flags) != 0;
    // Check parameters and resolve OIDs.
    if (!describeOnly) {
      for (ParameterList parameterList : parameterLists) {
        if (parameterList != null) {
          ((V3ParameterList) parameterList).checkAllParametersSet();
        }
      }
    }

    boolean autosave = false;
    ResultHandler handler = batchHandler;
    try {
      handler = sendQueryPreamble(batchHandler, flags);
      autosave = sendAutomaticSavepoint(queries[0], flags);
      estimatedReceiveBufferBytes = 0;

      for (int i = 0; i < queries.length; ++i) {
        Query query = queries[i];
        V3ParameterList parameters = (V3ParameterList) parameterLists[i];
        if (parameters == null) {
          parameters = SimpleQuery.NO_PARAMETERS;
        }

        sendQuery(query, parameters, maxRows, fetchSize, flags, handler, batchHandler);

        if (handler.getException() != null) {
          break;
        }
      }

      if (handler.getException() == null) {
        if ((flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0) {
          // Sync message is not required for 'Q' execution as 'Q' ends with ReadyForQuery message
          // on its own
        } else {
          sendSync();
        }
        processResults(handler, flags);
        estimatedReceiveBufferBytes = 0;
      }
    } catch (IOException e) {
      abort();
      handler.handleError(
          new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
              PSQLState.CONNECTION_FAILURE, e));
    }

    try {
      handler.handleCompletion();
    } catch (SQLException e) {
      rollbackIfRequired(autosave, e);
    }
  }

  private ResultHandler sendQueryPreamble(final ResultHandler delegateHandler, int flags)
      throws IOException {
    // First, send CloseStatements for finalized SimpleQueries that had statement names assigned.
    processDeadParsedQueries();
    processDeadPortals();

    // Send BEGIN on first statement in transaction.
    if ((flags & QueryExecutor.QUERY_SUPPRESS_BEGIN) != 0
        || getTransactionState() != TransactionState.IDLE) {
      return delegateHandler;
    }

    int beginFlags = QueryExecutor.QUERY_NO_METADATA;
    if ((flags & QueryExecutor.QUERY_ONESHOT) != 0) {
      beginFlags |= QueryExecutor.QUERY_ONESHOT;
    }

    beginFlags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;

    beginFlags = updateQueryMode(beginFlags);

    sendOneQuery(beginTransactionQuery, SimpleQuery.NO_PARAMETERS, 0, 0, beginFlags);

    // Insert a handler that intercepts the BEGIN.
    return new ResultHandlerDelegate(delegateHandler) {
      private boolean sawBegin = false;

      public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
          ResultCursor cursor) {
        if (sawBegin) {
          super.handleResultRows(fromQuery, fields, tuples, cursor);
        }
      }

      public void handleCommandStatus(String status, int updateCount, long insertOID) {
        if (!sawBegin) {
          sawBegin = true;
          if (!status.equals("BEGIN")) {
            handleError(new PSQLException(GT.tr("Expected command status BEGIN, got {0}.", status),
                PSQLState.PROTOCOL_VIOLATION));
          }
        } else {
          super.handleCommandStatus(status, updateCount, insertOID);
        }
      }
    };
  }

  //
  // Fastpath
  //

  public synchronized byte[] fastpathCall(int fnid, ParameterList parameters, boolean suppressBegin)
      throws SQLException {
    waitOnLock();
    if (!suppressBegin) {
      doSubprotocolBegin();
    }
    try {
      sendFastpathCall(fnid, (SimpleParameterList) parameters);
      return receiveFastpathResult();
    } catch (IOException ioe) {
      abort();
      throw new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  public void doSubprotocolBegin() throws SQLException {
    if (getTransactionState() == TransactionState.IDLE) {

      if (logger.logDebug()) {
        logger.debug("Issuing BEGIN before fastpath or copy call.");
      }

      ResultHandler handler = new ResultHandlerBase() {
        private boolean sawBegin = false;

        public void handleCommandStatus(String status, int updateCount, long insertOID) {
          if (!sawBegin) {
            if (!status.equals("BEGIN")) {
              handleError(
                  new PSQLException(GT.tr("Expected command status BEGIN, got {0}.", status),
                      PSQLState.PROTOCOL_VIOLATION));
            }
            sawBegin = true;
          } else {
            handleError(new PSQLException(GT.tr("Unexpected command status: {0}.", status),
                PSQLState.PROTOCOL_VIOLATION));
          }
        }

        public void handleWarning(SQLWarning warning) {
          // we don't want to ignore warnings and it would be tricky
          // to chain them back to the connection, so since we don't
          // expect to get them in the first place, we just consider
          // them errors.
          handleError(warning);
        }
      };

      try {
        sendOneQuery(beginTransactionQuery, SimpleQuery.NO_PARAMETERS, 0, 0,
            QueryExecutor.QUERY_NO_METADATA);
        sendSync();
        processResults(handler, 0);
        estimatedReceiveBufferBytes = 0;
      } catch (IOException ioe) {
        throw new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
            PSQLState.CONNECTION_FAILURE, ioe);
      }
    }

  }

  public ParameterList createFastpathParameters(int count) {
    return new SimpleParameterList(count, this);
  }

  private void sendFastpathCall(int fnid, SimpleParameterList params)
      throws SQLException, IOException {
    if (logger.logDebug()) {
      logger.debug(" FE=> FunctionCall(" + fnid + ", " + params.getParameterCount() + " params)");
    }

    //
    // Total size = 4 (length)
    // + 4 (function OID)
    // + 2 (format code count) + N * 2 (format codes)
    // + 2 (parameter count) + encodedSize (parameters)
    // + 2 (result format)

    int paramCount = params.getParameterCount();
    int encodedSize = 0;
    for (int i = 1; i <= paramCount; ++i) {
      if (params.isNull(i)) {
        encodedSize += 4;
      } else {
        encodedSize += 4 + params.getV3Length(i);
      }
    }


    pgStream.sendChar('F');
    pgStream.sendInteger4(4 + 4 + 2 + 2 * paramCount + 2 + encodedSize + 2);
    pgStream.sendInteger4(fnid);
    pgStream.sendInteger2(paramCount);
    for (int i = 1; i <= paramCount; ++i) {
      pgStream.sendInteger2(params.isBinary(i) ? 1 : 0);
    }
    pgStream.sendInteger2(paramCount);
    for (int i = 1; i <= paramCount; i++) {
      if (params.isNull(i)) {
        pgStream.sendInteger4(-1);
      } else {
        pgStream.sendInteger4(params.getV3Length(i)); // Parameter size
        params.writeV3Value(i, pgStream);
      }
    }
    pgStream.sendInteger2(1); // Binary result format
    pgStream.flush();
  }

  public synchronized void processNotifies() throws SQLException {
    waitOnLock();
    // Asynchronous notifies only arrive when we are not in a transaction
    if (getTransactionState() != TransactionState.IDLE) {
      return;
    }

    try {
      while (pgStream.hasMessagePending()) {
        int c = pgStream.receiveChar();
        switch (c) {
          case 'A': // Asynchronous Notify
            receiveAsyncNotify();
            break;
          case 'E':
            // Error Response (response to pretty much everything; backend then skips until Sync)
            throw receiveErrorResponse();
          case 'N': // Notice Response (warnings / info)
            SQLWarning warning = receiveNoticeResponse();
            addWarning(warning);
            break;
          default:
            throw new PSQLException(GT.tr("Unknown Response Type {0}.", (char) c),
                PSQLState.CONNECTION_FAILURE);
        }
      }
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  private byte[] receiveFastpathResult() throws IOException, SQLException {
    boolean endQuery = false;
    SQLException error = null;
    byte[] returnValue = null;

    while (!endQuery) {
      int c = pgStream.receiveChar();
      switch (c) {
        case 'A': // Asynchronous Notify
          receiveAsyncNotify();
          break;

        case 'E':
          // Error Response (response to pretty much everything; backend then skips until Sync)
          SQLException newError = receiveErrorResponse();
          if (error == null) {
            error = newError;
          } else {
            error.setNextException(newError);
          }
          // keep processing
          break;

        case 'N': // Notice Response (warnings / info)
          SQLWarning warning = receiveNoticeResponse();
          addWarning(warning);
          break;

        case 'Z': // Ready For Query (eventual response to Sync)
          receiveRFQ();
          endQuery = true;
          break;

        case 'V': // FunctionCallResponse
          int msgLen = pgStream.receiveInteger4();
          int valueLen = pgStream.receiveInteger4();

          if (logger.logDebug()) {
            logger.debug(" <=BE FunctionCallResponse(" + valueLen + " bytes)");
          }

          if (valueLen != -1) {
            byte buf[] = new byte[valueLen];
            pgStream.receive(buf, 0, valueLen);
            returnValue = buf;
          }

          break;

        default:
          throw new PSQLException(GT.tr("Unknown Response Type {0}.", (char) c),
              PSQLState.CONNECTION_FAILURE);
      }

    }

    // did we get an error during this query?
    if (error != null) {
      throw error;
    }

    return returnValue;
  }

  //
  // Copy subprotocol implementation
  //

  /**
   * Sends given query to BE to start, initialize and lock connection for a CopyOperation.
   *
   * @param sql COPY FROM STDIN / COPY TO STDOUT statement
   * @return CopyIn or CopyOut operation object
   * @throws SQLException on failure
   */
  public synchronized CopyOperation startCopy(String sql, boolean suppressBegin)
      throws SQLException {
    waitOnLock();
    if (!suppressBegin) {
      doSubprotocolBegin();
    }
    byte buf[] = Utils.encodeUTF8(sql);

    try {
      if (logger.logDebug()) {
        logger.debug(" FE=> Query(CopyStart)");
      }

      pgStream.sendChar('Q');
      pgStream.sendInteger4(buf.length + 4 + 1);
      pgStream.send(buf);
      pgStream.sendChar(0);
      pgStream.flush();

      return processCopyResults(null, true);
      // expect a CopyInResponse or CopyOutResponse to our query above
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when starting copy"),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  /**
   * Locks connection and calls initializer for a new CopyOperation Called via startCopy ->
   * processCopyResults
   *
   * @param op an unitialized CopyOperation
   * @throws SQLException on locking failure
   * @throws IOException on database connection failure
   */
  private synchronized void initCopy(CopyOperationImpl op) throws SQLException, IOException {
    pgStream.receiveInteger4(); // length not used
    int rowFormat = pgStream.receiveChar();
    int numFields = pgStream.receiveInteger2();
    int[] fieldFormats = new int[numFields];

    for (int i = 0; i < numFields; i++) {
      fieldFormats[i] = pgStream.receiveInteger2();
    }

    lock(op);
    op.init(this, rowFormat, fieldFormats);
  }

  /**
   * Finishes a copy operation and unlocks connection discarding any exchanged data.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @throws SQLException on any additional failure
   */
  public void cancelCopy(CopyOperationImpl op) throws SQLException {
    if (!hasLock(op)) {
      throw new PSQLException(GT.tr("Tried to cancel an inactive copy operation"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    SQLException error = null;
    int errors = 0;

    try {
      if (op instanceof CopyIn) {
        synchronized (this) {
          if (logger.logDebug()) {
            logger.debug("FE => CopyFail");
          }
          final byte[] msg = Utils.encodeUTF8("Copy cancel requested");
          pgStream.sendChar('f'); // CopyFail
          pgStream.sendInteger4(5 + msg.length);
          pgStream.send(msg);
          pgStream.sendChar(0);
          pgStream.flush();
          do {
            try {
              processCopyResults(op, true); // discard rest of input
            } catch (SQLException se) { // expected error response to failing copy
              errors++;
              if (error != null) {
                SQLException e = se;
                SQLException next;
                while ((next = e.getNextException()) != null) {
                  e = next;
                }
                e.setNextException(error);
              }
              error = se;
            }
          } while (hasLock(op));
        }
      } else if (op instanceof CopyOut) {
        sendQueryCancel();
      }

    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when canceling copy operation"),
          PSQLState.CONNECTION_FAILURE, ioe);
    } finally {
      // Need to ensure the lock isn't held anymore, or else
      // future operations, rather than failing due to the
      // broken connection, will simply hang waiting for this
      // lock.
      synchronized (this) {
        if (hasLock(op)) {
          unlock(op);
        }
      }
    }

    if (op instanceof CopyIn) {
      if (errors < 1) {
        throw new PSQLException(GT.tr("Missing expected error response to copy cancel request"),
            PSQLState.COMMUNICATION_ERROR);
      } else if (errors > 1) {
        throw new PSQLException(
            GT.tr("Got {0} error responses to single copy cancel request", String.valueOf(errors)),
            PSQLState.COMMUNICATION_ERROR, error);
      }
    }
  }

  /**
   * Finishes writing to copy and unlocks connection
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @return number of rows updated for server versions 8.2 or newer
   * @throws SQLException on failure
   */
  public synchronized long endCopy(CopyOperationImpl op) throws SQLException {
    if (!hasLock(op)) {
      throw new PSQLException(GT.tr("Tried to end inactive copy"), PSQLState.OBJECT_NOT_IN_STATE);
    }

    try {
      if (logger.logDebug()) {
        logger.debug(" FE=> CopyDone");
      }

      pgStream.sendChar('c'); // CopyDone
      pgStream.sendInteger4(4);
      pgStream.flush();

      do {
        processCopyResults(op, true);
      } while (hasLock(op));
      return op.getHandledRowCount();
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when ending copy"),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  /**
   * Sends data during a live COPY IN operation. Only unlocks the connection if server suddenly
   * returns CommandComplete, which should not happen
   *
   * @param op the CopyIn operation presumably currently holding lock on this connection
   * @param data bytes to send
   * @param off index of first byte to send (usually 0)
   * @param siz number of bytes to send (usually data.length)
   * @throws SQLException on failure
   */
  public synchronized void writeToCopy(CopyOperationImpl op, byte[] data, int off, int siz)
      throws SQLException {
    if (!hasLock(op)) {
      throw new PSQLException(GT.tr("Tried to write to an inactive copy operation"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    if (logger.logDebug()) {
      logger.debug(" FE=> CopyData(" + siz + ")");
    }

    try {
      pgStream.sendChar('d');
      pgStream.sendInteger4(siz + 4);
      pgStream.send(data, off, siz);

      processCopyResults(op, false); // collect any pending notifications without blocking
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when writing to copy"),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  public synchronized void flushCopy(CopyOperationImpl op) throws SQLException {
    if (!hasLock(op)) {
      throw new PSQLException(GT.tr("Tried to write to an inactive copy operation"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    try {
      pgStream.flush();
      processCopyResults(op, false); // collect any pending notifications without blocking
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when writing to copy"),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  /**
   * Wait for a row of data to be received from server on an active copy operation
   * Connection gets unlocked by processCopyResults() at end of operation
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @param block whether to block waiting for input
   * @throws SQLException on any failure
   */
  synchronized void readFromCopy(CopyOperationImpl op, boolean block) throws SQLException {
    if (!hasLock(op)) {
      throw new PSQLException(GT.tr("Tried to read from inactive copy"),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    try {
      processCopyResults(op, block); // expect a call to handleCopydata() to store the data
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Database connection failed when reading from copy"),
          PSQLState.CONNECTION_FAILURE, ioe);
    }
  }

  /**
   * Handles copy sub protocol responses from server. Unlocks at end of sub protocol, so operations
   * on pgStream or QueryExecutor are not allowed in a method after calling this!
   *
   * @param block whether to block waiting for input
   * @return CopyIn when COPY FROM STDIN starts; CopyOut when COPY TO STDOUT starts; null when copy
   *         ends; otherwise, the operation given as parameter.
   * @throws SQLException in case of misuse
   * @throws IOException from the underlying connection
   */
  CopyOperationImpl processCopyResults(CopyOperationImpl op, boolean block)
      throws SQLException, IOException {

    boolean endReceiving = false;
    SQLException error = null;
    SQLException errors = null;
    int len;

    while (!endReceiving && (block || pgStream.hasMessagePending())) {

      // There is a bug in the server's implementation of the copy
      // protocol. It returns command complete immediately upon
      // receiving the EOF marker in the binary protocol,
      // potentially before we've issued CopyDone. When we are not
      // blocking, we don't think we are done, so we hold off on
      // processing command complete and any subsequent messages
      // until we actually are done with the copy.
      //
      if (!block) {
        int c = pgStream.peekChar();
        if (c == 'C') {
          // CommandComplete
          if (logger.logDebug()) {
            logger.debug(" <=BE CommandStatus, Ignored until CopyDone");
          }
          break;
        }
      }

      int c = pgStream.receiveChar();
      switch (c) {

        case 'A': // Asynchronous Notify

          if (logger.logDebug()) {
            logger.debug(" <=BE Asynchronous Notification while copying");
          }

          receiveAsyncNotify();
          break;

        case 'N': // Notice Response

          if (logger.logDebug()) {
            logger.debug(" <=BE Notification while copying");
          }

          addWarning(receiveNoticeResponse());
          break;

        case 'C': // Command Complete

          String status = receiveCommandStatus();

          try {
            if (op == null) {
              throw new PSQLException(GT
                  .tr("Received CommandComplete ''{0}'' without an active copy operation", status),
                  PSQLState.OBJECT_NOT_IN_STATE);
            }
            op.handleCommandStatus(status);
          } catch (SQLException se) {
            error = se;
          }

          block = true;
          break;

        case 'E': // ErrorMessage (expected response to CopyFail)

          error = receiveErrorResponse();
          // We've received the error and we now expect to receive
          // Ready for query, but we must block because it might still be
          // on the wire and not here yet.
          block = true;
          break;

        case 'G': // CopyInResponse

          if (logger.logDebug()) {
            logger.debug(" <=BE CopyInResponse");
          }

          if (op != null) {
            error = new PSQLException(GT.tr("Got CopyInResponse from server during an active {0}",
                op.getClass().getName()), PSQLState.OBJECT_NOT_IN_STATE);
          }

          op = new CopyInImpl();
          initCopy(op);
          endReceiving = true;
          break;

        case 'H': // CopyOutResponse

          if (logger.logDebug()) {
            logger.debug(" <=BE CopyOutResponse");
          }

          if (op != null) {
            error = new PSQLException(GT.tr("Got CopyOutResponse from server during an active {0}",
                op.getClass().getName()), PSQLState.OBJECT_NOT_IN_STATE);
          }

          op = new CopyOutImpl();
          initCopy(op);
          endReceiving = true;
          break;

        case 'W': // CopyBothResponse

          if (logger.logDebug()) {
            logger.debug(" <=BE CopyBothResponse");
          }

          if (op != null) {
            error = new PSQLException(GT.tr("Got CopyBothResponse from server during an active {0}",
                    op.getClass().getName()), PSQLState.OBJECT_NOT_IN_STATE);
          }

          op = new CopyDualImpl();
          initCopy(op);
          endReceiving = true;
          break;

        case 'd': // CopyData

          if (logger.logDebug()) {
            logger.debug(" <=BE CopyData");
          }

          len = pgStream.receiveInteger4() - 4;
          byte[] buf = pgStream.receive(len);
          if (op == null) {
            error = new PSQLException(GT.tr("Got CopyData without an active copy operation"),
                PSQLState.OBJECT_NOT_IN_STATE);
          } else if (!(op instanceof CopyOut)) {
            error = new PSQLException(
                GT.tr("Unexpected copydata from server for {0}", op.getClass().getName()),
                PSQLState.COMMUNICATION_ERROR);
          } else {
            op.handleCopydata(buf);
          }
          endReceiving = true;
          break;

        case 'c': // CopyDone (expected after all copydata received)

          if (logger.logDebug()) {
            logger.debug(" <=BE CopyDone");
          }

          len = pgStream.receiveInteger4() - 4;
          if (len > 0) {
            pgStream.receive(len); // not in specification; should never appear
          }

          if (!(op instanceof CopyOut)) {
            error = new PSQLException("Got CopyDone while not copying from server",
                PSQLState.OBJECT_NOT_IN_STATE);
          }

          // keep receiving since we expect a CommandComplete
          block = true;
          break;
        case 'S': // Parameter Status
        {
          int l_len = pgStream.receiveInteger4();
          String name = pgStream.receiveString();
          String value = pgStream.receiveString();
          if (logger.logDebug()) {
            logger.debug(" <=BE ParameterStatus(" + name + " = " + value + ")");
          }

          if (name.equals("client_encoding") && !value.equalsIgnoreCase("UTF8")
              && !allowEncodingChanges) {
            close(); // we're screwed now; we can't trust any subsequent string.
            error = new PSQLException(GT.tr(
                "The server''s client_encoding parameter was changed to {0}. The JDBC driver requires client_encoding to be UTF8 for correct operation.",
                value), PSQLState.CONNECTION_FAILURE);
            endReceiving = true;
          }

          if (name.equals("DateStyle") && !value.startsWith("ISO,")) {
            close(); // we're screwed now; we can't trust any subsequent date.
            error = new PSQLException(GT.tr(
                "The server''s DateStyle parameter was changed to {0}. The JDBC driver requires DateStyle to begin with ISO for correct operation.",
                value), PSQLState.CONNECTION_FAILURE);
            endReceiving = true;
          }

          if (name.equals("standard_conforming_strings")) {
            if (value.equals("on")) {
              setStandardConformingStrings(true);
            } else if (value.equals("off")) {
              setStandardConformingStrings(false);
            } else {
              close();
              // we're screwed now; we don't know how to escape string literals
              error = new PSQLException(GT.tr(
                  "The server''s standard_conforming_strings parameter was reported as {0}. The JDBC driver expected on or off.",
                  value), PSQLState.CONNECTION_FAILURE);
              endReceiving = true;
            }
          }
          break;
        }

        case 'Z': // ReadyForQuery: After FE:CopyDone => BE:CommandComplete

          receiveRFQ();
          if (hasLock(op)) {
            unlock(op);
          }
          op = null;
          endReceiving = true;
          break;

        // If the user sends a non-copy query, we've got to handle some additional things.
        //
        case 'T': // Row Description (response to Describe)
          if (logger.logDebug()) {
            logger.debug(" <=BE RowDescription (during copy ignored)");
          }


          skipMessage();
          break;

        case 'D': // DataRow
          if (logger.logDebug()) {
            logger.debug(" <=BE DataRow (during copy ignored)");
          }

          skipMessage();
          break;

        default:
          throw new IOException(
              GT.tr("Unexpected packet type during copy: {0}", Integer.toString(c)));
      }

      // Collect errors into a neat chain for completeness
      if (error != null) {
        if (errors != null) {
          error.setNextException(errors);
        }
        errors = error;
        error = null;
      }
    }

    if (errors != null) {
      throw errors;
    }

    return op;
  }

  /*
   * To prevent client/server protocol deadlocks, we try to manage the estimated recv buffer size
   * and force a sync +flush and process results if we think it might be getting too full.
   *
   * See the comments above MAX_BUFFERED_RECV_BYTES's declaration for details.
   */
  private void flushIfDeadlockRisk(Query query, boolean disallowBatching,
      ResultHandler resultHandler,
      BatchResultHandler batchHandler,
      final int flags) throws IOException {
    // Assume all statements need at least this much reply buffer space,
    // plus params
    estimatedReceiveBufferBytes += NODATA_QUERY_RESPONSE_SIZE_BYTES;

    SimpleQuery sq = (SimpleQuery) query;
    if (sq.isStatementDescribed()) {
      /*
       * Estimate the response size of the fields and add it to the expected response size.
       *
       * It's impossible for us to estimate the rowcount. We'll assume one row, as that's the common
       * case for batches and we're leaving plenty of breathing room in this approach. It's still
       * not deadlock-proof though; see pgjdbc github issues #194 and #195.
       */
      int maxResultRowSize = sq.getMaxResultRowSize();
      if (maxResultRowSize >= 0) {
        estimatedReceiveBufferBytes += maxResultRowSize;
      } else {
        logger.debug("Couldn't estimate result size or result size unbounded, "
            + "disabling batching for this query.");
        disallowBatching = true;
      }
    } else {
      /*
       * We only describe a statement if we're expecting results from it, so it's legal to batch
       * unprepared statements. We'll abort later if we get any uresults from them where none are
       * expected. For now all we can do is hope the user told us the truth and assume that
       * NODATA_QUERY_RESPONSE_SIZE_BYTES is enough to cover it.
       */
    }

    if (disallowBatching || estimatedReceiveBufferBytes >= MAX_BUFFERED_RECV_BYTES) {
      logger.debug("Forcing Sync, receive buffer full or batching disallowed");
      sendSync();
      processResults(resultHandler, flags);
      estimatedReceiveBufferBytes = 0;
      if (batchHandler != null) {
        batchHandler.secureProgress();
      }
    }

  }

  /*
   * Send a query to the backend.
   */
  private void sendQuery(Query query, V3ParameterList parameters, int maxRows, int fetchSize,
      int flags, ResultHandler resultHandler,
      BatchResultHandler batchHandler) throws IOException, SQLException {
    // Now the query itself.
    Query[] subqueries = query.getSubqueries();
    SimpleParameterList[] subparams = parameters.getSubparams();

    // We know this is deprecated, but still respect it in case anyone's using it.
    // PgJDBC its self no longer does.
    @SuppressWarnings("deprecation")
    boolean disallowBatching = (flags & QueryExecutor.QUERY_DISALLOW_BATCHING) != 0;

    if (subqueries == null) {
      flushIfDeadlockRisk(query, disallowBatching, resultHandler, batchHandler, flags);

      // If we saw errors, don't send anything more.
      if (resultHandler.getException() == null) {
        sendOneQuery((SimpleQuery) query, (SimpleParameterList) parameters, maxRows, fetchSize,
            flags);
      }
    } else {
      for (int i = 0; i < subqueries.length; ++i) {
        final Query subquery = subqueries[i];
        flushIfDeadlockRisk(subquery, disallowBatching, resultHandler, batchHandler, flags);

        // If we saw errors, don't send anything more.
        if (resultHandler.getException() != null) {
          break;
        }

        // In the situation where parameters is already
        // NO_PARAMETERS it cannot know the correct
        // number of array elements to return in the
        // above call to getSubparams(), so it must
        // return null which we check for here.
        //
        SimpleParameterList subparam = SimpleQuery.NO_PARAMETERS;
        if (subparams != null) {
          subparam = subparams[i];
        }
        sendOneQuery((SimpleQuery) subquery, subparam, maxRows, fetchSize, flags);
      }
    }
  }

  //
  // Message sending
  //

  private void sendSync() throws IOException {
    if (logger.logDebug()) {
      logger.debug(" FE=> Sync");
    }

    pgStream.sendChar('S'); // Sync
    pgStream.sendInteger4(4); // Length
    pgStream.flush();
    pendingExecuteQueue.add(new ExecuteRequest(sync, null, true));
    pendingDescribePortalQueue.add(sync);
  }

  private void sendParse(SimpleQuery query, SimpleParameterList params, boolean oneShot)
      throws IOException {
    // Already parsed, or we have a Parse pending and the types are right?
    int[] typeOIDs = params.getTypeOIDs();
    if (query.isPreparedFor(typeOIDs, deallocateEpoch)) {
      return;
    }

    // Clean up any existing statement, as we can't use it.
    query.unprepare();
    processDeadParsedQueries();

    // Remove any cached Field values. The re-parsed query might report different
    // fields because input parameter types may result in different type inferences
    // for unspecified types.
    query.setFields(null);

    String statementName = null;
    if (!oneShot) {
      // Generate a statement name to use.
      statementName = "S_" + (nextUniqueID++);

      // And prepare the new statement.
      // NB: Must clone the OID array, as it's a direct reference to
      // the SimpleParameterList's internal array that might be modified
      // under us.
      query.setStatementName(statementName, deallocateEpoch);
      query.setStatementTypes(typeOIDs.clone());
      registerParsedQuery(query, statementName);
    }

    byte[] encodedStatementName = query.getEncodedStatementName();
    String nativeSql = query.getNativeSql();

    if (logger.logDebug()) {
      StringBuilder sbuf = new StringBuilder(" FE=> Parse(stmt=" + statementName + ",query=\"");
      sbuf.append(nativeSql);
      sbuf.append("\",oids={");
      for (int i = 1; i <= params.getParameterCount(); ++i) {
        if (i != 1) {
          sbuf.append(",");
        }
        sbuf.append(params.getTypeOID(i));
      }
      sbuf.append("})");
      logger.debug(sbuf.toString());
    }

    //
    // Send Parse.
    //

    byte[] queryUtf8 = Utils.encodeUTF8(nativeSql);

    // Total size = 4 (size field)
    // + N + 1 (statement name, zero-terminated)
    // + N + 1 (query, zero terminated)
    // + 2 (parameter count) + N * 4 (parameter types)
    int encodedSize = 4
        + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1
        + queryUtf8.length + 1
        + 2 + 4 * params.getParameterCount();

    pgStream.sendChar('P'); // Parse
    pgStream.sendInteger4(encodedSize);
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName);
    }
    pgStream.sendChar(0); // End of statement name
    pgStream.send(queryUtf8); // Query string
    pgStream.sendChar(0); // End of query string.
    pgStream.sendInteger2(params.getParameterCount()); // # of parameter types specified
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      pgStream.sendInteger4(params.getTypeOID(i));
    }

    pendingParseQueue.add(query);
  }

  private void sendBind(SimpleQuery query, SimpleParameterList params, Portal portal,
      boolean noBinaryTransfer) throws IOException {
    //
    // Send Bind.
    //

    String statementName = query.getStatementName();
    byte[] encodedStatementName = query.getEncodedStatementName();
    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());

    if (logger.logDebug()) {
      StringBuilder sbuf =
          new StringBuilder(" FE=> Bind(stmt=" + statementName + ",portal=" + portal);
      for (int i = 1; i <= params.getParameterCount(); ++i) {
        sbuf.append(",$").append(i).append("=<")
            .append(params.toString(i,true))
            .append(">");
      }
      sbuf.append(")");
      logger.debug(sbuf.toString());
    }

    // Total size = 4 (size field) + N + 1 (destination portal)
    // + N + 1 (statement name)
    // + 2 (param format code count) + N * 2 (format codes)
    // + 2 (param value count) + N (encoded param value size)
    // + 2 (result format code count, 0)
    long encodedSize = 0;
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      if (params.isNull(i)) {
        encodedSize += 4;
      } else {
        encodedSize += (long) 4 + params.getV3Length(i);
      }
    }

    Field[] fields = query.getFields();
    if (!noBinaryTransfer && query.needUpdateFieldFormats()) {
      for (Field field : fields) {
        if (useBinary(field)) {
          field.setFormat(Field.BINARY_FORMAT);
          query.setHasBinaryFields(true);
        }
      }
    }

    // This is not the number of binary fields, but the total number
    // of fields if any of them are binary or zero if all of them
    // are text.
    int numBinaryFields = !noBinaryTransfer && query.hasBinaryFields() ? fields.length : 0;

    encodedSize = 4
        + (encodedPortalName == null ? 0 : encodedPortalName.length) + 1
        + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1
        + 2 + params.getParameterCount() * 2
        + 2 + encodedSize
        + 2 + numBinaryFields * 2;

    // backend's MaxAllocSize is the largest message that can
    // be received from a client. If we have a bigger value
    // from either very large parameters or incorrent length
    // descriptions of setXXXStream we do not send the bind
    // messsage.
    //
    if (encodedSize > 0x3fffffff) {
      throw new PGBindException(new IOException(GT.tr(
          "Bind message length {0} too long.  This can be caused by very large or incorrect length specifications on InputStream parameters.",
          encodedSize)));
    }

    pgStream.sendChar('B'); // Bind
    pgStream.sendInteger4((int) encodedSize); // Message size
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // Destination portal name.
    }
    pgStream.sendChar(0); // End of portal name.
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName); // Source statement name.
    }
    pgStream.sendChar(0); // End of statement name.

    pgStream.sendInteger2(params.getParameterCount()); // # of parameter format codes
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      pgStream.sendInteger2(params.isBinary(i) ? 1 : 0); // Parameter format code
    }

    pgStream.sendInteger2(params.getParameterCount()); // # of parameter values

    // If an error occurs when reading a stream we have to
    // continue pumping out data to match the length we
    // said we would. Once we've done that we throw
    // this exception. Multiple exceptions can occur and
    // it really doesn't matter which one is reported back
    // to the caller.
    //
    PGBindException bindException = null;

    for (int i = 1; i <= params.getParameterCount(); ++i) {
      if (params.isNull(i)) {
        pgStream.sendInteger4(-1); // Magic size of -1 means NULL
      } else {
        pgStream.sendInteger4(params.getV3Length(i)); // Parameter size
        try {
          params.writeV3Value(i, pgStream); // Parameter value
        } catch (PGBindException be) {
          bindException = be;
        }
      }
    }

    pgStream.sendInteger2(numBinaryFields); // # of result format codes
    for (int i = 0; i < numBinaryFields; ++i) {
      pgStream.sendInteger2(fields[i].getFormat());
    }

    pendingBindQueue.add(portal == null ? UNNAMED_PORTAL : portal);

    if (bindException != null) {
      throw bindException;
    }
  }

  /**
   * Returns true if the specified field should be retrieved using binary encoding.
   *
   * @param field The field whose Oid type to analyse.
   * @return True if {@link Field#BINARY_FORMAT} should be used, false if
   *         {@link Field#BINARY_FORMAT}.
   */
  private boolean useBinary(Field field) {
    int oid = field.getOID();
    return useBinaryForReceive(oid);
  }

  private void sendDescribePortal(SimpleQuery query, Portal portal) throws IOException {
    //
    // Send Describe.
    //

    if (logger.logDebug()) {
      logger.debug(" FE=> Describe(portal=" + portal + ")");
    }

    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());

    // Total size = 4 (size field) + 1 (describe type, 'P') + N + 1 (portal name)
    int encodedSize = 4 + 1 + (encodedPortalName == null ? 0 : encodedPortalName.length) + 1;

    pgStream.sendChar('D'); // Describe
    pgStream.sendInteger4(encodedSize); // message size
    pgStream.sendChar('P'); // Describe (Portal)
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // portal name to close
    }
    pgStream.sendChar(0); // end of portal name

    pendingDescribePortalQueue.add(query);
    query.setPortalDescribed(true);
  }

  private void sendDescribeStatement(SimpleQuery query, SimpleParameterList params,
      boolean describeOnly) throws IOException {
    // Send Statement Describe

    if (logger.logDebug()) {
      logger.debug(" FE=> Describe(statement=" + query.getStatementName() + ")");
    }

    byte[] encodedStatementName = query.getEncodedStatementName();

    // Total size = 4 (size field) + 1 (describe type, 'S') + N + 1 (portal name)
    int encodedSize = 4 + 1 + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1;

    pgStream.sendChar('D'); // Describe
    pgStream.sendInteger4(encodedSize); // Message size
    pgStream.sendChar('S'); // Describe (Statement);
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName); // Statement name
    }
    pgStream.sendChar(0); // end message

    // Note: statement name can change over time for the same query object
    // Thus we take a snapshot of the query name
    pendingDescribeStatementQueue.add(
        new DescribeRequest(query, params, describeOnly, query.getStatementName()));
    pendingDescribePortalQueue.add(query);
    query.setStatementDescribed(true);
    query.setPortalDescribed(true);
  }

  private void sendExecute(SimpleQuery query, Portal portal, int limit) throws IOException {
    //
    // Send Execute.
    //

    if (logger.logDebug()) {
      logger.debug(" FE=> Execute(portal=" + portal + ",limit=" + limit + ")");
    }

    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());
    int encodedSize = (encodedPortalName == null ? 0 : encodedPortalName.length);

    // Total size = 4 (size field) + 1 + N (source portal) + 4 (max rows)
    pgStream.sendChar('E'); // Execute
    pgStream.sendInteger4(4 + 1 + encodedSize + 4); // message size
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // portal name
    }
    pgStream.sendChar(0); // portal name terminator
    pgStream.sendInteger4(limit); // row limit

    pendingExecuteQueue.add(new ExecuteRequest(query, portal, false));
  }

  private void sendClosePortal(String portalName) throws IOException {
    //
    // Send Close.
    //

    if (logger.logDebug()) {
      logger.debug(" FE=> ClosePortal(" + portalName + ")");
    }

    byte[] encodedPortalName = (portalName == null ? null : Utils.encodeUTF8(portalName));
    int encodedSize = (encodedPortalName == null ? 0 : encodedPortalName.length);

    // Total size = 4 (size field) + 1 (close type, 'P') + 1 + N (portal name)
    pgStream.sendChar('C'); // Close
    pgStream.sendInteger4(4 + 1 + 1 + encodedSize); // message size
    pgStream.sendChar('P'); // Close (Portal)
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName);
    }
    pgStream.sendChar(0); // unnamed portal
  }

  private void sendCloseStatement(String statementName) throws IOException {
    //
    // Send Close.
    //

    if (logger.logDebug()) {
      logger.debug(" FE=> CloseStatement(" + statementName + ")");
    }

    byte[] encodedStatementName = Utils.encodeUTF8(statementName);

    // Total size = 4 (size field) + 1 (close type, 'S') + N + 1 (statement name)
    pgStream.sendChar('C'); // Close
    pgStream.sendInteger4(4 + 1 + encodedStatementName.length + 1); // message size
    pgStream.sendChar('S'); // Close (Statement)
    pgStream.send(encodedStatementName); // statement to close
    pgStream.sendChar(0); // statement name terminator
  }



  // sendOneQuery sends a single statement via the extended query protocol.
  // Per the FE/BE docs this is essentially the same as how a simple query runs
  // (except that it generates some extra acknowledgement messages, and we
  // can send several queries before doing the Sync)
  //
  // Parse S_n from "query string with parameter placeholders"; skipped if already done previously
  // or if oneshot
  // Bind C_n from S_n plus parameters (or from unnamed statement for oneshot queries)
  // Describe C_n; skipped if caller doesn't want metadata
  // Execute C_n with maxRows limit; maxRows = 1 if caller doesn't want results
  // (above repeats once per call to sendOneQuery)
  // Sync (sent by caller)
  //
  private void sendOneQuery(SimpleQuery query, SimpleParameterList params, int maxRows,
      int fetchSize, int flags) throws IOException {
    boolean asSimple = (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0;
    if (asSimple) {
      assert (flags & QueryExecutor.QUERY_DESCRIBE_ONLY) == 0
          : "Simple mode does not support describe requests. sql = " + query.getNativeSql()
          + ", flags = " + flags;
      sendSimpleQuery(query, params);
      return;
    }

    assert !query.getNativeQuery().multiStatement
        : "Queries that might contain ; must be executed with QueryExecutor.QUERY_EXECUTE_AS_SIMPLE mode. "
        + "Given query is " + query.getNativeSql();

    // nb: if we decide to use a portal (usePortal == true) we must also use a named statement
    // (oneShot == false) as otherwise the portal will be closed under us unexpectedly when
    // the unnamed statement is next reused.

    boolean noResults = (flags & QueryExecutor.QUERY_NO_RESULTS) != 0;
    boolean noMeta = (flags & QueryExecutor.QUERY_NO_METADATA) != 0;
    boolean describeOnly = (flags & QueryExecutor.QUERY_DESCRIBE_ONLY) != 0;
    boolean usePortal = (flags & QueryExecutor.QUERY_FORWARD_CURSOR) != 0 && !noResults && !noMeta
        && fetchSize > 0 && !describeOnly;
    boolean oneShot = (flags & QueryExecutor.QUERY_ONESHOT) != 0 && !usePortal;
    boolean noBinaryTransfer = (flags & QUERY_NO_BINARY_TRANSFER) != 0;
    boolean forceDescribePortal = (flags & QUERY_FORCE_DESCRIBE_PORTAL) != 0;

    // Work out how many rows to fetch in this pass.

    int rows;
    if (noResults) {
      rows = 1; // We're discarding any results anyway, so limit data transfer to a minimum
    } else if (!usePortal) {
      rows = maxRows; // Not using a portal -- fetchSize is irrelevant
    } else if (maxRows != 0 && fetchSize > maxRows) {
      // fetchSize > maxRows, use maxRows (nb: fetchSize cannot be 0 if usePortal == true)
      rows = maxRows;
    } else {
      rows = fetchSize; // maxRows > fetchSize
    }

    sendParse(query, params, oneShot);

    // Must do this after sendParse to pick up any changes to the
    // query's state.
    //
    boolean queryHasUnknown = query.hasUnresolvedTypes();
    boolean paramsHasUnknown = params.hasUnresolvedTypes();

    boolean describeStatement = describeOnly
        || (!oneShot && paramsHasUnknown && queryHasUnknown && !query.isStatementDescribed());

    if (!describeStatement && paramsHasUnknown && !queryHasUnknown) {
      int queryOIDs[] = query.getStatementTypes();
      int paramOIDs[] = params.getTypeOIDs();
      for (int i = 0; i < paramOIDs.length; i++) {
        // Only supply type information when there isn't any
        // already, don't arbitrarily overwrite user supplied
        // type information.
        if (paramOIDs[i] == Oid.UNSPECIFIED) {
          params.setResolvedType(i + 1, queryOIDs[i]);
        }
      }
    }

    if (describeStatement) {
      sendDescribeStatement(query, params, describeOnly);
      if (describeOnly) {
        return;
      }
    }

    // Construct a new portal if needed.
    Portal portal = null;
    if (usePortal) {
      String portalName = "C_" + (nextUniqueID++);
      portal = new Portal(query, portalName);
    }

    sendBind(query, params, portal, noBinaryTransfer);

    // A statement describe will also output a RowDescription,
    // so don't reissue it here if we've already done so.
    //
    if (!noMeta && !describeStatement) {
      /*
       * don't send describe if we already have cached the row description from previous executions
       *
       * XXX Clearing the fields / unpreparing the query (in sendParse) is incorrect, see bug #267.
       * We might clear the cached fields in a later execution of this query if the bind parameter
       * types change, but we're assuming here that they'll still be valid when we come to process
       * the results of this query, so we don't send a new describe here. We re-describe after the
       * fields are cleared, but the result of that gets processed after processing the results from
       * earlier executions that we didn't describe because we didn't think we had to.
       *
       * To work around this, force a Describe at each execution in batches where this can be a
       * problem. It won't cause more round trips so the performance impact is low, and it'll ensure
       * that the field information available when we decoded the results. This is undeniably a
       * hack, but there aren't many good alternatives.
       */
      if (!query.isPortalDescribed() || forceDescribePortal) {
        sendDescribePortal(query, portal);
      }
    }

    sendExecute(query, portal, rows);
  }

  private void sendSimpleQuery(SimpleQuery query, SimpleParameterList params) throws IOException {
    String nativeSql = query.toString(params);

    if (logger.logDebug()) {
      logger.debug(" FE=> SimpleQuery(query=\"" + nativeSql + "\")");
    }
    Encoding encoding = pgStream.getEncoding();

    byte[] encoded = encoding.encode(nativeSql);
    pgStream.sendChar('Q');
    pgStream.sendInteger4(encoded.length + 4 + 1);
    pgStream.send(encoded);
    pgStream.sendChar(0);
    pgStream.flush();
    pendingExecuteQueue.add(new ExecuteRequest(query, null, true));
    pendingDescribePortalQueue.add(query);
  }

  //
  // Garbage collection of parsed statements.
  //
  // When a statement is successfully parsed, registerParsedQuery is called.
  // This creates a PhantomReference referring to the "owner" of the statement
  // (the originating Query object) and inserts that reference as a key in
  // parsedQueryMap. The values of parsedQueryMap are the corresponding allocated
  // statement names. The originating Query object also holds a reference to the
  // PhantomReference.
  //
  // When the owning Query object is closed, it enqueues and clears the associated
  // PhantomReference.
  //
  // If the owning Query object becomes unreachable (see java.lang.ref javadoc) before
  // being closed, the corresponding PhantomReference is enqueued on
  // parsedQueryCleanupQueue. In the Sun JVM, phantom references are only enqueued
  // when a GC occurs, so this is not necessarily prompt but should eventually happen.
  //
  // Periodically (currently, just before query execution), the parsedQueryCleanupQueue
  // is polled. For each enqueued PhantomReference we find, we remove the corresponding
  // entry from parsedQueryMap, obtaining the name of the underlying statement in the
  // process. Then we send a message to the backend to deallocate that statement.
  //

  private final HashMap<PhantomReference<SimpleQuery>, String> parsedQueryMap =
      new HashMap<PhantomReference<SimpleQuery>, String>();
  private final ReferenceQueue<SimpleQuery> parsedQueryCleanupQueue =
      new ReferenceQueue<SimpleQuery>();

  private void registerParsedQuery(SimpleQuery query, String statementName) {
    if (statementName == null) {
      return;
    }

    PhantomReference<SimpleQuery> cleanupRef =
        new PhantomReference<SimpleQuery>(query, parsedQueryCleanupQueue);
    parsedQueryMap.put(cleanupRef, statementName);
    query.setCleanupRef(cleanupRef);
  }

  private void processDeadParsedQueries() throws IOException {
    Reference<? extends SimpleQuery> deadQuery;
    while ((deadQuery = parsedQueryCleanupQueue.poll()) != null) {
      String statementName = parsedQueryMap.remove(deadQuery);
      sendCloseStatement(statementName);
      deadQuery.clear();
    }
  }

  //
  // Essentially the same strategy is used for the cleanup of portals.
  // Note that each Portal holds a reference to the corresponding Query
  // that generated it, so the Query won't be collected (and the statement
  // closed) until all the Portals are, too. This is required by the mechanics
  // of the backend protocol: when a statement is closed, all dependent portals
  // are also closed.
  //

  private final HashMap<PhantomReference<Portal>, String> openPortalMap =
      new HashMap<PhantomReference<Portal>, String>();
  private final ReferenceQueue<Portal> openPortalCleanupQueue = new ReferenceQueue<Portal>();

  private static final Portal UNNAMED_PORTAL = new Portal(null, "unnamed");

  private void registerOpenPortal(Portal portal) {
    if (portal == UNNAMED_PORTAL) {
      return; // Using the unnamed portal.
    }

    String portalName = portal.getPortalName();
    PhantomReference<Portal> cleanupRef =
        new PhantomReference<Portal>(portal, openPortalCleanupQueue);
    openPortalMap.put(cleanupRef, portalName);
    portal.setCleanupRef(cleanupRef);
  }

  private void processDeadPortals() throws IOException {
    Reference<? extends Portal> deadPortal;
    while ((deadPortal = openPortalCleanupQueue.poll()) != null) {
      String portalName = openPortalMap.remove(deadPortal);
      sendClosePortal(portalName);
      deadPortal.clear();
    }
  }

  protected void processResults(ResultHandler handler, int flags) throws IOException {
    boolean noResults = (flags & QueryExecutor.QUERY_NO_RESULTS) != 0;
    boolean bothRowsAndStatus = (flags & QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS) != 0;

    List<byte[][]> tuples = null;

    int c;
    boolean endQuery = false;

    // At the end of a command execution we have the CommandComplete
    // message to tell us we're done, but with a describeOnly command
    // we have no real flag to let us know we're done. We've got to
    // look for the next RowDescription or NoData message and return
    // from there.
    boolean doneAfterRowDescNoData = false;

    while (!endQuery) {
      c = pgStream.receiveChar();
      switch (c) {
        case 'A': // Asynchronous Notify
          receiveAsyncNotify();
          break;

        case '1': // Parse Complete (response to Parse)
          pgStream.receiveInteger4(); // len, discarded

          SimpleQuery parsedQuery = pendingParseQueue.removeFirst();
          String parsedStatementName = parsedQuery.getStatementName();

          if (logger.logDebug()) {
            logger.debug(" <=BE ParseComplete [" + parsedStatementName + "]");
          }

          break;

        case 't': // ParameterDescription
          pgStream.receiveInteger4(); // len, discarded

          if (logger.logDebug()) {
            logger.debug(" <=BE ParameterDescription");
          }

        {
          DescribeRequest describeData = pendingDescribeStatementQueue.getFirst();
          SimpleQuery query = describeData.query;
          SimpleParameterList params = describeData.parameterList;
          boolean describeOnly = describeData.describeOnly;
          // This might differ from query.getStatementName if the query was re-prepared
          String origStatementName = describeData.statementName;

          int numParams = pgStream.receiveInteger2();

          for (int i = 1; i <= numParams; i++) {
            int typeOid = pgStream.receiveInteger4();
            params.setResolvedType(i, typeOid);
          }

          // Since we can issue multiple Parse and DescribeStatement
          // messages in a single network trip, we need to make
          // sure the describe results we requested are still
          // applicable to the latest parsed query.
          //
          if ((origStatementName == null && query.getStatementName() == null)
              || (origStatementName != null
                  && origStatementName.equals(query.getStatementName()))) {
            query.setStatementTypes(params.getTypeOIDs().clone());
          }

          if (describeOnly) {
            doneAfterRowDescNoData = true;
          } else {
            pendingDescribeStatementQueue.removeFirst();
          }
          break;
        }

        case '2': // Bind Complete (response to Bind)
          pgStream.receiveInteger4(); // len, discarded

          Portal boundPortal = pendingBindQueue.removeFirst();
          if (logger.logDebug()) {
            logger.debug(" <=BE BindComplete [" + boundPortal + "]");
          }

          registerOpenPortal(boundPortal);
          break;

        case '3': // Close Complete (response to Close)
          pgStream.receiveInteger4(); // len, discarded
          if (logger.logDebug()) {
            logger.debug(" <=BE CloseComplete");
          }
          break;

        case 'n': // No Data (response to Describe)
          pgStream.receiveInteger4(); // len, discarded
          if (logger.logDebug()) {
            logger.debug(" <=BE NoData");
          }

          pendingDescribePortalQueue.removeFirst();

          if (doneAfterRowDescNoData) {
            DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
            SimpleQuery currentQuery = describeData.query;

            Field[] fields = currentQuery.getFields();

            if (fields != null) { // There was a resultset.
              tuples = new ArrayList<byte[][]>();
              handler.handleResultRows(currentQuery, fields, tuples, null);
              tuples = null;
            }
          }
          break;

        case 's': // Portal Suspended (end of Execute)
          // nb: this appears *instead* of CommandStatus.
          // Must be a SELECT if we suspended, so don't worry about it.

          pgStream.receiveInteger4(); // len, discarded
          if (logger.logDebug()) {
            logger.debug(" <=BE PortalSuspended");
          }

        {
          ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
          SimpleQuery currentQuery = executeData.query;
          Portal currentPortal = executeData.portal;

          Field[] fields = currentQuery.getFields();
          if (fields != null && !noResults && tuples == null) {
            tuples = new ArrayList<byte[][]>();
          }

          handler.handleResultRows(currentQuery, fields, tuples, currentPortal);
          tuples = null;
          break;
        }

        case 'C': // Command Status (end of Execute)
          // Handle status.
          String status = receiveCommandStatus();
          if (isFlushCacheOnDeallocate()
              && (status.startsWith("DEALLOCATE ALL") || status.startsWith("DISCARD ALL"))) {
            deallocateEpoch++;
          }

          doneAfterRowDescNoData = false;

        {
          ExecuteRequest executeData = pendingExecuteQueue.peekFirst();
          SimpleQuery currentQuery = executeData.query;
          Portal currentPortal = executeData.portal;

          if (status.startsWith("SET")) {
            String nativeSql = currentQuery.getNativeQuery().nativeSql;
            // Scan only the first 1024 characters to
            // avoid big overhead for long queries.
            if (nativeSql.lastIndexOf("search_path", 1024) != -1
                && !nativeSql.equals(lastSetSearchPathQuery)) {
              // Search path was changed, invalidate prepared statement cache
              lastSetSearchPathQuery = nativeSql;
              deallocateEpoch++;
            }
          }

          if (!executeData.asSimple) {
            pendingExecuteQueue.removeFirst();
          } else {
            // For simple 'Q' queries, executeQueue is cleared via ReadyForQuery message
          }

          if (currentQuery == autoSaveQuery) {
            // ignore "SAVEPOINT" status from autosave query
            break;
          }

          Field[] fields = currentQuery.getFields();
          if (fields != null && !noResults && tuples == null) {
            tuples = new ArrayList<byte[][]>();
          }

          // If we received tuples we must know the structure of the
          // resultset, otherwise we won't be able to fetch columns
          // from it, etc, later.
          if (fields == null && tuples != null) {
            throw new IllegalStateException(
                "Received resultset tuples, but no field structure for them");
          }

          if (fields != null || tuples != null) {
            // There was a resultset.
            handler.handleResultRows(currentQuery, fields, tuples, null);
            tuples = null;

            if (bothRowsAndStatus) {
              interpretCommandStatus(status, handler);
            }
          } else {
            interpretCommandStatus(status, handler);
          }

          if (executeData.asSimple) {
            // Simple queries might return several resultsets, thus we clear
            // fields, so queries like "select 1;update; select2" will properly
            // identify that "update" did not return any results
            currentQuery.setFields(null);
          }

          if (currentPortal != null) {
            currentPortal.close();
          }
          break;
        }

        case 'D': // Data Transfer (ongoing Execute response)
          byte[][] tuple = null;
          try {
            tuple = pgStream.receiveTupleV3();
          } catch (OutOfMemoryError oome) {
            if (!noResults) {
              handler.handleError(
                  new PSQLException(GT.tr("Ran out of memory retrieving query results."),
                      PSQLState.OUT_OF_MEMORY, oome));
            }
          }


          if (!noResults) {
            if (tuples == null) {
              tuples = new ArrayList<byte[][]>();
            }
            tuples.add(tuple);
          }

          if (logger.logDebug()) {
            int length;
            if (tuple == null) {
              length = -1;
            } else {
              length = 0;
              for (byte[] aTuple : tuple) {
                if (aTuple == null) {
                  continue;
                }
                length += aTuple.length;
              }
            }
            logger.debug(" <=BE DataRow(len=" + length + ")");
          }

          break;

        case 'E':
          // Error Response (response to pretty much everything; backend then skips until Sync)
          SQLException error = receiveErrorResponse();
          handler.handleError(error);
          if (willHealViaReparse(error)) {
            // prepared statement ... is not valid kind of error
            // Technically speaking, the error is unexpected, thus we invalidate other
            // server-prepared statements just in case.
            deallocateEpoch++;
            if (logger.logDebug()) {
              logger.debug(" FE: received " + error.getSQLState() + ", will invalidate statements. "
                  + "deallocateEpoch is now " + deallocateEpoch);
            }
          }
          // keep processing
          break;

        case 'I': // Empty Query (end of Execute)
          pgStream.receiveInteger4();

          if (logger.logDebug()) {
            logger.debug(" <=BE EmptyQuery");
          }

        {
          ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
          Portal currentPortal = executeData.portal;
          handler.handleCommandStatus("EMPTY", 0, 0);
          if (currentPortal != null) {
            currentPortal.close();
          }
          break;
        }

        case 'N': // Notice Response
          SQLWarning warning = receiveNoticeResponse();
          handler.handleWarning(warning);
          break;

        case 'S': // Parameter Status
        {
          int l_len = pgStream.receiveInteger4();
          String name = pgStream.receiveString();
          String value = pgStream.receiveString();
          if (logger.logDebug()) {
            logger.debug(" <=BE ParameterStatus(" + name + " = " + value + ")");
          }

          if (name.equals("client_encoding") && !value.equalsIgnoreCase("UTF8")
              && !allowEncodingChanges) {
            close(); // we're screwed now; we can't trust any subsequent string.
            handler.handleError(new PSQLException(GT.tr(
                "The server''s client_encoding parameter was changed to {0}. The JDBC driver requires client_encoding to be UTF8 for correct operation.",
                value), PSQLState.CONNECTION_FAILURE));
            endQuery = true;
          }

          if (name.equals("DateStyle") && !value.startsWith("ISO,")) {
            close(); // we're screwed now; we can't trust any subsequent date.
            handler.handleError(new PSQLException(GT.tr(
                "The server''s DateStyle parameter was changed to {0}. The JDBC driver requires DateStyle to begin with ISO for correct operation.",
                value), PSQLState.CONNECTION_FAILURE));
            endQuery = true;
          }

          if (name.equals("standard_conforming_strings")) {
            if (value.equals("on")) {
              setStandardConformingStrings(true);
            } else if (value.equals("off")) {
              setStandardConformingStrings(false);
            } else {
              close();
              // we're screwed now; we don't know how to escape string literals
              handler.handleError(new PSQLException(GT.tr(
                  "The server''s standard_conforming_strings parameter was reported as {0}. The JDBC driver expected on or off.",
                  value), PSQLState.CONNECTION_FAILURE));
              endQuery = true;
            }
          }

          if ("TimeZone".equals(name)) {
            setTimeZone(TimestampUtils.parseBackendTimeZone(value));
          }
          if ("application_name".equals(name)) {
            setApplicationName(value);
          }
          break;
        }

        case 'T': // Row Description (response to Describe)
          Field[] fields = receiveFields();
          tuples = new ArrayList<byte[][]>();

          SimpleQuery query = pendingDescribePortalQueue.peekFirst();
          if (!pendingExecuteQueue.isEmpty() && !pendingExecuteQueue.peekFirst().asSimple) {
            pendingDescribePortalQueue.removeFirst();
          }
          query.setFields(fields);

          if (doneAfterRowDescNoData) {
            DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
            SimpleQuery currentQuery = describeData.query;
            currentQuery.setFields(fields);

            handler.handleResultRows(currentQuery, fields, tuples, null);
            tuples = null;
          }
          break;

        case 'Z': // Ready For Query (eventual response to Sync)
          receiveRFQ();
          if (!pendingExecuteQueue.isEmpty() && pendingExecuteQueue.peekFirst().asSimple) {
            tuples = null;

            ExecuteRequest executeRequest = pendingExecuteQueue.removeFirst();
            // Simple queries might return several resultsets, thus we clear
            // fields, so queries like "select 1;update; select2" will properly
            // identify that "update" did not return any results
            executeRequest.query.setFields(null);

            pendingDescribePortalQueue.removeFirst();
            if (!pendingExecuteQueue.isEmpty()) {
              if (getTransactionState() == TransactionState.IDLE) {
                handler.secureProgress();
              }
              // process subsequent results (e.g. for cases like batched execution of simple 'Q' queries)
              break;
            }
          }
          endQuery = true;

          // Reset the statement name of Parses that failed.
          while (!pendingParseQueue.isEmpty()) {
            SimpleQuery failedQuery = pendingParseQueue.removeFirst();
            failedQuery.unprepare();
          }

          pendingParseQueue.clear(); // No more ParseComplete messages expected.
          pendingDescribeStatementQueue.clear(); // No more ParameterDescription messages expected.
          pendingDescribePortalQueue.clear(); // No more RowDescription messages expected.
          pendingBindQueue.clear(); // No more BindComplete messages expected.
          pendingExecuteQueue.clear(); // No more query executions expected.
          break;

        case 'G': // CopyInResponse
          if (logger.logDebug()) {
            logger.debug(" <=BE CopyInResponse");
            logger.debug(" FE=> CopyFail");
          }

          // COPY sub-protocol is not implemented yet
          // We'll send a CopyFail message for COPY FROM STDIN so that
          // server does not wait for the data.

          byte[] buf =
              Utils.encodeUTF8("The JDBC driver currently does not support COPY operations.");
          pgStream.sendChar('f');
          pgStream.sendInteger4(buf.length + 4 + 1);
          pgStream.send(buf);
          pgStream.sendChar(0);
          pgStream.flush();
          sendSync(); // send sync message
          skipMessage(); // skip the response message
          break;

        case 'H': // CopyOutResponse
          if (logger.logDebug()) {
            logger.debug(" <=BE CopyOutResponse");
          }

          skipMessage();
          // In case of CopyOutResponse, we cannot abort data transfer,
          // so just throw an error and ignore CopyData messages
          handler.handleError(
              new PSQLException(GT.tr("The driver currently does not support COPY operations."),
                  PSQLState.NOT_IMPLEMENTED));
          break;

        case 'c': // CopyDone
          skipMessage();
          if (logger.logDebug()) {
            logger.debug(" <=BE CopyDone");
          }
          break;

        case 'd': // CopyData
          skipMessage();
          if (logger.logDebug()) {
            logger.debug(" <=BE CopyData");
          }
          break;

        default:
          throw new IOException("Unexpected packet type: " + c);
      }

    }
  }

  /**
   * Ignore the response message by reading the message length and skipping over those bytes in the
   * communication stream.
   */
  private void skipMessage() throws IOException {
    int l_len = pgStream.receiveInteger4();
    // skip l_len-4 (length includes the 4 bytes for message length itself
    pgStream.skip(l_len - 4);
  }

  public synchronized void fetch(ResultCursor cursor, ResultHandler handler, int fetchSize)
      throws SQLException {
    waitOnLock();
    final Portal portal = (Portal) cursor;

    // Insert a ResultHandler that turns bare command statuses into empty datasets
    // (if the fetch returns no rows, we see just a CommandStatus..)
    final ResultHandler delegateHandler = handler;
    handler = new ResultHandlerDelegate(delegateHandler) {
      public void handleCommandStatus(String status, int updateCount, long insertOID) {
        handleResultRows(portal.getQuery(), null, new ArrayList<byte[][]>(), null);
      }
    };

    // Now actually run it.

    try {
      processDeadParsedQueries();
      processDeadPortals();

      sendExecute(portal.getQuery(), portal, fetchSize);
      sendSync();

      processResults(handler, 0);
      estimatedReceiveBufferBytes = 0;
    } catch (IOException e) {
      abort();
      handler.handleError(
          new PSQLException(GT.tr("An I/O error occurred while sending to the backend."),
              PSQLState.CONNECTION_FAILURE, e));
    }

    handler.handleCompletion();
  }

  /*
   * Receive the field descriptions from the back end.
   */
  private Field[] receiveFields() throws IOException {
    int l_msgSize = pgStream.receiveInteger4();
    int size = pgStream.receiveInteger2();
    Field[] fields = new Field[size];

    if (logger.logDebug()) {
      logger.debug(" <=BE RowDescription(" + size + ")");
    }

    for (int i = 0; i < fields.length; i++) {
      String columnLabel = pgStream.receiveString();
      int tableOid = pgStream.receiveInteger4();
      short positionInTable = (short) pgStream.receiveInteger2();
      int typeOid = pgStream.receiveInteger4();
      int typeLength = pgStream.receiveInteger2();
      int typeModifier = pgStream.receiveInteger4();
      int formatType = pgStream.receiveInteger2();
      fields[i] = new Field(columnLabel,
          typeOid, typeLength, typeModifier, tableOid, positionInTable);
      fields[i].setFormat(formatType);

      if (logger.logDebug()) {
        logger.debug("        " + fields[i]);
      }
    }

    return fields;
  }

  private void receiveAsyncNotify() throws IOException {
    int msglen = pgStream.receiveInteger4();
    int pid = pgStream.receiveInteger4();
    String msg = pgStream.receiveString();
    String param = pgStream.receiveString();
    addNotification(new org.postgresql.core.Notification(msg, pid, param));

    if (logger.logDebug()) {
      logger.debug(" <=BE AsyncNotify(" + pid + "," + msg + "," + param + ")");
    }
  }

  private SQLException receiveErrorResponse() throws IOException {
    // it's possible to get more than one error message for a query
    // see libpq comments wrt backend closing a connection
    // so, append messages to a string buffer and keep processing
    // check at the bottom to see if we need to throw an exception

    int elen = pgStream.receiveInteger4();
    EncodingPredictor.DecodeResult totalMessage = pgStream.receiveErrorString(elen - 4);
    ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage, logger.getLogLevel());

    if (logger.logDebug()) {
      logger.debug(" <=BE ErrorMessage(" + errorMsg.toString() + ")");
    }

    return new PSQLException(errorMsg);
  }

  private SQLWarning receiveNoticeResponse() throws IOException {
    int nlen = pgStream.receiveInteger4();
    ServerErrorMessage warnMsg =
        new ServerErrorMessage(pgStream.receiveString(nlen - 4), logger.getLogLevel());

    if (logger.logDebug()) {
      logger.debug(" <=BE NoticeResponse(" + warnMsg.toString() + ")");
    }

    return new PSQLWarning(warnMsg);
  }

  private String receiveCommandStatus() throws IOException {
    // TODO: better handle the msg len
    int l_len = pgStream.receiveInteger4();
    // read l_len -5 bytes (-4 for l_len and -1 for trailing \0)
    String status = pgStream.receiveString(l_len - 5);
    // now read and discard the trailing \0
    pgStream.receiveChar(); // Receive(1) would allocate new byte[1], so avoid it

    if (logger.logDebug()) {
      logger.debug(" <=BE CommandStatus(" + status + ")");
    }

    return status;
  }

  private void interpretCommandStatus(String status, ResultHandler handler) {
    int update_count = 0;
    long insert_oid = 0;

    if (status.startsWith("INSERT") || status.startsWith("UPDATE") || status.startsWith("DELETE")
        || status.startsWith("MOVE")) {
      try {
        long updates = Long.parseLong(status.substring(1 + status.lastIndexOf(' ')));

        // deal with situations where the update modifies more than 2^32 rows
        if (updates > Integer.MAX_VALUE) {
          update_count = Statement.SUCCESS_NO_INFO;
        } else {
          update_count = (int) updates;
        }

        if (status.startsWith("INSERT")) {
          insert_oid =
              Long.parseLong(status.substring(1 + status.indexOf(' '), status.lastIndexOf(' ')));
        }
      } catch (NumberFormatException nfe) {
        handler.handleError(new PSQLException(
            GT.tr("Unable to interpret the update count in command completion tag: {0}.", status),
            PSQLState.CONNECTION_FAILURE));
        return;
      }
    }

    handler.handleCommandStatus(status, update_count, insert_oid);
  }

  private void receiveRFQ() throws IOException {
    if (pgStream.receiveInteger4() != 5) {
      throw new IOException("unexpected length of ReadyForQuery message");
    }

    char tStatus = (char) pgStream.receiveChar();
    if (logger.logDebug()) {
      logger.debug(" <=BE ReadyForQuery(" + tStatus + ")");
    }

    // Update connection state.
    switch (tStatus) {
      case 'I':
        setTransactionState(TransactionState.IDLE);
        break;
      case 'T':
        setTransactionState(TransactionState.OPEN);
        break;
      case 'E':
        setTransactionState(TransactionState.FAILED);
        break;
      default:
        throw new IOException(
            "unexpected transaction state in ReadyForQuery message: " + (int) tStatus);
    }
  }

  @Override
  protected void sendCloseMessage() throws IOException {
    pgStream.sendChar('X');
    pgStream.sendInteger4(4);
  }

  public void readStartupMessages() throws IOException, SQLException {
    for (int i = 0; i < 1000; i++) {
      int beresp = pgStream.receiveChar();
      switch (beresp) {
        case 'Z':
          receiveRFQ();
          // Ready For Query; we're done.
          return;

        case 'K':
          // BackendKeyData
          int l_msgLen = pgStream.receiveInteger4();
          if (l_msgLen != 12) {
            throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                PSQLState.PROTOCOL_VIOLATION);
          }

          int pid = pgStream.receiveInteger4();
          int ckey = pgStream.receiveInteger4();

          if (logger.logDebug()) {
            logger.debug(" <=BE BackendKeyData(pid=" + pid + ",ckey=" + ckey + ")");
          }

          setBackendKeyData(pid, ckey);
          break;

        case 'E':
          // Error
          throw receiveErrorResponse();

        case 'N':
          // Warning
          addWarning(receiveNoticeResponse());
          break;

        case 'S':
          // ParameterStatus
          int l_len = pgStream.receiveInteger4();
          String name = pgStream.receiveString();
          String value = pgStream.receiveString();

          if (logger.logDebug()) {
            logger.debug(" <=BE ParameterStatus(" + name + " = " + value + ")");
          }

          if ("server_version_num".equals(name)) {
            setServerVersionNum(Integer.parseInt(value));
          } else if ("server_version".equals(name)) {
            setServerVersion(value);
          } else if ("client_encoding".equals(name)) {
            if (!"UTF8".equals(value)) {
              throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                  PSQLState.PROTOCOL_VIOLATION);
            }
            pgStream.setEncoding(Encoding.getDatabaseEncoding("UTF8"));
          } else if ("standard_conforming_strings".equals(name)) {
            if ("on".equals(value)) {
              setStandardConformingStrings(true);
            } else if ("off".equals(value)) {
              setStandardConformingStrings(false);
            } else {
              throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                  PSQLState.PROTOCOL_VIOLATION);
            }
          } else if ("integer_datetimes".equals(name)) {
            if ("on".equals(value)) {
              setIntegerDateTimes(true);
            } else if ("off".equals(value)) {
              setIntegerDateTimes(false);
            } else {
              throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                  PSQLState.PROTOCOL_VIOLATION);
            }
          }

          break;

        default:
          if (logger.logDebug()) {
            logger.debug("invalid message type=" + (char) beresp);
          }
          throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
              PSQLState.PROTOCOL_VIOLATION);
      }
    }
    throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
        PSQLState.PROTOCOL_VIOLATION);
  }



  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getApplicationName() {
    if (applicationName == null) {
      return "";
    }
    return applicationName;
  }

  @Override
  public ReplicationProtocol getReplicationProtocol() {
    return replicationProtocol;
  }

  @Override
  public boolean useBinaryForReceive(int oid) {
    return useBinaryReceiveForOids.contains(oid);
  }

  @Override
  public void setBinaryReceiveOids(Set<Integer> oids) {
    useBinaryReceiveForOids.clear();
    useBinaryReceiveForOids.addAll(oids);
  }

  @Override
  public boolean useBinaryForSend(int oid) {
    return useBinarySendForOids.contains(oid);
  }

  @Override
  public void setBinarySendOids(Set<Integer> oids) {
    useBinarySendForOids.clear();
    useBinarySendForOids.addAll(oids);
  }

  private void setIntegerDateTimes(boolean state) {
    integerDateTimes = state;
  }

  public boolean getIntegerDateTimes() {
    return integerDateTimes;
  }

  private final Deque<SimpleQuery> pendingParseQueue = new ArrayDeque<SimpleQuery>();
  private final Deque<Portal> pendingBindQueue = new ArrayDeque<Portal>();
  private final Deque<ExecuteRequest> pendingExecuteQueue = new ArrayDeque<ExecuteRequest>();
  private final Deque<DescribeRequest> pendingDescribeStatementQueue =
      new ArrayDeque<DescribeRequest>();
  private final Deque<SimpleQuery> pendingDescribePortalQueue = new ArrayDeque<SimpleQuery>();

  private long nextUniqueID = 1;
  private final boolean allowEncodingChanges;


  /**
   * The estimated server response size since we last consumed the input stream from the server, in
   * bytes.
   *
   * Starts at zero, reset by every Sync message. Mainly used for batches.
   *
   * Used to avoid deadlocks, see MAX_BUFFERED_RECV_BYTES.
   */
  private int estimatedReceiveBufferBytes = 0;

  private final SimpleQuery beginTransactionQuery =
      new SimpleQuery(
          new NativeQuery("BEGIN", new int[0], false, SqlCommand.BLANK),
          null, false);

  private final SimpleQuery EMPTY_QUERY =
      new SimpleQuery(
          new NativeQuery("", new int[0], false,
              SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK)
          ), null, false);

  private final SimpleQuery autoSaveQuery =
      new SimpleQuery(
          new NativeQuery("SAVEPOINT PGJDBC_AUTOSAVE", new int[0], false, SqlCommand.BLANK),
          null, false);

  private final SimpleQuery restoreToAutoSave =
      new SimpleQuery(
          new NativeQuery("ROLLBACK TO SAVEPOINT PGJDBC_AUTOSAVE", new int[0], false, SqlCommand.BLANK),
          null, false);
}
