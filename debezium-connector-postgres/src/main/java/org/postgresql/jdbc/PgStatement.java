/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;


import org.postgresql.Driver;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.Field;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.SqlCommand;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class PgStatement implements Statement, BaseStatement {
  private static final String[] NO_RETURNING_COLUMNS = new String[0];

  /**
   * Default state for use or not binary transfers. Can use only for testing purposes
   */
  private static final boolean DEFAULT_FORCE_BINARY_TRANSFERS =
      Boolean.getBoolean("org.postgresql.forceBinary");
  // only for testing purposes. even single shot statements will use binary transfers
  private boolean forceBinaryTransfers = DEFAULT_FORCE_BINARY_TRANSFERS;

  protected ArrayList<Query> batchStatements = null;
  protected ArrayList<ParameterList> batchParameters = null;
  protected final int resultsettype; // the resultset type to return (ResultSet.TYPE_xxx)
  protected final int concurrency; // is it updateable or not? (ResultSet.CONCUR_xxx)
  private final int rsHoldability;
  private boolean poolable;
  private boolean closeOnCompletion = false;
  protected int fetchdirection = ResultSet.FETCH_FORWARD;
  // fetch direction hint (currently ignored)

  /**
   * Protects current statement from cancelTask starting, waiting for a bit, and waking up exactly
   * on subsequent query execution. The idea is to atomically compare and swap the reference to the
   * task, so the task can detect that statement executes different query than the one the
   * cancelTask was created. Note: the field must be set/get/compareAndSet via
   * {@link #CANCEL_TIMER_UPDATER} as per {@link AtomicReferenceFieldUpdater} javadoc.
   */
  private volatile TimerTask cancelTimerTask = null;
  private static final AtomicReferenceFieldUpdater<PgStatement, TimerTask> CANCEL_TIMER_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(PgStatement.class, TimerTask.class, "cancelTimerTask");

  /**
   * Protects statement from out-of-order cancels. It protects from both
   * {@link #setQueryTimeout(int)} and {@link #cancel()} induced ones.
   *
   * {@link #execute(String)} and friends change the field to
   * {@link StatementCancelState#IN_QUERY} during execute. {@link #cancel()}
   * ignores cancel request if state is {@link StatementCancelState#IDLE}.
   * In case {@link #execute(String)} observes non-{@link StatementCancelState#IDLE} state as it
   * completes the query, it waits till {@link StatementCancelState#CANCELLED}. Note: the field must be
   * set/get/compareAndSet via {@link #STATE_UPDATER} as per {@link AtomicIntegerFieldUpdater}
   * javadoc.
   */
  private volatile StatementCancelState statementState = StatementCancelState.IDLE;

  private static final AtomicReferenceFieldUpdater<PgStatement, StatementCancelState> STATE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(PgStatement.class, StatementCancelState.class, "statementState");

  /**
   * Does the caller of execute/executeUpdate want generated keys for this execution? This is set by
   * Statement methods that have generated keys arguments and cleared after execution is complete.
   */
  protected boolean wantsGeneratedKeysOnce = false;

  /**
   * Was this PreparedStatement created to return generated keys for every execution? This is set at
   * creation time and never cleared by execution.
   */
  public boolean wantsGeneratedKeysAlways = false;

  // The connection who created us
  protected final BaseConnection connection;

  /**
   * The warnings chain.
   */
  protected SQLWarning warnings = null;
  /**
   * The last warning of the warning chain.
   */
  protected SQLWarning lastWarning = null;

  /**
   * Maximum number of rows to return, 0 = unlimited
   */
  protected int maxrows = 0;

  /**
   * Number of rows to get in a batch.
   */
  protected int fetchSize = 0;

  /**
   * Timeout (in milliseconds) for a query
   */
  protected int timeout = 0;

  protected boolean replaceProcessingEnabled = true;

  /**
   * The current results.
   */
  protected ResultWrapper result = null;

  /**
   * The first unclosed result.
   */
  protected ResultWrapper firstUnclosedResult = null;

  /**
   * Results returned by a statement that wants generated keys.
   */
  protected ResultWrapper generatedKeys = null;

  protected int m_prepareThreshold; // Reuse threshold to enable use of PREPARE

  protected int maxfieldSize = 0;

  PgStatement(PgConnection c, int rsType, int rsConcurrency, int rsHoldability)
      throws SQLException {
    this.connection = c;
    forceBinaryTransfers |= c.getForceBinary();
    resultsettype = rsType;
    concurrency = rsConcurrency;
    setFetchSize(c.getDefaultFetchSize());
    setPrepareThreshold(c.getPrepareThreshold());
    this.rsHoldability = rsHoldability;
  }

  public ResultSet createResultSet(Query originalQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) throws SQLException {
    PgResultSet newResult = new PgResultSet(originalQuery, this, fields, tuples, cursor,
        getMaxRows(), getMaxFieldSize(), getResultSetType(), getResultSetConcurrency(),
        getResultSetHoldability());
    newResult.setFetchSize(getFetchSize());
    newResult.setFetchDirection(getFetchDirection());
    return newResult;
  }

  public BaseConnection getPGConnection() {
    return connection;
  }

  public String getFetchingCursorName() {
    return null;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  protected boolean wantsScrollableResultSet() {
    return resultsettype != ResultSet.TYPE_FORWARD_ONLY;
  }

  protected boolean wantsHoldableResultSet() {
    // FIXME: false if not supported
    return rsHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  /**
   * ResultHandler implementations for updates, queries, and either-or.
   */
  public class StatementResultHandler extends ResultHandlerBase {
    private ResultWrapper results;
    private ResultWrapper lastResult;

    ResultWrapper getResults() {
      return results;
    }

    private void append(ResultWrapper newResult) {
      if (results == null) {
        lastResult = results = newResult;
      } else {
        lastResult.append(newResult);
      }
    }

    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
        ResultCursor cursor) {
      try {
        ResultSet rs = PgStatement.this.createResultSet(fromQuery, fields, tuples, cursor);
        append(new ResultWrapper(rs));
      } catch (SQLException e) {
        handleError(e);
      }
    }

    @Override
    public void handleCommandStatus(String status, int updateCount, long insertOID) {
      append(new ResultWrapper(updateCount, insertOID));
    }

    @Override
    public void handleCompletion() throws SQLException {
      SQLWarning warning = getWarning();
      if (warning != null) {
        PgStatement.this.addWarning(warning);
      }
      super.handleCompletion();
    }
  }

  public java.sql.ResultSet executeQuery(String p_sql) throws SQLException {
    if (!executeWithFlags(p_sql, 0)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    if (result.getNext() != null) {
      throw new PSQLException(GT.tr("Multiple ResultSets were returned by the query."),
          PSQLState.TOO_MANY_RESULTS);
    }

    return result.getResultSet();
  }

  public int executeUpdate(String p_sql) throws SQLException {
    executeWithFlags(p_sql, QueryExecutor.QUERY_NO_RESULTS);

    ResultWrapper iter = result;
    while (iter != null) {
      if (iter.getResultSet() != null) {
        throw new PSQLException(GT.tr("A result was returned when none was expected."),
            PSQLState.TOO_MANY_RESULTS);

      }
      iter = iter.getNext();
    }

    return getUpdateCount();
  }

  public boolean execute(String p_sql) throws SQLException {
    return executeWithFlags(p_sql, 0);
  }

  public boolean executeWithFlags(String sql, int flags) throws SQLException {
    return executeCachedSql(sql, flags, NO_RETURNING_COLUMNS);
  }

  private boolean executeCachedSql(String sql, int flags, String[] columnNames) throws SQLException {
    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    boolean shouldUseParameterized = preferQueryMode.compareTo(PreferQueryMode.EXTENDED) >= 0;
    QueryExecutor queryExecutor = connection.getQueryExecutor();
    Object key = queryExecutor
        .createQueryKey(sql, replaceProcessingEnabled, shouldUseParameterized, columnNames);
    CachedQuery cachedQuery;
    boolean shouldCache = preferQueryMode == PreferQueryMode.EXTENDED_CACHE_EVERYTING;
    if (shouldCache) {
      cachedQuery = queryExecutor.borrowQueryByKey(key);
    } else {
      cachedQuery = queryExecutor.createQueryByKey(key);
    }
    if (wantsGeneratedKeysOnce) {
      SqlCommand sqlCommand = cachedQuery.query.getSqlCommand();
      wantsGeneratedKeysOnce = sqlCommand != null && sqlCommand.isReturningKeywordPresent();
    }
    boolean res;
    try {
      res = executeWithFlags(cachedQuery, flags);
    } finally {
      if (shouldCache) {
        queryExecutor.releaseQuery(cachedQuery);
      }
    }
    return res;
  }

  public boolean executeWithFlags(CachedQuery simpleQuery, int flags) throws SQLException {
    checkClosed();
    if (connection.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) < 0) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }
    execute(simpleQuery, null, flags);
    return (result != null && result.getResultSet() != null);
  }

  public boolean executeWithFlags(int flags) throws SQLException {
    checkClosed();
    throw new PSQLException(GT.tr("Can''t use executeWithFlags(int) on a Statement."),
        PSQLState.WRONG_OBJECT_TYPE);
  }

  protected void closeForNextExecution() throws SQLException {
    // Every statement execution clears any previous warnings.
    clearWarnings();

    // Close any existing resultsets associated with this statement.
    while (firstUnclosedResult != null) {
      ResultSet rs = firstUnclosedResult.getResultSet();
      if (rs != null) {
        rs.close();
      }
      firstUnclosedResult = firstUnclosedResult.getNext();
    }
    result = null;

    if (generatedKeys != null) {
      if (generatedKeys.getResultSet() != null) {
        generatedKeys.getResultSet().close();
      }
      generatedKeys = null;
    }
  }

  /**
   * Returns true if query is unlikely to be reused
   *
   * @param cachedQuery to check (null if current query)
   * @return true if query is unlikely to be reused
   */
  protected boolean isOneShotQuery(CachedQuery cachedQuery) {
    if (cachedQuery == null) {
      return true;
    }
    cachedQuery.increaseExecuteCount();
    if ((m_prepareThreshold == 0 || cachedQuery.getExecuteCount() < m_prepareThreshold)
        && !getForceBinaryTransfer()) {
      return true;
    }
    return false;
  }

  protected final void execute(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    try {
      executeInternal(cachedQuery, queryParameters, flags);
    } catch (SQLException e) {
      // Don't retry composite queries as it might get partially executed
      if (cachedQuery.query.getSubqueries() != null
          || !connection.getQueryExecutor().willHealOnRetry(e)) {
        throw e;
      }
      cachedQuery.query.close();
      // Execute the query one more time
      executeInternal(cachedQuery, queryParameters, flags);
    }
  }

  private void executeInternal(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    closeForNextExecution();

    // Enable cursor-based resultset if possible.
    if (fetchSize > 0 && !wantsScrollableResultSet() && !connection.getAutoCommit()
        && !wantsHoldableResultSet()) {
      flags |= QueryExecutor.QUERY_FORWARD_CURSOR;
    }

    if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
      flags |= QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS;

      // If the no results flag is set (from executeUpdate)
      // clear it so we get the generated keys results.
      //
      if ((flags & QueryExecutor.QUERY_NO_RESULTS) != 0) {
        flags &= ~(QueryExecutor.QUERY_NO_RESULTS);
      }
    }

    if (isOneShotQuery(cachedQuery)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    }
    // Only use named statements after we hit the threshold. Note that only
    // named statements can be transferred in binary format.

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    // updateable result sets do not yet support binary updates
    if (concurrency != ResultSet.CONCUR_READ_ONLY) {
      flags |= QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    }

    Query queryToExecute = cachedQuery.query;

    if (queryToExecute.isEmpty()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    if (!queryToExecute.isStatementDescribed() && forceBinaryTransfers
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Simple 'Q' execution does not need to know parameter types
      // When binaryTransfer is forced, then we need to know resulting parameter and column types,
      // thus sending a describe request.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler();
      connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler2, 0, 0,
          flags2);
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    StatementResultHandler handler = new StatementResultHandler();
    result = null;
    try {
      startTimer();
      connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler, maxrows,
          fetchSize, flags);
    } finally {
      killTimerTask();
    }
    result = firstUnclosedResult = handler.getResults();

    if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
      generatedKeys = result;
      result = result.getNext();

      if (wantsGeneratedKeysOnce) {
        wantsGeneratedKeysOnce = false;
      }
    }

  }

  public void setCursorName(String name) throws SQLException {
    checkClosed();
    // No-op.
  }

  // This is intentionally non-volatile to avoid performance hit in isClosed checks
  // see #close()
  protected boolean isClosed = false;

  public int getUpdateCount() throws SQLException {
    checkClosed();
    if (result == null || result.getResultSet() != null) {
      return -1;
    }

    return result.getUpdateCount();
  }

  public boolean getMoreResults() throws SQLException {
    if (result == null) {
      return false;
    }

    result = result.getNext();

    // Close preceding resultsets.
    while (firstUnclosedResult != result) {
      if (firstUnclosedResult.getResultSet() != null) {
        firstUnclosedResult.getResultSet().close();
      }
      firstUnclosedResult = firstUnclosedResult.getNext();
    }

    return (result != null && result.getResultSet() != null);
  }

  public int getMaxRows() throws SQLException {
    checkClosed();
    return maxrows;
  }

  public void setMaxRows(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw new PSQLException(
          GT.tr("Maximum number of rows must be a value grater than or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    maxrows = max;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    checkClosed();
    replaceProcessingEnabled = enable;
  }

  public int getQueryTimeout() throws SQLException {
    return getQueryTimeoutMs() / 1000;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    setQueryTimeoutMs(seconds * 1000);
  }

  /**
   * The queryTimeout limit is the number of milliseconds the driver will wait for a Statement to
   * execute. If the limit is exceeded, a SQLException is thrown.
   *
   * @return the current query timeout limit in milliseconds; 0 = unlimited
   * @throws SQLException if a database access error occurs
   */
  public int getQueryTimeoutMs() throws SQLException {
    checkClosed();
    return timeout;
  }

  /**
   * Sets the queryTimeout limit
   *
   * @param millis - the new query timeout limit in milliseconds
   * @throws SQLException if a database access error occurs
   */
  public void setQueryTimeoutMs(int millis) throws SQLException {
    checkClosed();

    if (millis < 0) {
      throw new PSQLException(GT.tr("Query timeout must be a value greater than or equals to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    timeout = millis;
  }

  /**
   * This adds a warning to the warning chain. We track the tail of the warning chain as well to
   * avoid O(N) behavior for adding a new warning to an existing chain. Some server functions which
   * RAISE NOTICE (or equivalent) produce a ton of warnings.
   *
   * @param warn warning to add
   */
  public void addWarning(SQLWarning warn) {
    if (warnings == null) {
      warnings = warn;
      lastWarning = warn;
    } else {
      lastWarning.setNextWarning(warn);
      lastWarning = warn;
    }
  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    return warnings;
  }

  public int getMaxFieldSize() throws SQLException {
    return maxfieldSize;
  }

  public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw new PSQLException(
          GT.tr("The maximum field size must be a value greater than or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    maxfieldSize = max;
  }

  public void clearWarnings() throws SQLException {
    warnings = null;
    lastWarning = null;
  }

  public java.sql.ResultSet getResultSet() throws SQLException {
    checkClosed();

    if (result == null) {
      return null;
    }

    return result.getResultSet();
  }

  /**
   * <B>Note:</B> even though {@code Statement} is automatically closed when it is garbage
   * collected, it is better to close it explicitly to lower resource consumption.
   *
   * {@inheritDoc}
   */
  public void close() throws SQLException {
    // closing an already closed Statement is a no-op.
    if (isClosed) {
      return;
    }

    cleanupTimer();

    closeForNextExecution();

    isClosed = true;
  }

  /*
   *
   * The following methods are postgres extensions and are defined in the interface BaseStatement
   *
   */

  public long getLastOID() throws SQLException {
    checkClosed();
    if (result == null) {
      return 0;
    }
    return result.getInsertOID();
  }

  public void setPrepareThreshold(int newThreshold) throws SQLException {
    checkClosed();

    if (newThreshold < 0) {
      forceBinaryTransfers = true;
      newThreshold = 1;
    }

    this.m_prepareThreshold = newThreshold;
  }

  public int getPrepareThreshold() {
    return m_prepareThreshold;
  }

  public void setUseServerPrepare(boolean flag) throws SQLException {
    setPrepareThreshold(flag ? 1 : 0);
  }

  public boolean isUseServerPrepare() {
    return false;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed) {
      throw new PSQLException(GT.tr("This statement has been closed."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
  }

  // ** JDBC 2 Extensions **

  public void addBatch(String p_sql) throws SQLException {
    checkClosed();

    if (batchStatements == null) {
      batchStatements = new ArrayList<Query>();
      batchParameters = new ArrayList<ParameterList>();
    }

    boolean shouldUseParameterized = connection.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) >= 0;
    CachedQuery cachedQuery = connection.createQuery(p_sql, replaceProcessingEnabled, shouldUseParameterized);
    batchStatements.add(cachedQuery.query);
    batchParameters.add(null);
  }

  public void clearBatch() throws SQLException {
    if (batchStatements != null) {
      batchStatements.clear();
      batchParameters.clear();
    }
  }

  protected BatchResultHandler createBatchHandler(Query[] queries,
      ParameterList[] parameterLists) {
    return new BatchResultHandler(this, queries, parameterLists,
        wantsGeneratedKeysAlways);
  }

  public int[] executeBatch() throws SQLException {
    checkClosed();

    closeForNextExecution();

    if (batchStatements == null || batchStatements.isEmpty()) {
      return new int[0];
    }

    // Construct query/parameter arrays.
    transformQueriesAndParameters();
    // Empty arrays should be passed to toArray
    // see http://shipilev.net/blog/2016/arrays-wisdom-ancients/
    Query[] queries = batchStatements.toArray(new Query[0]);
    ParameterList[] parameterLists =
        batchParameters.toArray(new ParameterList[0]);
    batchStatements.clear();
    batchParameters.clear();

    int flags = 0;

    // Force a Describe before any execution? We need to do this if we're going
    // to send anything dependent on the Describe results, e.g. binary parameters.
    boolean preDescribe = false;

    if (wantsGeneratedKeysAlways) {
      /*
       * This batch will return generated keys, tell the executor to expect result rows. We also
       * force a Describe later so we know the size of the results to expect.
       *
       * If the parameter type(s) change between batch entries and the default binary-mode changes
       * we might get mixed binary and text in a single result set column, which we cannot handle.
       * To prevent this, disable binary transfer mode in batches that return generated keys. See
       * GitHub issue #267
       */
      flags = QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS | QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    } else {
      // If a batch hasn't specified that it wants generated keys, using the appropriate
      // Connection.createStatement(...) interfaces, disallow any result set.
      flags = QueryExecutor.QUERY_NO_RESULTS;
    }

    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    if (preferQueryMode == PreferQueryMode.SIMPLE
        || (preferQueryMode == PreferQueryMode.EXTENDED_FOR_PREPARED
        && parameterLists[0] == null)) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }

    boolean sameQueryAhead = queries.length > 1 && queries[0] == queries[1];

    if (!sameQueryAhead
        // If executing the same query twice in a batch, make sure the statement
        // is server-prepared. In other words, "oneshot" only if the query is one in the batch
        // or the queries are different
        && isOneShotQuery(null)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    } else {
      // If a batch requests generated keys and isn't already described,
      // force a Describe of the query before proceeding. That way we can
      // determine the appropriate size of each batch by estimating the
      // maximum data returned. Without that, we don't know how many queries
      // we'll be able to queue up before we risk a deadlock.
      // (see v3.QueryExecutorImpl's MAX_BUFFERED_RECV_BYTES)

      // SameQueryAhead is just a quick way to issue pre-describe for batch execution
      // TODO: It should be reworked into "pre-describe if query has unknown parameter
      // types and same query is ahead".
      preDescribe = (wantsGeneratedKeysAlways || sameQueryAhead)
          && !queries[0].isStatementDescribed();
      /*
       * It's also necessary to force a Describe on the first execution of the new statement, even
       * though we already described it, to work around bug #267.
       */
      flags |= QueryExecutor.QUERY_FORCE_DESCRIBE_PORTAL;
    }

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    BatchResultHandler handler;
    handler = createBatchHandler(queries, parameterLists);

    if ((preDescribe || forceBinaryTransfers)
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Do a client-server round trip, parsing and describing the query so we
      // can determine its result types for use in binary parameters, batch sizing,
      // etc.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler();
      try {
        connection.getQueryExecutor().execute(queries[0], parameterLists[0], handler2, 0, 0, flags2);
      } catch (SQLException e) {
        // Unable to parse the first statement -> throw BatchUpdateException
        handler.handleError(e);
        handler.handleCompletion();
        // Will not reach here (see above)
      }
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    result = null;

    try {
      startTimer();
      connection.getQueryExecutor().execute(queries, parameterLists, handler, maxrows, fetchSize,
          flags);
    } finally {
      killTimerTask();
      // There might be some rows generated even in case of failures
      if (wantsGeneratedKeysAlways) {
        generatedKeys = new ResultWrapper(handler.getGeneratedKeys());
      }
    }

    return handler.getUpdateCount();
  }

  public void cancel() throws SQLException {
    if (!STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.CANCELING)) {
      // Not in query, there's nothing to cancel
      return;
    }
    try {
      // Synchronize on connection to avoid spinning in killTimerTask
      synchronized (connection) {
        connection.cancelQuery();
      }
    } finally {
      STATE_UPDATER.set(this, StatementCancelState.CANCELLED);
      synchronized (connection) {
        connection.notifyAll(); // wake-up killTimerTask
      }
    }
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public int getFetchDirection() {
    return fetchdirection;
  }

  public int getResultSetConcurrency() {
    return concurrency;
  }

  public int getResultSetType() {
    return resultsettype;
  }

  public void setFetchDirection(int direction) throws SQLException {
    switch (direction) {
      case ResultSet.FETCH_FORWARD:
      case ResultSet.FETCH_REVERSE:
      case ResultSet.FETCH_UNKNOWN:
        fetchdirection = direction;
        break;
      default:
        throw new PSQLException(GT.tr("Invalid fetch direction constant: {0}.", direction),
            PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    if (rows < 0) {
      throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    fetchSize = rows;
  }

  private void startTimer() {
    /*
     * there shouldn't be any previous timer active, but better safe than sorry.
     */
    cleanupTimer();

    STATE_UPDATER.set(this, StatementCancelState.IN_QUERY);

    if (timeout == 0) {
      return;
    }

    TimerTask cancelTask = new TimerTask() {
      public void run() {
        try {
          if (!CANCEL_TIMER_UPDATER.compareAndSet(PgStatement.this, this, null)) {
            // Nothing to do here, statement has already finished and cleared
            // cancelTimerTask reference
            return;
          }
          PgStatement.this.cancel();
        } catch (SQLException e) {
        }
      }
    };

    CANCEL_TIMER_UPDATER.set(this, cancelTask);
    connection.addTimerTask(cancelTask, timeout);
  }

  /**
   * Clears {@link #cancelTimerTask} if any. Returns true if and only if "cancel" timer task would
   * never invoke {@link #cancel()}.
   */
  private boolean cleanupTimer() {
    TimerTask timerTask = CANCEL_TIMER_UPDATER.get(this);
    if (timerTask == null) {
      // If timeout is zero, then timer task did not exist, so we safely report "all clear"
      return timeout == 0;
    }
    if (!CANCEL_TIMER_UPDATER.compareAndSet(this, timerTask, null)) {
      // Failed to update reference -> timer has just fired, so we must wait for the query state to
      // become "cancelling".
      return false;
    }
    timerTask.cancel();
    connection.purgeTimerTasks();
    // All clear
    return true;
  }

  private void killTimerTask() {
    boolean timerTaskIsClear = cleanupTimer();
    // The order is important here: in case we need to wait for the cancel task, the state must be
    // kept StatementCancelState.IN_QUERY, so cancelTask would be able to cancel the query.
    // It is believed that this case is very rare, so "additional cancel and wait below" would not
    // harm it.
    if (timerTaskIsClear && STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.IDLE)) {
      return;
    }

    // Being here means someone managed to call .cancel() and our connection did not receive
    // "timeout error"
    // We wait till state becomes "cancelled"
    boolean interrupted = false;
    while (!STATE_UPDATER.compareAndSet(this, StatementCancelState.CANCELLED, StatementCancelState.IDLE)) {
      synchronized (connection) {
        try {
          // Note: wait timeout here is irrelevant since synchronized(connection) would block until
          // .cancel finishes
          connection.wait(10);
        } catch (InterruptedException e) { // NOSONAR
          // Either re-interrupt this method or rethrow the "InterruptedException"
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  protected boolean getForceBinaryTransfer() {
    return forceBinaryTransfers;
  }

  public long getLargeUpdateCount() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getLargeUpdateCount");
  }

  public void setLargeMaxRows(long max) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setLargeMaxRows");
  }

  public long getLargeMaxRows() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getLargeMaxRows");
  }

  public long[] executeLargeBatch() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeBatch");
  }

  public long executeLargeUpdate(String sql) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
  }

  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
  }

  public long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
  }

  public long executeLargeUpdate(String sql, String columnNames[]) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
  }

  public long executeLargeUpdate() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
  }

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public void setPoolable(boolean poolable) throws SQLException {
    checkClosed();
    this.poolable = poolable;
  }

  public boolean isPoolable() throws SQLException {
    checkClosed();
    return poolable;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw Driver.notImplemented(this.getClass(), "getParentLogger()");
  }

  public void closeOnCompletion() throws SQLException {
    closeOnCompletion = true;
  }

  public boolean isCloseOnCompletion() throws SQLException {
    return closeOnCompletion;
  }

  protected void checkCompletion() throws SQLException {
    if (!closeOnCompletion) {
      return;
    }

    ResultWrapper result = firstUnclosedResult;
    while (result != null) {
      if (result.getResultSet() != null && !result.getResultSet().isClosed()) {
        return;
      }
      result = result.getNext();
    }

    // prevent all ResultSet.close arising from Statement.close to loop here
    closeOnCompletion = false;
    try {
      close();
    } finally {
      // restore the status if one rely on isCloseOnCompletion
      closeOnCompletion = true;
    }
  }

  public boolean getMoreResults(int current) throws SQLException {
    // CLOSE_CURRENT_RESULT
    if (current == Statement.CLOSE_CURRENT_RESULT && result != null
        && result.getResultSet() != null) {
      result.getResultSet().close();
    }

    // Advance resultset.
    if (result != null) {
      result = result.getNext();
    }

    // CLOSE_ALL_RESULTS
    if (current == Statement.CLOSE_ALL_RESULTS) {
      // Close preceding resultsets.
      while (firstUnclosedResult != result) {
        if (firstUnclosedResult.getResultSet() != null) {
          firstUnclosedResult.getResultSet().close();
        }
        firstUnclosedResult = firstUnclosedResult.getNext();
      }
    }

    // Done.
    return (result != null && result.getResultSet() != null);
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    checkClosed();
    if (generatedKeys == null || generatedKeys.getResultSet() == null) {
      return createDriverResultSet(new Field[0], new ArrayList<byte[][]>());
    }

    return generatedKeys.getResultSet();
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeUpdate(sql);
    }

    return executeUpdate(sql, (String[]) null);
  }

  public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
    if (columnIndexes == null || columnIndexes.length == 0) {
      return executeUpdate(sql);
    }

    throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
        PSQLState.NOT_IMPLEMENTED);
  }

  public int executeUpdate(String sql, String columnNames[]) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return executeUpdate(sql);
    }

    wantsGeneratedKeysOnce = true;
    if (!executeCachedSql(sql, 0, columnNames)) {
      // no resultset returned. What's a pity!
    }
    return getUpdateCount();
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    }
    return execute(sql, (String[]) null);
  }

  public boolean execute(String sql, int columnIndexes[]) throws SQLException {
    if (columnIndexes != null && columnIndexes.length == 0) {
      return execute(sql);
    }

    throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
        PSQLState.NOT_IMPLEMENTED);
  }

  public boolean execute(String sql, String columnNames[]) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return execute(sql);
    }

    wantsGeneratedKeysOnce = true;
    return executeCachedSql(sql, 0, columnNames);
  }

  public int getResultSetHoldability() throws SQLException {
    return rsHoldability;
  }

  public ResultSet createDriverResultSet(Field[] fields, List<byte[][]> tuples)
      throws SQLException {
    return createResultSet(null, fields, tuples, null);
  }

  protected void transformQueriesAndParameters() throws SQLException {
  }

}
