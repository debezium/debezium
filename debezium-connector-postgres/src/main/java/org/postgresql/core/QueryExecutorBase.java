package org.postgresql.core;

import org.postgresql.PGNotification;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.HostSpec;
import org.postgresql.util.LruCache;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Properties;

public abstract class QueryExecutorBase implements QueryExecutor {
  protected final Logger logger;
  protected final PGStream pgStream;
  private final String user;
  private final String database;
  private final int cancelSignalTimeout;

  private int cancelPid;
  private int cancelKey;
  private boolean closed = false;
  private String serverVersion;
  private int serverVersionNum = 0;
  private TransactionState transactionState;
  private final boolean reWriteBatchedInserts;
  private final boolean columnSanitiserDisabled;
  private final PreferQueryMode preferQueryMode;
  private AutoSave autoSave;
  private boolean flushCacheOnDeallocate = true;

  // default value for server versions that don't report standard_conforming_strings
  private boolean standardConformingStrings = false;

  private SQLWarning warnings;
  private final ArrayList<PGNotification> notifications = new ArrayList<PGNotification>();

  private final LruCache<Object, CachedQuery> statementCache;
  private final CachedQueryCreateAction cachedQueryCreateAction;

  protected QueryExecutorBase(Logger logger, PGStream pgStream, String user, String database,
      int cancelSignalTimeout, Properties info) throws SQLException {
    this.logger = logger;
    this.pgStream = pgStream;
    this.user = user;
    this.database = database;
    this.cancelSignalTimeout = cancelSignalTimeout;
    this.reWriteBatchedInserts = PGProperty.REWRITE_BATCHED_INSERTS.getBoolean(info);
    this.columnSanitiserDisabled = PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(info);
    String preferMode = PGProperty.PREFER_QUERY_MODE.get(info);
    this.preferQueryMode = PreferQueryMode.of(preferMode);
    this.autoSave = AutoSave.of(PGProperty.AUTOSAVE.get(info));
    this.cachedQueryCreateAction = new CachedQueryCreateAction(this);
    statementCache = new LruCache<Object, CachedQuery>(
        Math.max(0, PGProperty.PREPARED_STATEMENT_CACHE_QUERIES.getInt(info)),
        Math.max(0, PGProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.getInt(info) * 1024 * 1024),
        false,
        cachedQueryCreateAction,
        new LruCache.EvictAction<CachedQuery>() {
          @Override
          public void evict(CachedQuery cachedQuery) throws SQLException {
            cachedQuery.query.close();
          }
        });
  }

  protected abstract void sendCloseMessage() throws IOException;

  @Override
  public HostSpec getHostSpec() {
    return pgStream.getHostSpec();
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  public void setBackendKeyData(int cancelPid, int cancelKey) {
    this.cancelPid = cancelPid;
    this.cancelKey = cancelKey;
  }

  @Override
  public int getBackendPID() {
    return cancelPid;
  }

  @Override
  public void abort() {
    try {
      pgStream.getSocket().close();
    } catch (IOException e) {
      // ignore
    }
    closed = true;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    try {
      if (logger.logDebug()) {
        logger.debug(" FE=> Terminate");
      }
      sendCloseMessage();
      pgStream.flush();
      pgStream.close();
    } catch (IOException ioe) {
      // Forget it.
      if (logger.logDebug()) {
        logger.debug("Discarding IOException on close:", ioe);
      }
    }

    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void sendQueryCancel() throws SQLException {
    if (cancelPid <= 0) {
      return;
    }

    PGStream cancelStream = null;

    // Now we need to construct and send a cancel packet
    try {
      if (logger.logDebug()) {
        logger.debug(" FE=> CancelRequest(pid=" + cancelPid + ",ckey=" + cancelKey + ")");
      }

      cancelStream =
          new PGStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), cancelSignalTimeout);
      if (cancelSignalTimeout > 0) {
        cancelStream.getSocket().setSoTimeout(cancelSignalTimeout);
      }
      cancelStream.sendInteger4(16);
      cancelStream.sendInteger2(1234);
      cancelStream.sendInteger2(5678);
      cancelStream.sendInteger4(cancelPid);
      cancelStream.sendInteger4(cancelKey);
      cancelStream.flush();
      cancelStream.receiveEOF();
    } catch (IOException e) {
      // Safe to ignore.
      if (logger.logDebug()) {
        logger.debug("Ignoring exception on cancel request:", e);
      }
    } finally {
      if (cancelStream != null) {
        try {
          cancelStream.close();
        } catch (IOException e) {
          // Ignored.
        }
      }
    }
  }

  public synchronized void addWarning(SQLWarning newWarning) {
    if (warnings == null) {
      warnings = newWarning;
    } else {
      warnings.setNextWarning(newWarning);
    }
  }

  public synchronized void addNotification(PGNotification notification) {
    notifications.add(notification);
  }

  @Override
  public synchronized PGNotification[] getNotifications() throws SQLException {
    PGNotification[] array = notifications.toArray(new PGNotification[notifications.size()]);
    notifications.clear();
    return array;
  }

  @Override
  public synchronized SQLWarning getWarnings() {
    SQLWarning chain = warnings;
    warnings = null;
    return chain;
  }

  @Override
  public String getServerVersion() {
    return serverVersion;
  }

  @Override
  public int getServerVersionNum() {
    if (serverVersionNum != 0) {
      return serverVersionNum;
    }
    return serverVersionNum = Utils.parseServerVersionStr(serverVersion);
  }

  public void setServerVersion(String serverVersion) {
    this.serverVersion = serverVersion;
  }

  public void setServerVersionNum(int serverVersionNum) {
    this.serverVersionNum = serverVersionNum;
  }

  public synchronized void setTransactionState(TransactionState state) {
    transactionState = state;
  }

  public synchronized void setStandardConformingStrings(boolean value) {
    standardConformingStrings = value;
  }

  @Override
  public synchronized boolean getStandardConformingStrings() {
    return standardConformingStrings;
  }

  @Override
  public synchronized TransactionState getTransactionState() {
    return transactionState;
  }

  public void setEncoding(Encoding encoding) throws IOException {
    pgStream.setEncoding(encoding);
  }

  @Override
  public Encoding getEncoding() {
    return pgStream.getEncoding();
  }

  @Override
  public boolean isReWriteBatchedInsertsEnabled() {
    return this.reWriteBatchedInserts;
  }

  @Override
  public final CachedQuery borrowQuery(String sql) throws SQLException {
    return statementCache.borrow(sql);
  }

  @Override
  public final CachedQuery borrowCallableQuery(String sql) throws SQLException {
    return statementCache.borrow(new CallableQueryKey(sql));
  }

  @Override
  public final CachedQuery borrowReturningQuery(String sql, String[] columnNames) throws SQLException {
    return statementCache.borrow(new QueryWithReturningColumnsKey(sql, true, true,
        columnNames
    ));
  }

  @Override
  public CachedQuery borrowQueryByKey(Object key) throws SQLException {
    return statementCache.borrow(key);
  }

  @Override
  public void releaseQuery(CachedQuery cachedQuery) {
    statementCache.put(cachedQuery.key, cachedQuery);
  }

  @Override
  public final Object createQueryKey(String sql, boolean escapeProcessing,
      boolean isParameterized, String... columnNames) {
    Object key;
    if (columnNames == null || columnNames.length != 0) {
      // Null means "return whatever sensible columns are" (e.g. primary key, or serial, or something like that)
      key = new QueryWithReturningColumnsKey(sql, isParameterized, escapeProcessing, columnNames);
    } else if (isParameterized) {
      // If no generated columns requested, just use the SQL as a cache key
      key = sql;
    } else {
      key = new BaseQueryKey(sql, false, escapeProcessing);
    }
    return key;
  }

  @Override
  public CachedQuery createQueryByKey(Object key) throws SQLException {
    return cachedQueryCreateAction.create(key);
  }

  @Override
  public final CachedQuery createQuery(String sql, boolean escapeProcessing,
      boolean isParameterized, String... columnNames)
      throws SQLException {
    Object key = createQueryKey(sql, escapeProcessing, isParameterized, columnNames);
    // Note: cache is not reused here for two reasons:
    //   1) Simplify initial implementation for simple statements
    //   2) Non-prepared statements are likely to have literals, thus query reuse would not be often
    return createQueryByKey(key);
  }

  @Override
  public boolean isColumnSanitiserDisabled() {
    return columnSanitiserDisabled;
  }

  @Override
  public PreferQueryMode getPreferQueryMode() {
    return preferQueryMode;
  }

  public AutoSave getAutoSave() {
    return autoSave;
  }

  public void setAutoSave(AutoSave autoSave) {
    this.autoSave = autoSave;
  }

  protected boolean willHealViaReparse(SQLException e) {
    // "prepared statement \"S_2\" does not exist"
    if (PSQLState.INVALID_SQL_STATEMENT_NAME.getState().equals(e.getSQLState())) {
      return true;
    }
    if (!PSQLState.NOT_IMPLEMENTED.getState().equals(e.getSQLState())) {
      return false;
    }

    if (!(e instanceof PSQLException)) {
      return false;
    }

    PSQLException pe = (PSQLException) e;

    ServerErrorMessage serverErrorMessage = pe.getServerErrorMessage();
    if (serverErrorMessage == null) {
      return false;
    }
    // "cached plan must not change result type"
    String routine = pe.getServerErrorMessage().getRoutine();
    return "RevalidateCachedQuery".equals(routine) // 9.2+
        || "RevalidateCachedPlan".equals(routine); // <= 9.1
  }

  @Override
  public boolean willHealOnRetry(SQLException e) {
    if (autoSave == AutoSave.NEVER && getTransactionState() == TransactionState.FAILED) {
      // If autorollback is not activated, then every statement will fail with
      // 'transaction is aborted', etc, etc
      return false;
    }
    return willHealViaReparse(e);
  }

  public boolean isFlushCacheOnDeallocate() {
    return flushCacheOnDeallocate;
  }

  public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
    this.flushCacheOnDeallocate = flushCacheOnDeallocate;
  }
}
