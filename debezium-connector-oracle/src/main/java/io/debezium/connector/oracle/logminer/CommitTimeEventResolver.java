/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;

import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringBeginEvent;
import io.debezium.connector.oracle.logminer.events.ExtendedStringWriteEvent;
import io.debezium.connector.oracle.logminer.events.LobEraseEvent;
import io.debezium.connector.oracle.logminer.events.LobWriteEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.LogMinerRedoEvent;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.SelectLobLocatorEvent;
import io.debezium.connector.oracle.logminer.events.XmlBeginEvent;
import io.debezium.connector.oracle.logminer.events.XmlEndEvent;
import io.debezium.connector.oracle.logminer.events.XmlWriteEvent;
import io.debezium.connector.oracle.logminer.parser.ExtendedStringParser;
import io.debezium.connector.oracle.logminer.parser.ExtendedStringWriteParser;
import io.debezium.connector.oracle.logminer.parser.LobWriteParser;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.SelectLobParser;
import io.debezium.connector.oracle.logminer.parser.XmlBeginParser;
import io.debezium.connector.oracle.logminer.parser.XmlWriteParser;
import io.debezium.relational.Table;

/**
 * Turns a stored {@link LogMinerRedoEvent} into the parsed {@link LogMinerEvent} that
 * {@link TransactionCommitConsumer} expects, when the event's transaction commits.
 * <p>
 * Each stored event is parsed through the same parsers, with the same table resolution, warnings and
 * failure handling, that the streaming source applies to a row while mining: the stored event is turned
 * back into its row view and handed to that code. Where a mined row would have been dropped, {@code null}
 * is returned and nothing is emitted.
 * <p>
 * One instance serves one commit, on the thread that commits it, since the single-column parsers keep
 * per-call state.
 *
 * @author Chris Cranford
 */
public class CommitTimeEventResolver {

    private final AbstractLogMinerStreamingChangeEventSource source;

    public CommitTimeEventResolver(AbstractLogMinerStreamingChangeEventSource source) {
        this.source = source;
    }

    /**
     * @param event the stored event, should not be {@code null}
     * @param transactionId the committing transaction
     * @return the event to dispatch, or {@code null} when the stored event emits nothing
     * @throws SQLException if fetching the definition of a newly captured table fails
     * @throws InterruptedException if the thread is interrupted
     */
    public LogMinerEvent resolve(LogMinerRedoEvent event, String transactionId) throws SQLException, InterruptedException {
        final LogMinerEventRow row = LogMinerEventRow.fromRedoEvent(event, transactionId);
        return switch (event.getEventType()) {
            case INSERT, UPDATE, DELETE -> resolveDataChange(row);
            case SELECT_LOB_LOCATOR -> resolveSelectLobLocator(row);
            case LOB_WRITE -> resolveLobWrite(row);
            case LOB_ERASE -> isTableKnown(row) ? new LobEraseEvent(row) : null;
            case XML_BEGIN -> resolveXmlBegin(row);
            case XML_WRITE -> resolveXmlWrite(row);
            case XML_END -> isTableKnown(row) ? new XmlEndEvent(row) : null;
            case EXTENDED_STRING_BEGIN -> resolveExtendedStringBegin(row);
            case EXTENDED_STRING_WRITE -> isTableKnown(row) ? new ExtendedStringWriteEvent(row, ExtendedStringWriteParser.parse(row.getRedoSql())) : null;
            default -> throw new IllegalStateException("Unexpected stored event type " + event.getEventType());
        };
    }

    private LogMinerEvent resolveDataChange(LogMinerEventRow row) throws SQLException, InterruptedException {
        final Table table = source.resolveTableAtCommit(row.getTableId());
        if (table == null) {
            return null;
        }

        final LogMinerDmlEntry parsedEvent = source.parseDmlStatement(row, table);
        if (parsedEvent == null) {
            return null;
        }
        parsedEvent.setObjectName(row.getTableName());
        parsedEvent.setObjectOwner(row.getTablespaceName());

        return source.getConfig().isLogMiningIncludeRedoSql()
                ? new RedoSqlDmlEvent(row, parsedEvent, row.getRedoSql())
                : new DmlEvent(row, parsedEvent);
    }

    private LogMinerEvent resolveSelectLobLocator(LogMinerEventRow row) {
        final Table table = tableFor(row);
        if (table == null) {
            return null;
        }

        final SelectLobParser parser = source.getSelectLobParser();
        final LogMinerDmlEntry parsedEvent = parser.parse(row.getRedoSql(), table);
        parsedEvent.setObjectName(row.getTableName());
        parsedEvent.setObjectOwner(row.getTablespaceName());

        return new SelectLobLocatorEvent(row, parsedEvent, parser.getColumnName(), parser.isBinary());
    }

    private LogMinerEvent resolveLobWrite(LogMinerEventRow row) {
        if (!isTableKnown(row)) {
            return null;
        }
        final LobWriteParser.LobWrite parsedEvent = LobWriteParser.parse(row.getRedoSql());
        return parsedEvent == null ? null : new LobWriteEvent(row, parsedEvent);
    }

    private LogMinerEvent resolveXmlBegin(LogMinerEventRow row) {
        final Table table = tableFor(row);
        if (table == null) {
            return null;
        }

        final XmlBeginParser.XmlBegin result = source.getXmlBeginParser().parse(row, table);
        result.parsedEvent().setObjectName(row.getTableName());
        result.parsedEvent().setObjectOwner(row.getTablespaceName());

        return new XmlBeginEvent(row, result.parsedEvent(), result.columnName());
    }

    private LogMinerEvent resolveXmlWrite(LogMinerEventRow row) {
        if (!isTableKnown(row)) {
            return null;
        }
        final XmlWriteParser.XmlWrite parsedEvent = XmlWriteParser.parse(row, source.getDatabaseCharacterSet());
        return new XmlWriteEvent(row, parsedEvent.data(), parsedEvent.length());
    }

    private LogMinerEvent resolveExtendedStringBegin(LogMinerEventRow row) {
        final Table table = tableFor(row);
        if (table == null) {
            return null;
        }

        final ExtendedStringParser parser = source.getExtendedStringParser();
        final LogMinerDmlEntry parsedEvent = parser.parse(row.getRedoSql(), table);
        parsedEvent.setObjectName(row.getTableName());
        parsedEvent.setObjectOwner(row.getTablespaceName());

        return new ExtendedStringBeginEvent(row, parsedEvent, parser.getColumnName());
    }

    private Table tableFor(LogMinerEventRow row) {
        return row.getTableId() == null ? null : source.getSchema().tableFor(row.getTableId());
    }

    private boolean isTableKnown(LogMinerEventRow row) {
        return tableFor(row) != null;
    }
}
