package org.postgresql.replication.fluent.logical;


import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.AbstractStreamBuilder;

import java.sql.SQLException;
import java.util.Properties;

public class LogicalStreamBuilder extends AbstractStreamBuilder<ChainedLogicalStreamBuilder>
    implements ChainedLogicalStreamBuilder, LogicalReplicationOptions {
  private final Properties slotOptions;

  private StartLogicalReplicationCallback startCallback;

  /**
   * @param startCallback not null callback that should be execute after build parameters for start
   *                      replication
   */
  public LogicalStreamBuilder(StartLogicalReplicationCallback startCallback) {
    this.startCallback = startCallback;
    this.slotOptions = new Properties();
  }

  @Override
  protected ChainedLogicalStreamBuilder self() {
    return this;
  }

  @Override
  public PGReplicationStream start() throws SQLException {
    return startCallback.start(this);
  }

  @Override
  public String getSlotName() {
    return slotName;
  }


  @Override
  public ChainedLogicalStreamBuilder withStartPosition(LogSequenceNumber lsn) {
    startPosition = lsn;
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, boolean optionValue) {
    slotOptions.setProperty(optionName, String.valueOf(optionValue));
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, int optionValue) {
    slotOptions.setProperty(optionName, String.valueOf(optionValue));
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, String optionValue) {
    slotOptions.setProperty(optionName, optionValue);
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOptions(Properties options) {
    for (String propertyName : options.stringPropertyNames()) {
      slotOptions.setProperty(propertyName, options.getProperty(propertyName));
    }
    return this;
  }

  @Override
  public LogSequenceNumber getStartLSNPosition() {
    return startPosition;
  }

  @Override
  public Properties getSlotOptions() {
    return slotOptions;
  }

  @Override
  public int getStatusInterval() {
    return statusIntervalMs;
  }
}
