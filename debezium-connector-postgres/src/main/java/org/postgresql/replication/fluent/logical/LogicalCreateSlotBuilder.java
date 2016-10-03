package org.postgresql.replication.fluent.logical;

import org.postgresql.core.BaseConnection;
import org.postgresql.replication.fluent.AbstractCreateSlotBuilder;

import java.sql.SQLException;
import java.sql.Statement;

public class LogicalCreateSlotBuilder
    extends AbstractCreateSlotBuilder<ChainedLogicalCreateSlotBuilder>
    implements ChainedLogicalCreateSlotBuilder {
  private String outputPlugin;
  private BaseConnection connection;

  public LogicalCreateSlotBuilder(BaseConnection connection) {
    this.connection = connection;
  }

  @Override
  protected ChainedLogicalCreateSlotBuilder self() {
    return this;
  }

  @Override
  public ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin) {
    this.outputPlugin = outputPlugin;
    return self();
  }

  @Override
  public void make() throws SQLException {
    if (outputPlugin == null || outputPlugin.isEmpty()) {
      throw new IllegalArgumentException(
          "OutputPlugin required parameter for logical replication slot");
    }

    if (slotName == null || slotName.isEmpty()) {
      throw new IllegalArgumentException("Replication slotName can't be null");
    }

    Statement statement = connection.createStatement();
    try {
      statement.execute(String.format("CREATE_REPLICATION_SLOT %s LOGICAL %s", slotName, outputPlugin));
    } finally {
      statement.close();
    }
  }
}
