package org.postgresql.replication.fluent.physical;


import org.postgresql.core.BaseConnection;
import org.postgresql.replication.fluent.AbstractCreateSlotBuilder;

import java.sql.SQLException;
import java.sql.Statement;

public class PhysicalCreateSlotBuilder
    extends AbstractCreateSlotBuilder<ChainedPhysicalCreateSlotBuilder>
    implements ChainedPhysicalCreateSlotBuilder {
  private BaseConnection connection;

  public PhysicalCreateSlotBuilder(BaseConnection connection) {
    this.connection = connection;
  }

  @Override
  protected ChainedPhysicalCreateSlotBuilder self() {
    return this;
  }

  @Override
  public void make() throws SQLException {
    if (slotName == null || slotName.isEmpty()) {
      throw new IllegalArgumentException("Replication slotName can't be null");
    }

    Statement statement = connection.createStatement();
    try {
      statement.execute(String.format("CREATE_REPLICATION_SLOT %s PHYSICAL", slotName));
    } finally {
      statement.close();
    }
  }
}
