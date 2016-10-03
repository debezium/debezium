package org.postgresql.replication.fluent.logical;


import org.postgresql.replication.fluent.CommonOptions;

import java.util.Properties;

public interface LogicalReplicationOptions extends CommonOptions {
  /**
   * Required parameter for logical replication
   *
   * @return not null logical replication slot name that already exists on server and free.
   */
  String getSlotName();

  /**
   * Parameters for output plugin. Parameters will be set to output plugin that register for
   * specified replication slot name.
   *
   * @return list options that will be pass to output_plugin for that was create replication slot
   */
  Properties getSlotOptions();
}
