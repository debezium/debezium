package org.postgresql.replication.fluent.logical;

import org.postgresql.replication.fluent.ChainedCommonCreateSlotBuilder;

/**
 * Logical replication slot specific parameters
 */
public interface ChainedLogicalCreateSlotBuilder
    extends ChainedCommonCreateSlotBuilder<ChainedLogicalCreateSlotBuilder> {

  /**
   * <p>Output plugin that should be use for decode physical represent WAL to some logical form.
   * Output plugin should be installed on server(exists in shared_preload_libraries).
   *
   * <p>Package postgresql-contrib provides sample output plugin <b>test_decoding</b> that can be
   * use for test logical replication api
   *
   * @param outputPlugin not null name of the output plugin used for logical decoding
   */
  ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin);
}
