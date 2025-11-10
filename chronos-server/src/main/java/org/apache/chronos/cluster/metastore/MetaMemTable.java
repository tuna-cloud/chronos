package org.apache.chronos.cluster.metastore;

import org.apache.chronos.cluster.meta.IMetaData;

public class MetaMemTable implements IMetaMemTable {
  private IMetaBlockManager blockManager;

  public MetaMemTable(IMetaBlockManager blockManager) {
    this.blockManager = blockManager;
  }

  @Override
  public IMetaData getById(int id) {
    return null;
  }

  @Override
  public IMetaData getByCode(String code) {
    return null;
  }
}
