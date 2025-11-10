package org.apache.chronos.cluster.metastore;

import org.apache.chronos.cluster.meta.IMetaData;

public interface IMetaMemTable {

  IMetaData getById(int id);

  IMetaData getByCode(String code);
}
