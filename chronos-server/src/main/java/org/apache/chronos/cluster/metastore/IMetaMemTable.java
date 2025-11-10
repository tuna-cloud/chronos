package org.apache.chronos.cluster.metastore;

import io.vertx.core.Future;
import org.apache.chronos.cluster.meta.IMetaData;

public interface IMetaMemTable {

  Future<Void> init();

  IMetaData getById(int id);

  IMetaData getByCode(String code);
}
