package org.apache.chronos.cluster.metastore;

import io.vertx.core.Future;
import java.util.Collection;
import java.util.List;
import org.apache.chronos.cluster.meta.IMetaData;

public interface IStorageEngine {
  Future<Void> init();

  IMetaData getById(int id);

  IMetaData getByCode(String code);

  List<IMetaData> listByTags(int pageNo, int offset, String... tags);

  int countByTags(String... tags);

  void save(IMetaData metaData);

  void save(Collection<IMetaData> metaData);

  void update(IMetaData metaData);

  long getVersion();
}
