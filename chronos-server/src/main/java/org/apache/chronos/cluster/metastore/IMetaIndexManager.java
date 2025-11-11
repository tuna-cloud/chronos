package org.apache.chronos.cluster.metastore;

import org.apache.chronos.cluster.meta.IMetaData;
import org.apache.chronos.cluster.meta.Offset;

public interface IMetaIndexManager {

  Offset getById(int id);

  Offset getByCode(String code);

  void updateIndex(IMetaData metaData, Offset offset);
}
