package org.apache.chronos.cluster.meta;

import java.util.List;

public interface IMetaData {

  int getId();

  long getCreatedAt();

  long getUpdatedAt();

  List<String> getTags();
}
