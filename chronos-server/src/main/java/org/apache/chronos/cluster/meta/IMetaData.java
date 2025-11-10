package org.apache.chronos.cluster.meta;

import java.util.Map;

public interface IMetaData {

  int getId();

  long getCreatedAt();

  long getUpdatedAt();

  Map<String, String> getTags();
}
