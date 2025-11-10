package org.apache.chronos.cluster.meta;

import java.util.List;
import java.util.Map;

public class MultiplyColumn implements IMetaData {

  private int id;
  private Map<String, String> tags;

  private long createdAt;
  private long updatedAt;

  private List<String> codes;
  private List<ValueType> types;

  @Override
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public List<String> getCodes() {
    return codes;
  }

  public void setCodes(List<String> codes) {
    this.codes = codes;
  }

  public List<ValueType> getTypes() {
    return types;
  }

  public void setTypes(List<ValueType> types) {
    this.types = types;
  }
}
