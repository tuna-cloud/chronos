package org.apache.chronos.cluster.meta;

import io.netty.util.Recycler;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Column implements IMetaData, Serializable {

  private int id;
  private long createdAt;
  private long updatedAt;
  private String code;
  private ValueType valueType;
  private List<String> tags;
  private Map<String, String> attrs;
  private final Recycler.Handle<Column> handle;

  public Column(Recycler.Handle<Column> handle) {
    this.handle = handle;
  }

  public void recycle() {
    id = -1;
    tags = null;
    createdAt = -1;
    updatedAt = -1;
    code = null;
    valueType = null;
    attrs = null;
    handle.recycle(this);
  }

  private static final Recycler<Column> RECYCLER = new Recycler<Column>() {
    @Override
    protected Column newObject(Handle<Column> handle) {
      return new Column(handle);
    }
  };

  public static Column create() {
    return RECYCLER.get();
  }

  @Override
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
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

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public void setValueType(ValueType valueType) {
    this.valueType = valueType;
  }

  public Map<String, String> getAttrs() {
    return attrs;
  }

  public void setAttrs(Map<String, String> attrs) {
    this.attrs = attrs;
  }
}
