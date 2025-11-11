package org.apache.chronos.cluster.meta;

import io.netty.util.Recycler;
import java.util.List;
import java.util.Map;

public class MultiplyColumn implements IMetaData {

  private int id;
  private long createdAt;
  private long updatedAt;

  private List<String> tags;
  private List<String> codes;
  private List<ValueType> types;
  private Map<String, String> attrs;
  private final Recycler.Handle<MultiplyColumn> handle;

  public MultiplyColumn(Recycler.Handle<MultiplyColumn> handle) {
    this.handle = handle;
  }

  public void recycle() {
    id = -1;
    tags = null;
    createdAt = -1;
    updatedAt = -1;
    codes = null;
    types = null;
    attrs = null;
    handle.recycle(this);
  }

  private static final Recycler<MultiplyColumn> RECYCLER = new Recycler<MultiplyColumn>() {
    @Override
    protected MultiplyColumn newObject(Handle<MultiplyColumn> handle) {
      return new MultiplyColumn(handle);
    }
  };

  public static MultiplyColumn create() {
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

  public Map<String, String> getAttrs() {
    return attrs;
  }

  public void setAttrs(Map<String, String> attrs) {
    this.attrs = attrs;
  }
}
