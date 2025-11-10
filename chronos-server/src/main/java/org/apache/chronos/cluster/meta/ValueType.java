package org.apache.chronos.cluster.meta;

public enum ValueType {
  BYTE(1, "byte"),
  CHAR(2, "char"),
  SHORT(3, "short"),
  INTEGER(4, "integer"),
  LONG(5, "long"),
  STRING(6, "string"),
  BYTES(7, "bytes"),
  ;

  ValueType(int value, String desc) {
    this.value = value;
    this.desc = desc;
  }

  private int value;
  private String desc;

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  public static ValueType fromValue(int value) {
    for (ValueType valueType : ValueType.values()) {
      if (valueType.getValue() == value) {
        return valueType;
      }
    }
    return null;
  }
}

