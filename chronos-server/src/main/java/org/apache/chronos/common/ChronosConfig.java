package org.apache.chronos.common;

public enum ChronosConfig {
  CFG_FILE("CHRONOS_CONF", "chronos.conf", "/chronos.yml"),
  CFG_MANAGER_PORT("CHRONOS_MANAGER_PORT", "chronos.manager.port", "8911"),
  CFG_ENV_MANAGER_REUSE_PORT("CHRONOS_MANAGER_REUSE_PORT", "chronos.manager.reusePort", "true"),
  CFG_META_STORAGE_PATH("CHRONOS_META_STORAGE_PATH", "chronos.meta.storage.path", "./data/metaStore"),
  CFG_META_TAGS_INDEX_CAPACITY("CFG_META_TAGS_INDEX_CAPACITY", "cfg.meta.tags.index.capacity", "1000000")
  ;
  private String envKey;
  private String propKey;
  private String defaultValue;

  ChronosConfig(String envKey, String propKey, String defaultValue) {
    this.envKey = envKey;
    this.propKey = propKey;
    this.defaultValue = defaultValue;
  }

  public String getEnvKey() {
    return envKey;
  }

  public void setEnvKey(String envKey) {
    this.envKey = envKey;
  }

  public String getPropKey() {
    return propKey;
  }

  public void setPropKey(String propKey) {
    this.propKey = propKey;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }
}
