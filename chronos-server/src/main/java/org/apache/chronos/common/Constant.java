package org.apache.chronos.common;

public interface Constant {

  String LOCK_LEADER = "leader";

  String META_GLOBAL = "metaGlobal";

  String META_GLOBAL_LEADER = "curLeader";

  Integer CFG_MANAGER_PORT_DEFAULT = 8911;
  String CFG_MANAGER_PORT = "chronos.manager.port";
  boolean CFG_MANAGER_REUSE_PORT_DEFAULT = true;
  String CFG_MANAGER_REUSE_PORT = "chronos.manager.reusePort";
}
