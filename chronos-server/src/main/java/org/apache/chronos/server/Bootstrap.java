package org.apache.chronos.server;

import com.hazelcast.config.Config;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import org.apache.chronos.cluster.ManagerVerticle;

public class Bootstrap {

  public static void main(String[] args) {
    checkLog4j2Availability();
    System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
    System.setProperty("hazelcast.local.publicAddress", "127.0.0.1");
    Config hazelcastConfig = new Config();
    hazelcastConfig.setClusterName("chronos");
    HazelcastClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);

    mgr.nodeListener(new NodeListener() {
      @Override
      public void nodeAdded(String nodeID) {
        System.out.println("nodeAdded: " + nodeID);
      }

      @Override
      public void nodeLeft(String nodeID) {
        System.out.println("nodeLeft: " + nodeID);
      }
    });
    Vertx.builder().withClusterManager(mgr).buildClustered().onComplete(ar -> {
      if (ar.succeeded()) {
        Vertx cluster = ar.result();
        cluster.deployVerticle(new ManagerVerticle(), new DeploymentOptions().setHa(true));
      }
    });
  }

  public static void checkLog4j2Availability() {
    System.out.println("=== Log4j2 诊断信息 ===");

    // 检查类路径
    String classpath = System.getProperty("java.class.path");
    System.out.println("类路径: " + classpath);

    // 检查关键类
    checkClass("org.apache.logging.log4j.core.Logger");
    checkClass("org.apache.logging.log4j.core.config.ConfigurationFactory");
    checkClass("org.apache.logging.log4j.LogManager");

    // 检查系统属性
    System.out.println("log4j2.configurationFile: " +
        System.getProperty("log4j2.configurationFile"));
    System.out.println("log4j.configurationFile: " +
        System.getProperty("log4j.configurationFile"));
  }

  private static void checkClass(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      System.out.println("✓ 找到类: " + className);

      // 检查类所在 jar
      ProtectionDomain protectionDomain = clazz.getProtectionDomain();
      CodeSource codeSource = protectionDomain.getCodeSource();
      if (codeSource != null) {
        System.out.println("  位置: " + codeSource.getLocation());
      }

    } catch (ClassNotFoundException e) {
      System.err.println("✗ 未找到类: " + className);
    }
  }
}