package org.apache.chronos.server;

import com.hazelcast.config.Config;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class Bootstrap {

  public static void main(String[] args) {
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
    });
  }
}