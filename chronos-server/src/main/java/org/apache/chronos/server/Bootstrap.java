package org.apache.chronos.server;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.transport.Transport;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.apache.chronos.cluster.ManagerVerticle;
import org.apache.chronos.common.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Bootstrap {

  private static final Logger log = LogManager.getLogger(Bootstrap.class);

  public static void main(String[] args) throws Exception {
    JsonObject config = ConfigUtil.readConfig();
    Vertx.builder()
        .with(new VertxOptions().setHAEnabled(true))
        .withClusterManager(new HazelcastClusterManager())
        .withTransport(Transport.nativeTransport())
        .buildClustered().onComplete(result -> {
          if (result.succeeded()) {
            Vertx vertx = result.result();
            vertx.deployVerticle(new ManagerVerticle(), new DeploymentOptions().setHa(true).setConfig(config));
          } else {
            log.error("Deployment failed", result.cause());
          }
        });
  }
}