package org.apache.chronos.cluster;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ManagerVerticle extends AbstractVerticle {

  private static final Logger log = LogManager.getLogger(ManagerVerticle.class);
  private NodeRoleManager nodeRoleManager;

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    nodeRoleManager = new NodeRoleManager((VertxImpl) vertx, context);
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    nodeRoleManager.start(startPromise);
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    nodeRoleManager.stop(stopPromise);
  }
}
