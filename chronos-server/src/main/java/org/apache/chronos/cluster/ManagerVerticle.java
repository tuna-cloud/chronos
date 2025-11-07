package org.apache.chronos.cluster;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ManagerVerticle extends AbstractVerticle {
  private final Logger logger = LogManager.getLogger(ManagerVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    doInit();
    startPromise.complete();
  }

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
  }

  protected void doInit() {
    logger.info("ManagerVerticle doInit");
  }
}
