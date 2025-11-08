package org.apache.chronos.cluster;

import com.apache.chronos.protocol.codec.MessageDecoder;
import com.apache.chronos.protocol.codec.MessageEncoder;
import com.apache.chronos.protocol.message.AbstractMessage;
import com.apache.chronos.protocol.message.Ping;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.internal.net.NetSocketInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.chronos.common.Constant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetaFollower {

  private static final Logger log = LogManager.getLogger(MetaFollower.class);
  private final Vertx vertx;
  private final Context context;
  private NetClient netClientToLeader;
  private String selfNodeId;


  public MetaFollower(Vertx vertx, Context context) {
    this.vertx = vertx;
    this.context = context;
  }

  public void start(Promise<Void> startPromise) {
    vertx.sharedData().<String, String>getAsyncMap(Constant.META_GLOBAL).compose(result -> result.get(Constant.META_GLOBAL_LEADER)).onComplete(result -> {
      if (result.succeeded()) {
        String leaderNodeId = result.result();

        ClusterManager clusterManager = ((VertxImpl) vertx).clusterManager();
        selfNodeId = clusterManager.getNodeId();

        clusterManager.getNodeInfo(leaderNodeId, (result1, failure) -> {
          if (failure == null) {
            String leaderHost = result1.host();
            connectLeader(leaderNodeId, leaderHost).onComplete(startPromise);
          } else {
            log.error("Get Leader NodeInfo err", failure.getCause());
            startPromise.fail(failure);
          }
        });
      } else {
        log.error("Get LeaderId err", result.cause());
        startPromise.fail(result.cause());
      }
    });
  }

  public void stop(Promise<Void> stopPromise) {
    netClientToLeader.close().onComplete(r -> {
      if (r.succeeded()) {
        stopPromise.complete();
      } else {
        stopPromise.fail(r.cause());
      }
    });
  }

  private Future<Void> connectLeader(String leaderNodeId, String host) {
    Promise<Void> promise = Promise.promise();

    JsonObject config = context.config();
    int leaderPort = config.getInteger(Constant.CFG_MANAGER_PORT, Constant.CFG_MANAGER_PORT_DEFAULT);

    NetClientOptions options = new NetClientOptions();
    options.setReconnectAttempts(Integer.MAX_VALUE);
    netClientToLeader = vertx.createNetClient(options);

    doConnect(leaderNodeId, host, leaderPort, promise);
    return promise.future();
  }

  private void doConnect(String leaderNodeId, String host, int port, Promise<Void> promise) {
    netClientToLeader.connect(port, host).onFailure(f -> promise.fail(f.getCause())).onSuccess(socket -> {
      log.info("Connect to Leader success, host: {}, leaderNodeId: {}, selfNodeId: {}", host, leaderNodeId, selfNodeId);
      NetSocketInternal soi = (NetSocketInternal) socket;
      ChannelPipeline pipeline = soi.channelHandlerContext().pipeline();

      initChannel(pipeline);

      soi.messageHandler(msg -> {
        messageHandle(soi, (AbstractMessage) msg);
      });

      soi.exceptionHandler(ex -> {
        log.error("Leader connection exception", ex);
      });

      soi.closeHandler(v -> {
        log.info("Leader Connection closed.");
      });

      promise.complete();
    });
  }

  private void initChannel(ChannelPipeline pipeline) {

    pipeline.addBefore("handler", "loggerByteBuf", new LoggingHandler(LogLevel.INFO));
    pipeline.addBefore("handler", "messageEncoder", new MessageEncoder());
    pipeline.addBefore("handler", "messageDecoder", new MessageDecoder());
    pipeline.addBefore("handler", "loggerMessage", new LoggingHandler(LogLevel.INFO));
    //  adding the idle state handler for timeout on CONNECT packet
    pipeline.addBefore("handler", "idle", new IdleStateHandler(10, 0, 0));
    pipeline.addBefore("handler", "timeoutOnConnect", new ChannelDuplexHandler() {

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt instanceof IdleStateEvent) {
          IdleStateEvent e = (IdleStateEvent) evt;
          if (e.state() == IdleState.READER_IDLE) {
            Ping ping = Ping.create();
            ping.setNodeId(selfNodeId);
//            ctx.channel().writeAndFlush(ping);
          } else {
            ctx.fireUserEventTriggered(evt);
          }
        } else {
          ctx.fireUserEventTriggered(evt);
        }
      }
    });
  }

  private void messageHandle(NetSocketInternal soi, AbstractMessage message) {
    log.info("messageHandle: {}", message.getClass().getName());
  }
}
