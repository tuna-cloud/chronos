package org.apache.chronos.cluster;

import com.apache.chronos.protocol.codec.MessageDecoder;
import com.apache.chronos.protocol.codec.MessageEncoder;
import com.apache.chronos.protocol.message.AbstractMessage;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.net.NetSocketInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import java.util.Map;
import org.apache.chronos.common.ChronosConfig;
import org.apache.chronos.common.CfgUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetaLeader {

  private static final Logger log = LogManager.getLogger(MetaLeader.class);

  private final Vertx vertx;
  private final Context context;
  private NetServer netServer;
  private final Map<String, NetSocketInternal> followerSession = Maps.newConcurrentMap();
  private final Map<String, String> nodeId2ChannelId = Maps.newConcurrentMap();

  public MetaLeader(Vertx vertx, Context context) {
    this.vertx = vertx;
    this.context = context;
  }

  public void start(Promise<Void> startPromise) {
    JsonObject config = context.config();
    NetServerOptions options = new NetServerOptions();
    options.setPort(CfgUtil.getInteger(ChronosConfig.CFG_MANAGER_PORT, config));
    options.setReusePort(CfgUtil.getBoolean(ChronosConfig.CFG_ENV_MANAGER_REUSE_PORT, config));
    options.setReuseAddress(true);
    options.setTcpFastOpen(true);
    options.setTcpKeepAlive(true);
    options.setTcpNoDelay(true);
    options.setTcpQuickAck(true);

    netServer = vertx.createNetServer(options);

    netServer.connectHandler(netSocket -> {
      log.info("Follower connected, host: {}", netSocket.remoteAddress().host());
      NetSocketInternal soi = (NetSocketInternal) netSocket;
      ChannelPipeline pipeline = soi.channelHandlerContext().pipeline();

      initChannel(pipeline);

      followerSession.put(soi.channelHandlerContext().channel().id().asShortText(), soi);

      netSocket.closeHandler(v -> {
        followerSession.remove(soi.channelHandlerContext().channel().id().asShortText());
        clearClosedSession(soi.channelHandlerContext().channel().id().asShortText());
      });

      soi.messageHandler(msg -> {
        messageHandle(soi, (AbstractMessage) msg);
      });
    });

    netServer.listen().onSuccess(netServer -> {
      log.info("Chronos Manager Server bind at port: {}", netServer.actualPort());
      startPromise.complete();
    }).onFailure(r -> {
      log.error("Chronos Manager Server bind at port failed", r.getCause());
      startPromise.fail(r);
    });
  }

  public void stop(Promise<Void> stopPromise) throws Exception {
    netServer.close().onComplete(r -> {
      if (r.succeeded()) {
        stopPromise.complete();
      } else {
        stopPromise.fail(r.cause());
      }
    });
  }

  private void initChannel(ChannelPipeline pipeline) {

    pipeline.addBefore("handler", "loggerByteBuf", new LoggingHandler(LogLevel.INFO));
    pipeline.addBefore("handler", "messageEncoder", new MessageEncoder());
    pipeline.addBefore("handler", "messageDecoder", new MessageDecoder());
    pipeline.addBefore("handler", "loggerMessage", new LoggingHandler(LogLevel.INFO));
    //  adding the idle state handler for timeout on CONNECT packet
    pipeline.addBefore("handler", "idle", new IdleStateHandler(30, 0, 0));
    pipeline.addBefore("handler", "timeoutOnConnect", new ChannelDuplexHandler() {

      @Override
      public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

        if (evt instanceof IdleStateEvent) {
          IdleStateEvent e = (IdleStateEvent) evt;
          if (e.state() == IdleState.READER_IDLE) {
            log.info("Session readTimeout, host: {}", ctx.channel().remoteAddress().toString());
            ctx.channel().close();
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
    nodeId2ChannelId.put(message.getNodeId(), soi.channelHandlerContext().channel().id().asShortText());
  }

  private void clearClosedSession(String channelId) {
    nodeId2ChannelId.entrySet().removeIf(entry -> entry.getValue().equals(channelId));
  }

  public void broadcast(AbstractMessage message) {
    for (NetSocketInternal soi : followerSession.values()) {
      soi.writeMessage(message);
    }
  }
}
