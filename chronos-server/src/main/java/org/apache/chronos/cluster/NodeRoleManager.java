package org.apache.chronos.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryUpdatedListener;
import io.netty.util.internal.StringUtil;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import java.util.List;
import java.util.UUID;
import org.apache.chronos.common.Constant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeRoleManager implements NodeListener {

  private static final Logger log = LogManager.getLogger(NodeRoleManager.class);

  private final Context context;
  private final VertxImpl vertx;
  private List<String> clusterNodes;

  private ClusterManager clusterManager;
  private MetaLeader metaLeader;
  private MetaFollower metaFollower;
  private Lock leaderLock;
  private UUID leaderListenerId;

  public NodeRoleManager(VertxImpl vertx, Context context) {
    this.vertx = vertx;
    this.context = context;
  }

  public void start(Promise<Void> startPromise) {
    clusterManager = vertx.clusterManager();
    clusterManager.nodeListener(this);
    clusterNodes = clusterManager.getNodes();
    log.info("clusterNodes: {}", clusterNodes.toString());

    vertx.sharedData().<String, String>getAsyncMap(Constant.META_GLOBAL).compose(result -> result.get(Constant.META_GLOBAL_LEADER)).onComplete(result -> {
      if (result.succeeded()) {
        if (StringUtil.isNullOrEmpty(result.result())) {
          log.info("meta map: {} key: {} is null", Constant.META_GLOBAL, Constant.META_GLOBAL_LEADER);
          tryBecomeLeader(startPromise);
        } else {
          LeaderInfo leaderInfo = new JsonObject(result.result()).mapTo(LeaderInfo.class);
          log.info("meta map: {} key: {} value: {}", Constant.META_GLOBAL, Constant.META_GLOBAL_LEADER, result.result());
          if (clusterManager.getNodeId().equals(leaderInfo.getNodeId())) {
            log.info("leader has exist, leaderInfo: {}, tobe leader", result.result());
            metaLeader = new MetaLeader(vertx, context);
            metaLeader.start(startPromise);
          } else {
            log.info("leader has exist, leaderInfo: {}, tobe follower", result.result());
            metaFollower = new MetaFollower(vertx, context);
            metaFollower.start(startPromise);
          }
        }
      } else {
        tryBecomeLeader(startPromise);
      }
    });

    if (clusterManager instanceof HazelcastClusterManager) {
      HazelcastInstance instance = ((HazelcastClusterManager) clusterManager).getHazelcastInstance();
      leaderListenerId = instance.getMap(Constant.META_GLOBAL).addEntryListener((EntryUpdatedListener<String, String>) event -> {
        log.info("Leader node change, new leaderNodeId: {}", event.getValue());
        LeaderInfo leaderInfo = new JsonObject(event.getValue()).mapTo(LeaderInfo.class);
        if (!clusterManager.getNodeId().equals(leaderInfo.getNodeId())) {
          log.info("leader change, leaderInfo: {}, tobe follower", event.getValue());
          metaFollower = new MetaFollower(vertx, context);
          metaFollower.start(Promise.promise());
        }
      }, Constant.META_GLOBAL_LEADER, true);
    } else {
      // TODO
      startPromise.fail("Now only support HazelcastClusterManager");
    }
  }

  public void stop(Promise<Void> stopPromise) throws Exception {
    if (leaderLock != null) {
      leaderLock.release();
    }
    if (metaLeader != null) {
      metaLeader.stop(stopPromise);
    }
    if (metaFollower != null) {
      metaFollower.stop(stopPromise);
    }
    if (leaderListenerId != null) {
      HazelcastInstance instance = ((HazelcastClusterManager) clusterManager).getHazelcastInstance();
      instance.getMap(Constant.META_GLOBAL).removeEntryListener(leaderListenerId);
    }
  }

  @Override
  public void nodeAdded(String nodeID) {
    log.info("NodeRoleManager nodeAdded: {}", nodeID);
    clusterNodes.add(nodeID);
  }

  @Override
  public void nodeLeft(String nodeID) {
    log.info("NodeRoleManager nodeLeft: {}", nodeID);
    clusterNodes.removeIf(nodeID::equals);
    log.info("clusterNodes: {}", clusterNodes.toString());
    /**
     * Cluster Leader is down, try to become leader
     * First, if this node is a follower, stop it first.
     */
    if (nodeID.equals(leaderNodeId)) {
      log.info("Cluster leader is down, try become leader");
      if (metaFollower != null) {
        Promise<Void> promise = Promise.promise();
        try {
          metaFollower.stop(promise);
        } catch (Exception e) {
          log.error("metaFollower stop err", e);
          promise.complete();
        }
        metaFollower = null;
        promise.future().andThen(r -> tryBecomeLeader(Promise.promise()));
      } else {
        tryBecomeLeader(Promise.promise());
      }
    }
  }

  protected void tryBecomeLeader(Promise<Void> startPromise) {
    vertx.sharedData().getLock(Constant.LOCK_LEADER).onComplete(result -> {
      if (result.succeeded()) {
        leaderLock = result.result();
        log.info("tryBecomeLeader success, leaderNodeId: {}", clusterManager.getNodeId());

        vertx.sharedData().<String, String>getAsyncMap(Constant.META_GLOBAL).andThen(mapResult -> {
          NodeInfo nodeInfo = clusterManager.getNodeInfo();
          mapResult.result().put(Constant.META_GLOBAL_LEADER,
              JsonObject.mapFrom(new LeaderInfo(clusterManager.getNodeId(), nodeInfo.host(), context.config().getInteger(Constant.CFG_MANAGER_PORT, Constant.CFG_MANAGER_PORT_DEFAULT))).toString());
        }).onSuccess(r -> {
          metaLeader = new MetaLeader(vertx, context);
          metaLeader.start(startPromise);
        }).onFailure(f -> {
          log.error("Meta global map leader update failed", f.getCause());
        });
      }
    });
  }
}
