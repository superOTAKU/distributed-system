package org.otaku.jobmanager;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * <pre>
 * 1. master负责分配任务给slave节点分配任务。
 *
 * 2. 需要竞争成为master，但是可以起多个候选节点竞争成为master，作用是避免master单点异常。
 *
 * 3. zk结构的设计：
 *
 *  /master 临时节点，存在该节点表示master当前在线，节点的内容是serverId，每个master节点启动时，要配置自己的serverId，serverId必须全局唯一
 *  /slave/slave-1 临时顺序节点，该节点表示每个在线的worker，负责具体的工作负载
 *  /work/work-1 临时顺序节点，该节点表示客户端指定的工作内容
 *  /assign/slave-1/work-1 工作的分配情况，如分配work-1给slave-1
 *  /status/work-1 工作状态，client主要关心这个
 *
 * </pre>
 */
@Slf4j
public class Master implements Watcher {
    private final String connectStr;
    private final int sessionTimeout;
    private final String serverId = UUID.randomUUID().toString();
    private ZooKeeper zk;
    private State state = State.RUNNING;
    //会被用来控制主线程的状态，需要volatile
    private volatile boolean connected;
    private volatile boolean expired;
    //slaveCache在zk的event thread被访问，不需要volatile
    private ChildrenCache slavesCache;

    public Master(String connectStr, int sessionTimeout) {
        this.connectStr = connectStr;
        this.sessionTimeout = sessionTimeout;
    }

    public void connectZk() throws IOException {
        zk = new ZooKeeper(connectStr, sessionTimeout, this);
    }

    //当前节点状态
    private enum State {
        RUNNING, ELECTED, NOT_ELECTED
    }

    /**
     * 竞选master，就是竞争创建/master节点
     */
    private void selectMaster() {
        zk.create("/master", serverId.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, createMasterCallback, null);
    }

    private void masterExists() {
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    /**
     * 接管master的工作
     */
    public void takeLeaderShip() {
        getSlaves();
        //TODO getWorks，分配work给slave
    }

    /**
     * 查找所有的slave节点
     */
    private void getSlaves() {
        zk.getChildren("/slave", slaveChangeWatcher, slaveChildrenCallback, null);
    }

    /**
     * 分配任务，要应对slave掉线的情况，这时候要重新分配旧任务
     */
    private void reassignWorksToSlaves(List<String> slaves) {
        //掉线的slave
        List<String> absentSlaves = null;
        if (slavesCache == null) {
            slavesCache = new ChildrenCache(slaves);
        } else {
            log.info("slaves change, get new come slaves.");
            absentSlaves = slavesCache.removeAndSet(slaves);
        }
        if (absentSlaves != null) {
            for (String slave : absentSlaves) {
                getAbsentSlaveWorks(slave);
            }
        }
    }

    /**
     * 获取所有待分配的任务（属于原本掉线的slave节点）
     */
    private void getAbsentSlaveWorks(String slave) {
        zk.getChildren("/assign/" + slave, false, getAbsentSlaveAssignCallback, null);
    }

    /**
     * 获取任务详情并重新分配
     */
    private void doReassignAWork(String path, String work) {
        zk.getData(path, false, doReassignAWorkCallback, work);
    }

    /**
     * 重新创建任务挂载到work下
     */
    private void recreateWork(RecreateWorkCtx ctx) {
        zk.create("/work/" + ctx.getWork(), ctx.getData(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                recreateWorkCallback, ctx);
    }

    /**
     * 删除旧的任务分配
     */
    private void deleteAssign(String assignPath) {
        zk.delete(assignPath,  -1, assignDeleteCallback, null);
    }

    /**
     * 启动阶段创建所有需要的根节点
     */
    private void bootstrapAllRootNodes() {
        //疑问：有没有可能，bootstrap没有成功，就走了selectMaster?
        bootstrap("/slave");
        bootstrap("/work");
        bootstrap("/assign");
        bootstrap("/status");
        log.info("request bootstrap all root nodes");
    }

    private void bootstrap(String path) {
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, bootstrapCallback, null);
    }


    public static void main(String[] args) throws Exception {
        Master master = new Master("localhost:2181", 3000);
        master.connectZk();
        master.bootstrapAllRootNodes();
        Thread.sleep(500000);
    }

    //-----------------------------------watcher以及所有的callback---------------------------------

    @Override
    public void process(WatchedEvent event) {
        KeeperState state = event.getState();
        if (state == KeeperState.SyncConnected) {
            connected = true;
            log.info("connected to zk!");
            //连上zk就创建/master节点，竞争成为master节点
            selectMaster();
            return;
        }
        if (state == KeeperState.Disconnected) {
            connected = false;
            //断线的情况下，zk会自动重连，这里只是打印出信息
            log.info("disconnect from zk!");
            return;
        }
        if (state == KeeperState.Expired) {
            expired = true;
            log.info("connection expired!");
        }
    }

    private final AsyncCallback.DataCallback checkMasterCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            Code code = Code.get(rc);
            if (code == Code.CONNECTIONLOSS) {
                //网络波动，继续检查
                checkMaster();
                return;
            }
            if (code == Code.NONODE) {
                selectMaster();
                return;
            }
            if (code == Code.OK) {
                state = serverId.equals(new String(data, StandardCharsets.UTF_8)) ? State.ELECTED : State.NOT_ELECTED;
                log.info("I am " + (state == State.ELECTED ? "" : "not ") + "the leader");
                return;
            }
            log.error("error when reading data",
                    KeeperException.create(code, path));
        }
    };

    /**
     * 检查master是否存在，不存在继续竞选
     */
    private void checkMaster() {
        zk.getData("/master", false, checkMasterCallback, null);
    }

    private final AsyncCallback.StringCallback createMasterCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Code code = Code.get(rc);
            log.info("in createMasterCallback, code: {}", code);
            if (code == Code.CONNECTIONLOSS) {
                //断线了，不知道什么情况，检查看看master有没有
                checkMaster();
                return;
            }
            if (code == Code.OK) {
                //成功了，说明已经成为leader
                state = State.ELECTED;
                log.info("I am leader");
                takeLeaderShip();
                return;
            }

            if (code == Code.NODEEXISTS) {
                //节点已经存在了，通过exists设置watcher，/master状态有变化则再次竞选
                masterExists();
                return;
            }

            //不应该出现，如果出现仅记录问题
            state = State.NOT_ELECTED;
            log.error("something went wrong when running for master",
                    KeeperException.create(code, path));
        }
    };

    //节点被删除就重新竞选
    private final Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            EventType type = event.getType();
            if (type == EventType.NodeDeleted) {
                assert "/master".equals(event.getPath());
                log.info("seems last master disconnected, start election.");
                selectMaster();
            }
        }
    };

    private final AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Code code = Code.get(rc);
            log.info("in masterExistsCallback, code: {}", code);
            if (code == Code.CONNECTIONLOSS) {
                masterExists();
                return;
            }
            if (code == Code.OK) {
                state = State.NOT_ELECTED;
                log.info("seems master already exists, wait for next election.");
                return;
            }
            if (code == Code.NONODE) {
                state = State.RUNNING;
                selectMaster();
                log.info("It sounds like the previous master is gone, "
                        + "so let's run for master again!");
                return;
            }
            //不知道啥情况，检查master存在不？
            checkMaster();
        }
    };

    private final AsyncCallback.StringCallback bootstrapCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Code code = Code.get(rc);
            if (code == Code.OK) {
                log.info("created bootstrap node {}", path);
                return;
            }
            if (code == Code.CONNECTIONLOSS) {
                bootstrap(path);
                return;
            }
            if (code == Code.NODEEXISTS) {
                log.info("bootstrap node {} exists", path);
                return;
            }
            log.error("error in create bootstrap node",
                    KeeperException.create(code, path));
        }
    };

    private final Watcher slaveChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == EventType.NodeChildrenChanged) {
                assert event.getPath().equals("/slave");
                getSlaves();
            }
        }
    };

    private final AsyncCallback.ChildrenCallback slaveChildrenCallback = new AsyncCallback.ChildrenCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            Code code = Code.get(rc);
            if (code == Code.CONNECTIONLOSS) {
                getSlaves();
                return;
            }
            if (code == Code.OK) {
                reassignWorksToSlaves(children);
                return;
            }
            log.error("get slave children error",
                    KeeperException.create(code, path));
        }

    };

    private final AsyncCallback.ChildrenCallback getAbsentSlaveAssignCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            Code code = Code.get(rc);
            if (code == Code.CONNECTIONLOSS) {
                getAbsentSlaveWorks(path);
                return;
            }
            if (code == Code.OK) {
                log.info("get absent slave assignments: {} works", children.size());
                for (String work: children) {
                    doReassignAWork(path + "/" + work, work);
                }
            }
        }
    };

    private final AsyncCallback.DataCallback doReassignAWorkCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            Code code = Code.get(rc);
            if (code == Code.CONNECTIONLOSS) {
                doReassignAWork(path, (String)ctx);
                return;
            }
            if (code == Code.OK) {
                //要做的是重新创建work，删除assign
                recreateWork(new RecreateWorkCtx(path, (String)ctx, data));
                return;
            }
            log.error("get assign data error",
                    KeeperException.create(code, path));
        }
    };

    @Data
    @AllArgsConstructor
    private static class RecreateWorkCtx {
        private String path;
        private String work;
        private byte[] data;
    }

    private final AsyncCallback.StringCallback recreateWorkCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Code code = Code.get(rc);
            RecreateWorkCtx recreateWorkCtx = (RecreateWorkCtx)ctx;
            if (code == Code.CONNECTIONLOSS) {
                recreateWork(recreateWorkCtx);
                return;
            }
            if (code == Code.OK) {
                deleteAssign(recreateWorkCtx.getPath());
                return;
            }
            if (code == Code.NODEEXISTS) {
                //不理解，如果task还存在，为什么重试？
                log.info("node exists already, bu if it hasn't been deleted, " +
                        "then it will eventually, so we keep trying: {}", path);
                recreateWork(recreateWorkCtx);
                return;
            }
            log.error("recreate work error",
                    KeeperException.create(code, path));
        }
    };

    private final AsyncCallback.VoidCallback assignDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            Code code = Code.get(rc);
            if (code == Code.CONNECTIONLOSS) {
                deleteAssign(path);
                return;
            }
            if (code == Code.OK) {
                log.info("task correctly deleted: {}", path);
                return;
            }
            log.error("failed to delete task data",
                    KeeperException.create(code, path));
        }
    };

}
