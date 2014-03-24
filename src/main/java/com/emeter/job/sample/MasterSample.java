package com.emeter.job.sample;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

/**
 * The class MasterSample
 *
 * This class is the sample master class working on a sample of events
 *
 * This would be a game between the master and workers where every worker gets up with a certain count data in it which is an even number
 * now as soon as a worker is added master decrements the counter of the worker's data, and as soon as the workers counter is decremented
 * the worker then itself decrements it's counter mainly it will only decrement the odd number, and as soon as the counter is set to 0 worker deletes
 * the self znode from the cache
 *
 * <p/>
 * Created by ic033930 on 3/14/14.
 */
public class MasterSample implements Closeable, LeaderSelectorListener {

    private static final int        DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int        DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSample.class);

    // the unique id for the Master
    private String myId;

    // path at which the workers would be added
    public static final String WORKER_PATH = "/workers";

    // the curator framework client which would handle all path cache getting and setting of the data
    private CuratorFramework client;
    private final LeaderSelector leaderSelector;
    private final PathChildrenCache workerCache;

    // latch for making master wait in the take leadership call
    private CountDownLatch closeLatch = new CountDownLatch(1);

    private PathChildrenCacheListener workerCacheListener = new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED:
                    // as soon as the child is added get it's data and if the value is even then reduce it
                    decrementCounter(pathChildrenCacheEvent.getData().getPath());
                    break;
                case CHILD_UPDATED:
                    // so the data in child is updated carry on the reducing task here as well
                    decrementCounter(pathChildrenCacheEvent.getData().getPath());
                    break;
                case CONNECTION_LOST:
                    // connection is lost from the child remove the child from the group
                    removeChild(pathChildrenCacheEvent.getData().getPath());
                    break;
                default:
                    break;
            }
        }
    };

    /**
     * create the master
     *
     * @param myId master id
     * @param hostPort port on which it is to be connected to ZK
     * @param retryPolicy the retry policy for ZK
     */
    public MasterSample(String myId, String hostPort, RetryPolicy retryPolicy) {
        this.myId = myId;
        this.client = CuratorFrameworkFactory.builder().
                compressionProvider(new CustomCompressionProvider()).
                connectString(hostPort).
                sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS).
                connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS).
                retryPolicy(retryPolicy).
                build();

        this.leaderSelector = new LeaderSelector(this.client, "/master", this);
        this.workerCache = new PathChildrenCache(this.client, WORKER_PATH, true);
    }

    /**
     * decrement the data of the node
     *
     * @param path path of the node whose counter is to be decremented
     */
    private void decrementCounter(String path) {
        LOGGER.info("############################## Decrementing Counter from master ########################################");
        try {
            Integer counter = fromBytes(client.getData().forPath(path));
            LOGGER.info("Current Value [" + counter + "]");
            if (counter % 2 == 0) {
                updateData(path, toBytes(counter - 1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Update the data for the node
     *
     * @param path path of the node
     * @param data data to be updated
     * @throws Exception
     */
    private void updateData(String path, byte[] data) throws Exception {
        client.setData().inBackground().forPath(path, data);
    }

    /**
     * remove the child
     *
     * @param path path of child to be removed
     */
    private void removeChild(String path) {
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            LOGGER.warn("Exception came while deleting the node for path " + path + " - exception - " + e);
        }
    }

    /**
     * start the zookeeper client
     */
    public void startZK() {
        client.start();
    }

    /**
     * Bootstrap the groups in it like the worker group
     *
     * @throws Exception
     */
    public void bootstrap() throws Exception {
        client.create().forPath(WORKER_PATH, new byte[0]);
    }

    /**
     * run the master
     */
    public void runForMaster() {
        LOGGER.info("Starting master selection: " + myId);
        leaderSelector.setId(myId);
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        client.close();
        leaderSelector.close();
        closeLatch.countDown();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

        LOGGER.info( "Mastership participants: " + myId + ", " + leaderSelector.getParticipants() );

        workerCache.getListenable().addListener(workerCacheListener);
        workerCache.start();

        // don't let this method quit unless the close is called upon
        closeLatch.await();
    }

    public static byte[] toBytes(int value) throws UnsupportedEncodingException {
        return String.valueOf(value).getBytes("UTF-8");
    }

    public static int fromBytes(byte[] bytes) throws UnsupportedEncodingException {
        return Integer.valueOf(new String(bytes, "UTF-8")).intValue();
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        switch (connectionState) {
            case CONNECTED:
                break;
            case RECONNECTED:
                break;
            case SUSPENDED:
                break;
            case LOST:
                try {
                    close();
                } catch (IOException e) {
                    LOGGER.warn("Exception while closing" + e);
                }
                break;
            default:
                break;

        }
    }

    /**
     * is the client connected to zk server or not
     *
     * @return boolean value
     */
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    public static void main(String[] args) {
        try{
            MasterSample master = new MasterSample("server1", "localhost:2181", new ExponentialBackoffRetry(1000, 5));
            master.startZK();
            master.bootstrap();

            WorkerSample worker1 = new WorkerSample(100, "localhost:2181", new ExponentialBackoffRetry(1000, 5));
            // WorkerSample worker2 = new WorkerSample(1000, "192.168.80.109:2181", new ExponentialBackoffRetry(1000, 5));
            // WorkerSample worker3 = new WorkerSample(500, "192.168.80.109:2181", new ExponentialBackoffRetry(1000, 5));

            worker1.startZK();
            // worker2.startZK();
            // worker3.startZK();

            worker1.bootstrap();

            while(!worker1.isConnected()) {
                Thread.sleep(1000);
            }

            Thread workerThread1 = new Thread(worker1, "WorkerThread1");
            workerThread1.start();

            master.runForMaster();
        } catch (Exception e) {
            LOGGER.error("Exception while running curator master.", e);
        }
    }
}
