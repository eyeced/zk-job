package com.emeter.job.zk;

import com.emeter.job.data.JobTrigger;
import com.emeter.job.route.IWorkerSelectionStrategy;
import com.emeter.job.util.BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by abhinav on 6/3/14.
 */
public class Master implements Closeable, LeaderSelectorListener {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    public static final String DISPATCHER_PATH = "/dispatchers";
    public static final String EXECUTOR_PATH = "/executors";
    public static final String FIRED_TRIGGERS_PATH = "/firedTriggers";
    public static final String ASSIGN_PATH = "/assign";

    private CountDownLatch leaderLatch = new CountDownLatch(1);
    private CountDownLatch closeLatch = new CountDownLatch(1);

    private final LeaderSelector leaderSelector;
    private CuratorFramework client;

    private String myId;

    /** the executors path children cache, this is from where it will access the registered executors */
    private final PathChildrenCache executorCache;

    private final PathChildrenCache assignmentCache;

    /** executor cache listener with types of events published. */
    PathChildrenCacheListener executorsCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            switch (event.getType()) {
                case CHILD_ADDED:
                    LOG.info("Executor child was added");
                    break;
                case CHILD_UPDATED:
                    LOG.info("Executor child data was ");
                    break;
                case CHILD_REMOVED:
                    LOG.info("Executor child was removed");
                    break;
                case CONNECTION_RECONNECTED:
                    LOG.info("Executor child reconnected");
                    break;
                case INITIALIZED:
                    LOG.info("Executor child was initialized");
                    break;
            }
        }
    };

    /**
     * Creates the master object
     *
     * @param myId unique id for the master
     * @param hostPort list of zookeeper servers comma-separated
     * @param retryPolicy Curator retry policy
     */
    public Master(String myId, String hostPort, RetryPolicy retryPolicy) {
        LOG.info(myId + ": " + hostPort );
        this.myId = myId;
        this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);

        this.leaderSelector = new LeaderSelector(this.client, DISPATCHER_PATH, this);
        this.executorCache = new PathChildrenCache(this.client, EXECUTOR_PATH, true);
        this.assignmentCache = new PathChildrenCache(this.client, ASSIGN_PATH, true);
    }

    /**
     * start the master node
     */
    public void startZk() {
        client.start();
    }

    /**
     * start the leader election process
     */
    public void doRun() {
        LOG.info("Starting master - " + myId);
        leaderSelector.setId(myId);
        leaderSelector.start();
    }

    /**
     * if unable to get the leadership wait for it
     * @throws InterruptedException
     */
    public void awaitLeaderShip() throws InterruptedException {
        leaderLatch.await();
    }

    /**
     * bootstrap server and add some znodes for using it
     *
     * @throws Exception
     */
    public void bootstrap() throws Exception {
        // initiate both groups executors and fired triggers
        // in executors we will assign the new executors joining in
        client.create().forPath(EXECUTOR_PATH, new byte[0]);
        // initiate the assignment group, here every worker would make an entry on it's name
        // now whenever a job trigger is assigned to that worker, a node would be created under the worker's group
        client.create().forPath(ASSIGN_PATH, new byte[0]);
        // in fired triggers, when executor gets a job then it will create a fired trigger znode in the
        // group with a unique_identity of {trigger_id}_{job_def_id} and in it we will set the data in it
        client.create().forPath(FIRED_TRIGGERS_PATH, new byte[0]);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closeLatch.countDown();
        client.close();
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        LOG.info("WooHoo!! I am the leader now - " + myId);

        // register our listener on the executors
        executorCache.getListenable().addListener(executorsCacheListener);
        // start the cache
        executorCache.start();

        // adding this call here to make it wait until the close call is made
        // which lets it exit
        closeLatch.await();
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        switch (connectionState) {
            case CONNECTED:
                // I am connected lets get on with things
                break;
            case RECONNECTED:
                // I am still connected
                break;
            case LOST:
                try {
                    close();
                } catch (IOException e) {
                    LOG.warn("Exception while closing - " + e);
                }
                break;
        }
    }

    /**
     * select a worker from the provided routing strategy and assign the trigger to this worker
     *
     * @param jobTrigger the trigger
     * @param selectionStrategy the worker selection strategy
     */
    public void assignToSelectedWorker(final JobTrigger jobTrigger, IWorkerSelectionStrategy selectionStrategy) {
        // get the worker Id for the selected worker
        final ChildData selectedWorker = selectionStrategy.select(getWorkers());
        final String firedTriggerPath = FIRED_TRIGGERS_PATH + "/" + jobTrigger.getId() + "_" + jobTrigger.getJobDefId();
        if (selectedWorker != null) {
            final String selectedWorkerPath = selectedWorker.getPath();
            final String workerId = selectedWorkerPath.replace("/workers/", "");
            try {
                final int currentCapacity = BytesUtil.toInt(selectedWorker.getData());
                // take a lock on the trigger by creating a node in fired trigger path
                client.create().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        switch (curatorEvent.getType()) {
                            case SET_DATA:
                                if (curatorEvent.getResultCode() == KeeperException.Code.OK.intValue()) {
                                    // lock is being taken now assign it to the worker
                                    assignToWorker(workerId, jobTrigger);
                                    // decrement the capacity of the worker
                                    decrementCounter(selectedWorkerPath, currentCapacity);                             }
                                break;

                            default:
                                break;
                        }

                    }
                }).forPath(firedTriggerPath, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("trigger already assigned");
            } catch (UnsupportedEncodingException e) {
                LOG.error("Error while parsing node data" + e);
            } catch (Exception e) {
                LOG.warn("Unable to create node with path - " + ASSIGN_PATH + "/" + workerId + "/" + jobTrigger.getId());
            }
        }
    }

    /**
     * assign trigger to the given worker
     *
     * @param workerId the worker id, which is the node name of the worker
     * @param jobTrigger the job trigger to be executed
     * @throws Exception
     */
    private void assignToWorker(String workerId, JobTrigger jobTrigger) throws Exception {
        client.create().withMode(CreateMode.EPHEMERAL).inBackground().
                forPath(ASSIGN_PATH + "/" + workerId + "/" + jobTrigger.getId(), BytesUtil.toBytes(jobTrigger.getId().intValue()));
    }

    /**
     * decrement the capacity of that worker
     *
     * @param workerPath worker node path
     * @param capacity capacity of the worker
     * @throws Exception
     */
    private void decrementCounter(String workerPath, int capacity) throws Exception {
        client.setData().inBackground().forPath(workerPath, BytesUtil.toBytes(capacity - 1));
    }

    /**
     * is the client connected
     *
     * @return
     */
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    /**
     * Return the workers from the executor's path cache
     *
     * @return list of workers
     */
    public List<ChildData> getWorkers() {
        return executorCache.getCurrentData();
    }

    /**
     * main method
     * @param args arguments
     */
    public static void main (String[] args) {
        try{
            Master master = new Master("test", "localhost:2181",
                    new ExponentialBackoffRetry(1000, 5));
            master.startZk();
            master.bootstrap();
            master.doRun();
        } catch (Exception e) {
            LOG.error("Exception while running curator master.", e);
        }
    }
}
