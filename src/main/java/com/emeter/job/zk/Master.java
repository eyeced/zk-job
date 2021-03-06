package com.emeter.job.zk;

import com.emeter.job.Dispatcher;
import com.emeter.job.common.MySharedCount;
import com.emeter.job.data.JobTrigger;
import com.emeter.job.route.IWorkerSelectionStrategy;
import com.emeter.job.sample.CustomCompressionProvider;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by abhinav on 6/3/14.
 */
public class Master implements Closeable, LeaderSelectorListener {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    public static final String DISPATCHER_PATH = "/dispatchers";
    public static final String EXECUTOR_PATH = "/executors";
    public static final String FIRED_TRIGGERS_PATH = "/firedTriggers";

    public static final int DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    public static final int DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    private CountDownLatch leaderLatch = new CountDownLatch(1);
    private CountDownLatch closeLatch = new CountDownLatch(1);

    private final LeaderSelector leaderSelector;
    private CuratorFramework client;

    private String myId;

    /** shared counter of the executors with worker path as the key to the shared counter. */
    private final Map<String, MySharedCount> executorCounters = new HashMap<>();

    /** the executors path children cache, this is from where it will access the registered executors */
    private final PathChildrenCache executorCache;

    /** executor cache listener with types of events published. */
    PathChildrenCacheListener executorsCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            switch (event.getType()) {
                case CHILD_ADDED:
                    LOG.info("Executor child was added");
                    initCounterForWorker(event.getData().getPath());
                    break;
                case CHILD_UPDATED:
                    // LOG.info("Executor child data was updated");
                    break;
                case CHILD_REMOVED:
                    // LOG.info("Executor child was removed");
                    try {
                        executorCounters.get(event.getData().getPath()) .close();
                    } catch (IOException e) {
                        LOG.error(e.toString(), e);
                    }
                    break;
                case CONNECTION_RECONNECTED:
                    // LOG.info("Executor child reconnected");
                    break;
                case INITIALIZED:
                    // LOG.info("Executor child was initialized");
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
        this.client = CuratorFrameworkFactory.builder().
                compressionProvider(new CustomCompressionProvider()).
                connectString(hostPort).
                sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS).
                connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS).
                retryPolicy(retryPolicy).
                build();

        this.leaderSelector = new LeaderSelector(this.client, DISPATCHER_PATH, this);
        this.executorCache = new PathChildrenCache(this.client, EXECUTOR_PATH, true);
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

        // in fired triggers, when executor gets a job then it will create a fired trigger znode in the
        // group with a unique_identity of {trigger_id}_{job_def_id} and in it we will set the data in it
        client.create().forPath(FIRED_TRIGGERS_PATH, new byte[0]);

        // create the completed group, here workers will update the total number triggers they have executed
        client.create().forPath("/completed", new byte[0]);
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
        for (String path : executorCounters.keySet()) {
            executorCounters.get(path).close();
        }
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
     * start the process of assigning triggers
     *
     * @param triggers list of triggers
     * @param selectionStrategy selection strategy passed on
     */
    public void assignTriggers(final List<JobTrigger> triggers, final IWorkerSelectionStrategy selectionStrategy) {
        assignTriggersToWorkers(triggers, selectionStrategy, 0);
    }

    /**
     * Recursively assign triggers to the selected workers.
     *
     * There is some dynamic nature to this call, as we are assigning triggers the strategy would change the worker, because as the task would be assigned
     * it will change it's capacity, so the initial call which would start the recursive process would start with index 0
     *
     * the logic in here goes like this -
     * - Take lock on fired trigger entry and attach a callback onto it
     * - fired trigger created, callback would now assign a task for the selected worker with callback
     * - as soon as the trigger is assigned then add a callback to decrement capacity for that worker
     * - as soon we decrement counter recursively callback to the function with increased index
     *
     * @param triggers list of triggers
     * @param selectionStrategy selection strategy passed on
     * @param index at which index we are from where we would need to get the triggers
     */
    private void assignTriggersToWorkers(final List<JobTrigger> triggers, final IWorkerSelectionStrategy selectionStrategy, final int index) {
        // recursion exit check
        if (triggers.size() == index) {
            return;
        }
        final JobTrigger jobTrigger = triggers.get(index);
        // assign to the worker selected by the strategy
        Dispatcher.triggerMap.put(jobTrigger.getId(), jobTrigger);
        final ChildData selectedWorker = selectionStrategy.select(getWorkers());
        final String firedTriggerPath = FIRED_TRIGGERS_PATH + "/" + jobTrigger.getId() + "_" + jobTrigger.getJobDefId();

        if (selectedWorker != null) {
            // LOG.info("Selected worker - " + selectedWorker.getPath());
            final String selectedWorkerPath = selectedWorker.getPath();
            final String workerId = selectedWorkerPath.replace(EXECUTOR_PATH + "/", "");

            try {
                final int currentCapacity = executorCounters.get(selectedWorkerPath).getCount();
                if (currentCapacity == 0) {
                    LOG.info("Worker [" + selectedWorkerPath + "] is full capacity [" + currentCapacity + "]");
                    return;
                }

                // assign task callback, called when the user is able to create the fired trigger for the given trigger
                final BackgroundCallback assignTaskCallback = new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        switch (curatorEvent.getType()) {
                            case CREATE:
                                decrementCounter(selectedWorkerPath);
                                assignTriggersToWorkers(triggers, selectionStrategy, index + 1);
                                break;
                            default:
                                LOG.info("Event not handled 1 - " + curatorEvent.getType().name());
                                break;
                        }
                    }
                };

                // create the fired trigger entry and add this callback which calls on the subsequent task that is to assign the trigger to the selected worker
                final BackgroundCallback firedTriggerCreatedCallback = new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        switch (curatorEvent.getType()) {
                            case CREATE:
                                // LOG.info("Creating assignment node at " + EXECUTOR_PATH + "/" + workerId + "/" + jobTrigger.getId());
                                client.create().withMode(CreateMode.EPHEMERAL).inBackground(assignTaskCallback).
                                        forPath(EXECUTOR_PATH + "/" + workerId + "/" + jobTrigger.getId(), BytesUtil.toBytes(jobTrigger.getId().intValue()));
                                break;
                        }
                    }
                };
                // take a lock on the trigger by creating a node in fired trigger path
                client.create().withMode(CreateMode.EPHEMERAL).inBackground(firedTriggerCreatedCallback).forPath(firedTriggerPath, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                LOG.warn("trigger already assigned");
            } catch (UnsupportedEncodingException e) {
                LOG.error("Error while parsing node data" + e);
            } catch (Exception e) {
                LOG.warn("Unable to create node with path - " + EXECUTOR_PATH + "/" + workerId + "/" + jobTrigger.getId());
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
        // LOG.info("!!!!!!!!!! creating node with path " + EXECUTOR_PATH + "/" + workerId + "/" + jobTrigger.getId() + "!!!!!!!!!!!!!!!");
        client.create().withMode(CreateMode.EPHEMERAL).inBackground().
                forPath(EXECUTOR_PATH + "/" + workerId + "/" + jobTrigger.getId(), BytesUtil.toBytes(jobTrigger.getId().intValue()));
    }

    /**
     * decrement the capacity of that worker
     *
     * @param workerPath worker node path
     * @throws Exception
     */
    private void decrementCounter(String workerPath) throws Exception {
        final MySharedCount counter = executorCounters.get(workerPath);
        int newCount = counter.getCount() - 1;
        // keep trying until you hit the right version update on that counter.
        while (!counter.trySetCount(newCount)) {
            newCount = counter.getCount() - 1;
        }
    }

    /**
     * initialize the shared counter for that worker to accessed by the master in here
     *
     * @param workerPath
     */
    private void initCounterForWorker(String workerPath) {
        // as the counter is already initiated by the worker so it would not matter on setting the initial counter value
        final MySharedCount sharedCount = new MySharedCount(client, workerPath, 10);
        executorCounters.put(workerPath, sharedCount);
        try {
            sharedCount.start();
        } catch (Exception e) {
            LOG.error(e.toString(), e);
        }
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
