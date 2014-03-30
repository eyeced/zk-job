package com.emeter.job.zk;

import com.emeter.job.Dispatcher;
import com.emeter.job.Executor;
import com.emeter.job.common.MySharedCount;
import com.emeter.job.data.JobExecution;
import com.emeter.job.data.JobTrigger;
import com.emeter.job.sample.CustomCompressionProvider;
import com.emeter.job.util.BytesUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Class Worker
 *
 * the zookeeper worker client
 *
 * User: abhinav
 * Date: 3/6/14
 * Time: 9:06 PM
 */
public class Worker implements Closeable, Runnable {

    /** The Logger */
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    /** My unique generated id, for reference to my path */
    private String workerId = Integer.toHexString((new Random()).nextInt());

    /** my complete path in zookeeper server. */
    private String myPath;

    /** the count down latch for closing the worker, await for it until the close is called upon by the close method.*/
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /** assign tasks path cache, to keep a tab on the triggers assigned to me. */
    private PathChildrenCache assignCache;

    /** my personal cache, storing the blocking queue capacity */
    // private NodeCache myCache;

    /** the god curator framework client. */
    private final CuratorFramework client;

    /** My counter shared with master. */
    private final MySharedCount counter;

    /** update the completed counter on zk. */
    private final MySharedCount completedCounter;

    /** this manages the counter for every worker's capacity, decreased when a trigger is picked and increases after the work is done */
    private ThreadLocal<AtomicInteger> threadLocal = new ThreadLocal<AtomicInteger>() {
        @Override
        protected AtomicInteger initialValue() {
            return new AtomicInteger(10);
        }
    };

    /** have the completed counter as well, mainly for UI purpose, showing up how many tasks has been completed by the worker.*/
    private ThreadLocal<AtomicInteger> completedThreadLocal = new ThreadLocal<AtomicInteger>() {
        @Override
        protected AtomicInteger initialValue() {
            return new AtomicInteger(0);
        }
    };

    /** listener which listens on the different events fired */
    private PathChildrenCacheListener assignCacheListener = new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED:
                    // task was assigned to me with job trigger id as its data
                    // LOG.info("Worker - Child added for assignment");
                    final Long jobTriggerId = Long.valueOf(BytesUtil.toString(pathChildrenCacheEvent.getData().getData()));
                    if (threadLocal.get().intValue() == 0) {
                        LOG.error("Errrrrrrrrrrrr I am busy !!!!!!!!!!");
                        deleteAssignment(jobTriggerId);
                        deleteFiredTrigger(jobTriggerId, jobTriggerId % 10);
                        setCounter(counter, threadLocal.get().addAndGet(1));
                        // Dispatcher.triggerMap.remove(jobTriggerId);
                    } else {
                        // process the job
                        processJobTrigger(jobTriggerId);
                    }

                    break;
                case CHILD_UPDATED:
                    LOG.info("Child updated");
                    break;
                case INITIALIZED:
                    LOG.info("initialized");
                    break;
                default:
                    LOG.info("Event was fired but not caught - " + pathChildrenCacheEvent.getType().name());
                    break;
            }
        }
    };

    /*
     * In general, it is not a good idea to block the callback thread
     * of the ZooKeeper client. We use a thread pool executor to detach
     * the computation from the callback.
     */
    private ThreadPoolExecutor executor;

    /**
     * Blocking queue used by the executor, it would be used to define the worker selection strategy
     * every time a thread would be freed worker would update the capacity for self.
     */
    private final BlockingQueue<Runnable> blockingQueue;

    /** The job executor which would run the triggers */
    private final Executor jobExecutor;

    /**
     * Creates a new Worker instance.
     *
     * @param hostPort
     */
    public Worker(String hostPort) {
        this.client = CuratorFrameworkFactory.builder().
                compressionProvider(new CustomCompressionProvider()).
                connectString(hostPort).
                sessionTimeoutMs(Master.DEFAULT_SESSION_TIMEOUT_MS).
                connectionTimeoutMs(Master.DEFAULT_CONNECTION_TIMEOUT_MS).
                retryPolicy(new ExponentialBackoffRetry(1000, 5)).
                build();

        this.myPath = Master.EXECUTOR_PATH + "/" + workerId;

        // initiate the assign cache and listen on this for assignments to this worker
        this.assignCache = new PathChildrenCache(client, this.myPath, true);
        // this is the worker cache, initiate it and listen on it for any changes
        // this.myCache = new NodeCache(client, myPath, true);

        // initiate the shared counter, which would be shared by master and this worker
        this.counter = new MySharedCount(client, myPath, 10);

        // initiate the completed counter, this would increase the counter of the total completed jobs by this server.
        this.completedCounter = new MySharedCount(client, "/completed/" + workerId, 0);

        threadLocal.set(new AtomicInteger(10));

        this.blockingQueue = new ArrayBlockingQueue<>(10);
        this.executor = new ThreadPoolExecutor(1, 1,
                1000L,
                TimeUnit.MILLISECONDS,
                blockingQueue);

        this.jobExecutor = new Executor();
    }

    /**
     * initialize the worker
     */
    public void init() {
        try {
            // create the two nodes in here
            // one which show up the capacity of the worker or we can add some config to it
            client.create().withMode(CreateMode.PERSISTENT).inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                    switch (curatorEvent.getType()) {
                        case CREATE:
                            LOG.info("Worker node is created for path - " + curatorEvent.getPath());
                            assignCache.getListenable().addListener(assignCacheListener);
                            assignCache.start();
                            break;
                        default:
                            LOG.info("default case called for event - " + curatorEvent.getType().name());
                            break;

                    }
                }
            }).forPath(myPath, BytesUtil.toBytes(blockingQueue.remainingCapacity()));
        } catch (Exception e) {
            LOG.error("Error while creating worker node path [" + myPath + "] - " + e);
        }
    }

    /**
     * Creates a ZooKeeper session.
     *
     * @throws IOException
     */
    public void startZK() throws IOException {
        client.start();
    }

    @Override
    public void run() {
        try {
            // myCache.getListenable().addListener(myCacheListener);
            // myCache.start();

            counter.start();
            completedCounter.start();
            closeLatch.await();
        } catch (InterruptedException e) {
            LOG.warn("Worker thread was interrupted " + e);
        } catch (Exception e) {
            LOG.warn("Exception came while running the worker [" + myPath + "]" + e);
        }
    }

    /**
     * update the executor capacity
     */
    public void setCounter(MySharedCount counter, int newCount) {
        try {
            counter.setCount(newCount);
        } catch (Exception e) {
            LOG.error("Some exception came while updating the worker[" + myPath + "] capacity - " + e);
        }
    }

    /**
     * delete the fired trigger entry for the given fields
     *
     * @param jobTriggerId the job trigger id
     * @param jobDefId the job definition id
     */
    public void deleteFiredTrigger(final Long jobTriggerId, final Long jobDefId) {
        final String firedTriggerNodePath = Master.FIRED_TRIGGERS_PATH + "/" + jobTriggerId + "_" + jobDefId;
        try {
            // LOG.info("####### Deleting fired trigger for path - " + firedTriggerNodePath);
            client.delete().inBackground().forPath(firedTriggerNodePath);
        } catch (Exception e) {
            LOG.error("Exception came while deleting the fired trigger entry[" + firedTriggerNodePath + "] error -" + e);
        }
    }

    /**
     * Checks if this client is connected.
     *
     * @return boolean
     */
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    public void close() throws IOException {
        LOG.info( "Closing" );
        counter.close();
        assignCache.close();
        client.close();
    }

    /**
     * process the execution of the job trigger,
     * and after execution handle the assignment logic in zookeeper
     *
     * @param jobTriggerId the trigger id
     */
    public void processJobTrigger(Long jobTriggerId) {
        threadLocal.get().addAndGet(-1);
        // submit the job for execution and then wait for future to get the result
        Future<JobExecution> future = submitJob(jobTriggerId);
        try {
            // got the completed or failed job execution.
            future.get();
            // LOG.info("Worker " + myPath + " - capacity - " + (10 - executor.getActiveCount()));
            setCounter(counter, threadLocal.get().addAndGet(1));
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e.toString(), e);
        } finally {
            // Step 2 - delete the job trigger id from the worker assignment path
            deleteAssignment(jobTriggerId);
            // Step 3 - delete the fired trigger entry for the job trigger and job def id
            deleteFiredTrigger(jobTriggerId, jobTriggerId % 10);
            // Dispatcher.triggerMap.remove(jobTriggerId);
            setCounter(completedCounter, completedThreadLocal.get().addAndGet(1));
        }
    }

    /**
     * delete the assigned trigger for this worker
     *
     * @param jobTriggerId the job trigger id, ideally that's the name of the node assigned to this worker
     */
    private void deleteAssignment(Long jobTriggerId) {
        try {
            client.delete().inBackground().forPath(myPath + "/" + jobTriggerId);
        } catch (Exception e) {
            LOG.error("Exception while deleting the node [" + myPath + "/" + jobTriggerId + "]");
        }
    }

    /**
     * execute the submit job trigger id
     *
     * @param jobTriggerId the trigger id
     */
    private Future<JobExecution> submitJob(Long jobTriggerId) {
        return executor.submit(new Callable<JobExecution>() {
            JobTrigger trigger;

            public Callable<JobExecution> init(Long triggerId) {
                this.trigger = Dispatcher.triggerMap.get(triggerId);
                return this;
            }
            @Override
            public JobExecution call() throws Exception {
                return jobExecutor.execute(this.trigger);
            }
        }.init(jobTriggerId));
    }

    /**
     * Main method showing the steps to execute a worker.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {

    }
}
