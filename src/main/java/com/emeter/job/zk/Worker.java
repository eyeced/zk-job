package com.emeter.job.zk;

import com.emeter.job.Dispatcher;
import com.emeter.job.Executor;
import com.emeter.job.data.JobExecution;
import com.emeter.job.data.JobTrigger;
import com.emeter.job.util.BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;

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

    /** I will listen for the assignments on this node path. */
    private String myAssignPath;

    /** the count down latch for closing the worker, await for it until the close is called upon by the close method.*/
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    /** assign tasks path cache, to keep a tab on the triggers assigned to me. */
    private PathChildrenCache assignCache;

    /** my personal cache, storing the blocking queue capacity */
    private NodeCache myCache;

    /** the god curator framework client. */
    private final CuratorFramework client;

    /** listener which listens on the different events fired */
    private PathChildrenCacheListener assignCacheListener = new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED:
                    // task was assigned to me with job trigger id as its data
                    final Long jobTriggerId = Long.valueOf(BytesUtil.toString(pathChildrenCacheEvent.getData().getData()));
                    if (blockingQueue.remainingCapacity() == 0) {
                        deleteAssignment(jobTriggerId);
                        deleteFiredTrigger(jobTriggerId, jobTriggerId % 10);
                        Dispatcher.triggerMap.remove(jobTriggerId);
                    } else {
                        LOG.info("Job trigger assigned to me id - " + jobTriggerId);
                        // process the job
                        processJobTrigger(jobTriggerId);
                    }

                    break;
                default:
                    break;
            }
        }
    };

    /** my node cache listener, listener for any state change on self. */
    private NodeCacheListener myCacheListener = new NodeCacheListener() {
        @Override
        public void nodeChanged() throws Exception {
            LOG.info("Node changed");
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
    public Worker(String hostPort, RetryPolicy retryPolicy) {
        this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);

        this.myPath = Master.EXECUTOR_PATH + "/" + workerId;

        // initiate the assign cache and listen on this for assignments to this worker
        this.assignCache = new PathChildrenCache(client, Master.EXECUTOR_PATH, true);
        // this is the worker cache, initiate it and listen on it for any changes
        this.myCache = new NodeCache(client, myPath, true);

        this.blockingQueue = new ArrayBlockingQueue<>(200);
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
            client.create().withMode(CreateMode.EPHEMERAL).forPath(myPath, new byte[0]);
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
            myCache.start();
            assignCache.start();
            closeLatch.await();
        } catch (InterruptedException e) {
            LOG.warn("Worker thread was interrupted " + e);
        } catch (Exception e) {
            LOG.warn("Exception came while running the worker [" + myPath + "]" + e);
        }
    }

    /**
     * update the executor capacity
     *
     * @param capacity
     */
    public void updateCapacity(int capacity) {
        try {
            client.setData().inBackground().forPath(myPath, BytesUtil.toBytes(capacity));
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
        final String firedTriggerNodePath = Master.FIRED_TRIGGERS_PATH + "/" + jobTriggerId + "/" + jobDefId;
        try {
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
        return connected;
    }

    /**
     * Checks if ZooKeeper session is expired.
     *
     * @return boolean value
     */
    public boolean isExpired() {
        return expired;
    }

    @Override
    public void close() throws IOException {
        LOG.info( "Closing" );
        myCache.close();
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
        // submit the job for execution and then wait for future to get the result
        Future<JobExecution> future = submitJob(jobTriggerId);
        try {
            // got the completed or failed job execution.
            JobExecution completedJobExecution = future.get();

            // Step 1 - update the capacity of self
            updateCapacity(blockingQueue.remainingCapacity());
            // Step 2 - delete the job trigger id from the worker assignment path
            deleteAssignment(jobTriggerId);
            // Step 3 - delete the fired trigger entry for the job trigger and job def id
            deleteFiredTrigger(jobTriggerId, completedJobExecution.getJobDefId());
            Dispatcher.triggerMap.remove(jobTriggerId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete the assigned trigger for this worker
     *
     * @param jobTriggerId the job trigger id, ideally that's the name of the node assigned to this worker
     */
    private void deleteAssignment(Long jobTriggerId) {
        try {
            client.delete().inBackground().forPath(myAssignPath + "/" + jobTriggerId);
        } catch (Exception e) {
            LOG.error("Exception while deleting the node [" + myAssignPath + "/" + jobTriggerId + "]");
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
                this.trigger = new JobTrigger();
                this.trigger.setId(triggerId);
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
        Worker w = new Worker(args[0], new ExponentialBackoffRetry(1000, 5));
        w.startZK();

        while(!w.isConnected()){
            Thread.sleep(100);
        }

        /*
         * Registers this worker so that the leader knows that
         * it is here.
         */
        while(!w.isExpired()){
            Thread.sleep(1000);
        }
    }
}
