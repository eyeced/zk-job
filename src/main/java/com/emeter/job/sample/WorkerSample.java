package com.emeter.job.sample;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * The class WorkerSample
 *
 * class only criteria is when starting up then register to the worker's cache
 * now whenever master reduces the counter then after that worker will reduce the counter
 *
 * and when the counter is set to 0 then it will delete itself from the group
 *
 * <p/>
 * Created by Abhinav on 3/14/14.
 */
public class WorkerSample implements Closeable, Runnable {

    private static final int        DEFAULT_SESSION_TIMEOUT_MS = Integer.getInteger("curator-default-session-timeout", 60 * 1000);
    private static final int        DEFAULT_CONNECTION_TIMEOUT_MS = Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

    private String name = Integer.toHexString((new Random()).nextInt());
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerSample.class);
    private Integer counter;
    private CuratorFramework client;
    private String myPath;

    private final CountDownLatch closeLatch = new CountDownLatch(1);

    private final NodeCache myCache;

    private final NodeCacheListener cacheListener = new NodeCacheListener() {
        @Override
        public void nodeChanged() throws Exception {
            LOGGER.info("###################### Node Changed ###################");
            decrementData();
        }
    };

    /**
     * decrement the data for this worker
     */
    private void decrementData() {
        LOGGER.info("############################## Decrementing Counter from Worker ########################################");
        try {
            Integer current = MasterSample.fromBytes(client.getData().forPath(myPath));
            LOGGER.info("Current Value [" + current + "]");
            if (current == 0) {
                close();
            }
            if (current % 2 == 1) {
                client.setData().inBackground().forPath(myPath, MasterSample.toBytes(current - 1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * create the worker
     *
     * @param hostPort port to which ZK is to be connected
     * @param retryPolicy retry policy for curator client
     */
    public WorkerSample(Integer counter, String hostPort, RetryPolicy retryPolicy) {
        this.counter = counter;
        this.client = CuratorFrameworkFactory.builder().
                compressionProvider(new CustomCompressionProvider()).
                connectString(hostPort).
                sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS).
                connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS).
                retryPolicy(retryPolicy).
                build();
        this.myPath = MasterSample.WORKER_PATH + "/" + name;
        this.myCache = new NodeCache(this.client, myPath, true);
    }

    /**
     * bootstrap the worker node
     *
     * @throws Exception
     */
    public void bootstrap() throws Exception {
        client.create().withMode(CreateMode.EPHEMERAL).forPath(myPath, String.valueOf(counter).getBytes());
    }

    /**
     * start the client
     */
    public void startZK() {
        client.start();
    }


    @Override
    public void run() {
        myCache.getListenable().addListener(cacheListener);
        try {
            myCache.start();
            closeLatch.await();
        } catch (Exception e) {
            LOGGER.error("Exception while starting worker - " + e);
        }
    }

    /**
     * call the close when connection is lost
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        myCache.close();
        client.close();
        closeLatch.countDown();
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
        int val = 4;
        // byte[] bytes = MasterSample.toBytes(100);
        // int nVal = MasterSample.fromBytes(bytes);
        // System.out.println("#######" + nVal);
    }
}