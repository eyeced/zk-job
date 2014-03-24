package com.emeter.job.zk.sample;

import com.emeter.job.sample.MasterSample;
import com.emeter.job.sample.WorkerSample;
import com.emeter.job.zk.BaseTestCase;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * The Class TestMasterSample
 * User: abhinav
 * Date: 3/18/14
 * Time: 9:02 PM
 */
public class TestMasterSample extends BaseTestCase {

    @Test
    public void testSample() throws Exception {
        MasterSample master = new MasterSample("server1", "localhost:" + port, new ExponentialBackoffRetry(1000, 5));
        master.startZK();
        master.bootstrap();

        WorkerSample worker1 = new WorkerSample(100, "localhost:" + port, new ExponentialBackoffRetry(1000, 5));
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

        System.out.println("Hi");
    }
}
