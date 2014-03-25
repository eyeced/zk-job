package com.emeter.job.zk;

import com.emeter.job.Dispatcher;
import org.junit.Test;

/**
 * The class TestJob
 * <p/>
 * Created by Abhinav on 3/25/2014.
 */
public class TestJob {

    @Test
    public void testJob() {
        Dispatcher dispatcher = new Dispatcher("server1", "192.168.80.109:2181");

        Worker worker1 = new Worker("192.168.80.109:2181");
        Worker worker2 = new Worker("192.168.80.109:2181");
        Worker worker3 = new Worker("192.168.80.109:2181");
        Worker worker4 = new Worker("192.168.80.109:2181");

        try {
            // start the worker zookeeper
            worker1.startZK();
            worker1.init();
            worker2.startZK();
            worker2.init();
            worker3.startZK();
            worker3.init();
            worker4.startZK();
            worker4.init();

            while (!worker1.isConnected() || !worker2.isConnected() || !worker3.isConnected() || !worker4.isConnected()) {
                Thread.sleep(1000);
            }

            // create the worker thread and keep it in ready state to receive tasks
            Thread worker1Thread = new Thread(worker1, "worker1");
            worker1Thread.start();

            Thread worker2Thread = new Thread(worker1, "worker2");
            worker2Thread.start();
            Thread worker3Thread = new Thread(worker1, "worker3");
            worker3Thread.start();
            Thread worker4Thread = new Thread(worker1, "worker4");
            worker4Thread.start();

            dispatcher.startZK();
            // start the dispatcher process ideally it should have been started in thread
            dispatcher.run();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
