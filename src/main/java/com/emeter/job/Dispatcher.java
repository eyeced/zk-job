package com.emeter.job;

import com.emeter.job.data.JobTrigger;
import com.emeter.job.route.IWorkerSelectionStrategy;
import com.emeter.job.route.MaxCapacitySelectionStrategy;
import com.emeter.job.zk.Master;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by abhinav on 6/3/14.
 */
public class Dispatcher implements Runnable {

    /** The Logger */
    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    // create the zookeeper master
    private Master master;

    // my unique id assigned to me when I am created
    private String myId;

    // hostname of the zk server
    private String hostName;

    // the routing strategy
    private IWorkerSelectionStrategy routeStrategy;

    // initiate the map for triggers this is where we would store the job trigger related data
    public static final ConcurrentHashMap<Long, JobTrigger> triggerMap = new ConcurrentHashMap<>();

    private Random rand = new Random(100);

    /**
     * create the dispatcher with zk master settings
     *
     * @param myId my unique id
     * @param hostName zk server host name
     */
    public Dispatcher(String myId, String hostName) {
        this.myId = myId;
        this.hostName = hostName;

        routeStrategy = new MaxCapacitySelectionStrategy();
        master = new Master(this.myId, this.hostName, new ExponentialBackoffRetry(1000, 5));
        // start the master client
        master.startZk();
        // now create the groups in it executors and fired_triggers
        try {
            master.bootstrap();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * initialize the dispatcher zk leader election process
     */
    public void startZK() throws Exception {
        // start the master leader election process
        master.doRun();
    }

    /**
     * start the polling process in here, fetch the triggers then on basis of the Load Balancing algorithm assign it to specific server
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(2000);
                List<JobTrigger> triggers = getTriggers();
                master.assignTriggers(triggers, routeStrategy);
                // Thread.sleep(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get the list of triggers which are to be run
     *
     * @return
     */
    private List<JobTrigger> getTriggers() {
        List<JobTrigger> triggers = new ArrayList<>(10);
        for (int i = 0; i < 40; i++) {
            JobTrigger trigger = new JobTrigger();
            Long id = new Long(rand.nextInt(100));
            trigger.setId(id);
            trigger.setJobDefId(id % 10);
            trigger.setNextFireTime(new Date());
            triggers.add(trigger);
        }
        return triggers;
    }
}
