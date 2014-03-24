package com.emeter.job;

import com.emeter.job.data.JobTrigger;
import com.emeter.job.route.IWorkerSelectionStrategy;
import com.emeter.job.route.MaxCapacitySelectionStrategy;
import com.emeter.job.zk.Master;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by abhinav on 6/3/14.
 */
public class Dispatcher {

    // create the zookeeper master
    private Master master;

    // my unique id assigned to me when I am created
    private String myId;

    // hostname of the zk server
    private String hostName;

    // the routing strategy
    private IWorkerSelectionStrategy routeStrategy;

    private Random rand = new Random(100);

    /**
     * initialize the dispatcher
     */
    public void onInit() throws Exception {
        routeStrategy = new MaxCapacitySelectionStrategy();
        master = new Master(myId, hostName, new ExponentialBackoffRetry(1000, 5));
        // start the master client
        master.startZk();
        // now create the groups in it executors and fired_triggers
        master.bootstrap();
        // start the master leader election process
        master.doRun();
    }

    /**
     * start the polling process in here, fetch the triggers then on basis of the Load Balancing algorithm assign it to specific server
     */
    public void start() {
        while (true) {
            try {
                Thread.sleep(2000);
                List<JobTrigger> triggers = getTriggers();
                for (JobTrigger trigger : triggers) {
                    // assign to the worker selected by the strategy
                    master.assignToSelectedWorker(trigger, routeStrategy);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
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
        for (int i = 0; i < 10; i++) {
            JobTrigger trigger = new JobTrigger();
            Long id = new Long(rand.nextInt());
            trigger.setId(id);
            trigger.setJobDefId(id % 10 + 1);
            trigger.setNextFireTime(new Date());
            triggers.add(trigger);
        }
        return triggers;
    }
}
