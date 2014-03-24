package com.emeter.job;

import com.emeter.job.data.JobExecution;
import com.emeter.job.data.JobTrigger;
import com.emeter.job.zk.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;

/**
 * The Class ${CLASSNAME}
 * User: abhinav
 * Date: 3/9/14
 * Time: 10:15 PM
 */
public class Executor {

    /** The logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);

    /** The Random, used for generating random execution time of the trigger */
    private Random random = new Random();

    /**
     * this method executes the job
     *
     * @param jobTrigger
     * @return
     */
    public JobExecution execute(JobTrigger jobTrigger) {
        // get a random number of wait milli seconds for which this job would take time to execute
        // this is just to reciprocate the production environment
        int waitMillis = random.nextInt(500);
        JobExecution jobExecution = new JobExecution();
        jobExecution.setStartTime(new Date());
        jobExecution.setTriggerFireTime(jobTrigger.getNextFireTime());
        try {
            Thread.sleep(waitMillis);
        } catch (InterruptedException e) {
            LOGGER.info("Thread interrupted, exiting - " + e);
        }

        jobExecution.setJobTriggerId(jobTrigger.getId());
        jobExecution.setJobDefId(jobTrigger.getJobDefId());
        jobExecution.setEndTime(new Date());
        return jobExecution;
    }
}
