package com.emeter.job.data;

import java.util.Date;

/**
 * The Class ${CLASSNAME}
 * User: abhinav
 * Date: 3/9/14
 * Time: 11:20 PM
 */
public class JobExecution {

    private Long jobTriggerId;

    private Date startTime;

    private Date endTime;

    private Date triggerFireTime;

    private Long jobDefId;

    public Long getJobDefId() {
        return jobDefId;
    }

    public void setJobDefId(Long jobDefId) {
        this.jobDefId = jobDefId;
    }

    public Long getJobTriggerId() {
        return jobTriggerId;
    }

    public void setJobTriggerId(Long jobTriggerId) {
        this.jobTriggerId = jobTriggerId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getTriggerFireTime() {
        return triggerFireTime;
    }

    public void setTriggerFireTime(Date triggerFireTime) {
        this.triggerFireTime = triggerFireTime;
    }
}
