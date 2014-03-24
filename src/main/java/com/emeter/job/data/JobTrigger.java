package com.emeter.job.data;

import java.util.Date;

/**
 * The Class ${CLASSNAME}
 * User: abhinav
 * Date: 3/8/14
 * Time: 5:41 PM
 */
public class JobTrigger {

    private Long id;

    private Long jobDefId;

    private String name;

    private Date nextFireTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getNextFireTime() {
        return nextFireTime;
    }

    public void setNextFireTime(Date nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    public Long getJobDefId() {
        return jobDefId;
    }

    public void setJobDefId(Long jobDefId) {
        this.jobDefId = jobDefId;
    }

}
