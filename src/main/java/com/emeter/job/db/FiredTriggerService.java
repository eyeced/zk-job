package com.emeter.job.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * The Class FiredTriggerService
 * User: abhinav
 * Date: 3/30/14
 * Time: 10:40 PM
 */
public class FiredTriggerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FiredTriggerService.class);

    private PostgresDB postgresDB;

    public FiredTriggerService() {
        this.postgresDB = new PostgresDB("jdbc:postgresql://localhost/jobs", "postgres", "postgres");
    }

    /**
     * insert into fired trigger table
     *
     * @param triggerId trigger id
     * @param defId job def id
     */
    public void insert(Long triggerId, Long defId) {
        Statement statement = null;
        try {
            statement = postgresDB.getConnection().createStatement();
            String insertSql = "INSERT INTO jobs.fired_trigger (job_trigger_id, job_def_id) values (" + triggerId + ", " + defId + ")";

            statement.executeUpdate(insertSql);
        } catch (SQLException e) {
            LOGGER.error(e.toString(), e);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.error(e.toString(), e);
            }
        }
    }

    /**
     * delete from fired trigger table
     * @param triggerId trigger id
     * @param defId job def id
     */
    public void delete(Long triggerId, Long defId) {
        Statement statement = null;
        try {
            statement = postgresDB.getConnection().createStatement();
            String insertSql = "DELETE FROM jobs.fired_trigger WHERE job_trigger_id = " + triggerId + " and job_def_id = " + defId;

            statement.executeUpdate(insertSql);
        } catch (SQLException e) {
            LOGGER.error(e.toString(), e);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.error(e.toString(), e);
            }
        }
    }
}
