package com.emeter.job.route;

import com.emeter.job.util.BytesUtil;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * The class MaxCapacitySelectionStrategy
 *
 * this strategy looks in all the executors capacity, adn returns the executor with maximum capacity
 * <p/>
 * Created by Abhinav on 3/14/14.
 */
public class MaxCapacitySelectionStrategy implements IWorkerSelectionStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaxCapacitySelectionStrategy.class);

    @Override
    public ChildData select(List<ChildData> workers) {
        int max = 0;
        int min = 10;
        String workerName = "";
        if (workers.size() > 0) {
            ChildData selected = workers.get(0);
            // go through the loop and find the worker with max capacity
            for (ChildData worker : workers) {
                try {
                    int capacity = BytesUtil.toInt(worker.getData());
                    if (capacity < min) {
                        min = capacity;
                        workerName = worker.getPath();
                    }
                    if (capacity > max) {
                        selected = worker;
                        max = capacity;
                    }
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            LOGGER.info("Found min " + min + " for worker -" + workerName);
            return max == 0 ? null : selected;
        }
        return null;
    }
}
