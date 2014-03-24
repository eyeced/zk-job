package com.emeter.job.route;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.math.BigInteger;
import java.util.List;

/**
 * The class MaxCapacitySelectionStrategy
 *
 * this strategy looks in all the executors capacity, adn returns the executor with maximum capacity
 * <p/>
 * Created by Abhinav on 3/14/14.
 */
public class MaxCapacitySelectionStrategy implements IWorkerSelectionStrategy {

    @Override
    public ChildData select(List<ChildData> workers) {
        Integer max = 0;
        ChildData selected = workers.get(0);
        // go through the loop and find the worker with max capacity
        for (ChildData worker : workers) {
            Integer capacity = new BigInteger(worker.getData()).intValue();
            if (capacity > max) {
                selected = worker;
                max = capacity;
            }
        }
        return max == 0 ? null : selected;
    }
}
