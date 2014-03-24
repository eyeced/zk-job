package com.emeter.job.route;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;

/**
 * The interface IWorkerSelectionStrategy
 *
 * define the strategy to find the executor path in the zoo keeper children
 * <p/>
 * Created by Abhinav Solan on 3/14/14.
 */
public interface IWorkerSelectionStrategy {

    /**
     * Get the path of the worker/executor
     *
     * @param executors List of child data fetched from the zookeeper
     * @return path string for the zookeper
     */
    ChildData select(List<ChildData> executors);
}
