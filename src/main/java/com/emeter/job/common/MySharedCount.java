package com.emeter.job.common;

import com.emeter.job.util.BytesUtil;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.shared.*;
import org.apache.curator.framework.state.ConnectionState;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * The Class MySharedCount
 *
 * User: abhinav
 * Date: 3/26/14
 * Time: 3:04 PM
 */
public class MySharedCount implements Closeable, SharedCountReader, Listenable<SharedCountListener> {
    /**
     * @param client    the client
     * @param path      the shared path - i.e. where the shared count is stored
     * @param seedValue the initial value for the count if/f the path has not yet been created
     */
    public MySharedCount(CuratorFramework client, String path, int seedValue) {
        sharedValue = new SharedValue(client, path, toBytes(seedValue));
    }

    private final Map<SharedCountListener, SharedValueListener> listeners = Maps.newConcurrentMap();
    private final SharedValue sharedValue;

    public int getCount()
    {
        return fromBytes(sharedValue.getValue());
    }

    /**
     * Change the shared count value irrespective of its previous state
     *
     * @param newCount new value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     setCount(int newCount) throws Exception
    {
        sharedValue.setValue(toBytes(newCount));
    }

    /**
     * Changes the shared count only if its value has not changed since this client last
     * read it. If the count has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the count is not successful you can get the updated value
     * by calling {@link #getCount()}.
     *
     * @param newCount the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getCount()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean  trySetCount(int newCount) throws Exception
    {
        return sharedValue.trySetValue(toBytes(newCount));
    }

    public void     addListener(SharedCountListener listener)
    {
        addListener(listener, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public void     addListener(final SharedCountListener listener, Executor executor)
    {
        SharedValueListener     valueListener = new SharedValueListener()
        {
            @Override
            public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception
            {
                listener.countHasChanged(MySharedCount.this, fromBytes(newValue));
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState)
            {
                listener.stateChanged(client, newState);
            }
        };
        sharedValue.getListenable().addListener(valueListener, executor);
        listeners.put(listener, valueListener);
    }

    public void     removeListener(SharedCountListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * The shared count must be started before it can be used. Call {@link #close()} when you are
     * finished with the shared count
     *
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     start() throws Exception
    {
        sharedValue.start();
    }

    public void close() throws IOException
    {
        sharedValue.close();
    }

    private static byte[]   toBytes(int value)
    {
        try {
            return BytesUtil.toBytes(value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static int      fromBytes(byte[] bytes)
    {
        try {
            return BytesUtil.toInt(bytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
