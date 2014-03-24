package com.emeter.job.sample;

import org.apache.curator.framework.api.CompressionProvider;

/**
 * The Class CustomCompressionProvider
 * User: abhinav
 * Date: 3/18/14
 * Time: 10:05 PM
 */
public class CustomCompressionProvider implements CompressionProvider {
    @Override
    public byte[] compress(String path, byte[] data) throws Exception {
        return data;
    }

    @Override
    public byte[] decompress(String path, byte[] compressedData) throws Exception {
        return compressedData;
    }
}
