package com.emeter.job.util;

import java.io.UnsupportedEncodingException;

/**
 * The class BytesUtil
 *
 * utility class for getting value from bytes and to bytes
 * <p/>
 * Created by ic033930 on 3/19/14.
 */
public class BytesUtil {

    /**
     * convert int to bytes
     *
     * @param value int value
     * @return byte[] array
     * @throws UnsupportedEncodingException
     */
    public static byte[] toBytes(int value) throws UnsupportedEncodingException {
        return toBytes(String.valueOf(value));
    }

    /**
     * get the bytes from the string value
     *
     * @param value the string
     * @return byte array
     * @throws UnsupportedEncodingException
     */
    public static byte[] toBytes(String value) throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    /**
     * get int value from bytes
     *
     * @param bytes array of bytes
     * @return int value
     * @throws UnsupportedEncodingException
     */
    public static int toInt(byte[] bytes) throws UnsupportedEncodingException {
        return Integer.valueOf(toString(bytes)).intValue();
    }

    /**
     * get the string value from the bytes
     *
     * @param bytes byte array
     * @return String value
     * @throws UnsupportedEncodingException
     */
    public static String toString(byte[] bytes) throws UnsupportedEncodingException {
        return new String(bytes, "UTF-8");
    }
}
