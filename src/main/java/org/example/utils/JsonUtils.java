package org.example.utils;

import com.alibaba.fastjson2.JSON;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class JsonUtils {

    public static byte[] jsonBytes(Object pubData) {
        return JSON.toJSONBytes(pubData);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return JSON.parseObject(data, clazz);
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(value);
        return buffer.array();
    }
}
