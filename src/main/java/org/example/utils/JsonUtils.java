package org.example.utils;

import com.alibaba.fastjson2.JSON;

public class JsonUtils {

    public static byte[] jsonBytes(Object pubData) {
        return JSON.toJSONBytes(pubData);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return JSON.parseObject(data, clazz);
    }
}
