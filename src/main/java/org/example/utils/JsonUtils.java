package org.example.utils;

import org.example.core.DistributedChannelOnKv;
import org.example.core.PubData;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class JsonUtils {

    public static byte[] jsonBytes(PubData pubData) {
        String json = String.format("%s|%s|%d|%s",
                pubData.getTopic(),
                Base64.getEncoder().encodeToString(pubData.getData()),
                pubData.getTimestamp(),
                pubData.getTopic());
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        if (clazz != DistributedChannelOnKv.PubData.class) {
            throw new IllegalArgumentException("Only PubData is supported");
        }
        String[] parts = new String(data, StandardCharsets.UTF_8).split("\\|");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid PubData format: " + new String(data));
        }
        return (T) new DistributedChannelOnKv.PubData(
                Base64.getDecoder().decode(parts[1]),
                Long.parseLong(parts[2]),
                parts[3],
                parts[0]);
    }
}
