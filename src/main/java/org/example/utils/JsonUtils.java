package org.example.utils;

public class JsonUtils {

    public static byte[] jsonBytes(DistributedChannelOnKv.PubData obj) {
        String json = String.format("%s|%s|%d|%s",
                obj.topic(),
                Base64.getEncoder().encodeToString(obj.data()),
                obj.timestamp(),
                obj.address());
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
