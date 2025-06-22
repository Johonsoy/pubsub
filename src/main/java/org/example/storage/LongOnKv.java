package org.example.storage;

import org.example.core.AppendEntry;
import org.example.utils.JsonUtils;

public class LongOnKv {
    private final KvInterface kv;
    private final String key;

    public LongOnKv(KvInterface kv, String key) {
        this.kv = kv;
        this.key = key;
    }

    public long get() {
        byte[] value = kv.get(key);
        return value != null ? Long.parseLong(new String(value)) : 0;
    }

    public void set(long value) {
        kv.set(key, String.valueOf(value).getBytes());
    }

    public boolean compareAndSet(long lastVersion, long nextVersion) {
        return kv.compareAndSet(key, JsonUtils.longToBytes(lastVersion),
                JsonUtils.longToBytes(nextVersion));
    }
}
