package org.example.storage;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryKv implements KvInterface {
    private final ConcurrentMap<String, byte[]> store = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> ttlMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @Override
    public boolean setIfAbsent(String key, byte[] value, long ttlSeconds) {
        if (store.containsKey(key)) {
            return false;
        }
        byte[] oldValue = store.putIfAbsent(key, value);
        if (oldValue == null) {
            if (ttlSeconds > 0) {
                ttlMap.put(key, System.currentTimeMillis() + ttlSeconds * 1000);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean compareAndSet(String key, byte[] oldValue, byte[] newValue, long ttlSeconds) {
        byte[] current = store.get(key);
        if (Arrays.equals(current, oldValue)) {
            store.put(key, newValue);
            if (ttlSeconds > 0) {
                ttlMap.put(key, System.currentTimeMillis() + ttlSeconds * 1000);
            }
            return true;
        }
        return false;
    }

    @Override
    public byte[] get(String key) {
        Long ttl = ttlMap.get(key);
        if (ttl != null && ttl < System.currentTimeMillis()) {
            store.remove(key);
            ttlMap.remove(key);
            return null;
        }
        return store.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        store.put(key, value);
        ttlMap.remove(key);
    }

    @Override
    public long increment(String key) {
        return counters.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    @Override
    public void delete(String key) {
        store.remove(key);
        ttlMap.remove(key);
    }
}