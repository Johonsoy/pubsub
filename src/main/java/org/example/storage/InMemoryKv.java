package org.example.storage;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryKv implements KvInterface {
    private final ConcurrentMap<String, byte[]> store = new ConcurrentHashMap<>();

    @Override
    public boolean setIfAbsent(String key, byte[] value) {
        if (store.containsKey(key)) {
            return false;
        }
        byte[] oldValue = store.putIfAbsent(key, value);
        return oldValue == null;
    }

    @Override
    public boolean compareAndSet(String key, byte[] oldValue, byte[] newValue) {
        byte[] current = store.get(key);
        if (Arrays.equals(current, oldValue)) {
            store.put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public byte[] get(String key) {
        return store.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        store.put(key, value);
    }

    @Override
    public long incrementAndGet() {
        return increment(1);
    }

    private long increment(int delta) {
        Random random = new Random();
        for(int retries = 0; ; retries++) {

        }
    }

    @Override
    public void delete(String key) {
        store.remove(key);
    }
}