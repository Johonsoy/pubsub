package org.example.storage;

public interface KvInterface {
    boolean setIfAbsent(String key, byte[] value, long ttlSeconds);
    boolean compareAndSet(String key, byte[] oldValue, byte[] newValue, long ttlSeconds);
    byte[] get(String key);
    void set(String key, byte[] value);
    long increment(String key);
    void delete(String key);
}
