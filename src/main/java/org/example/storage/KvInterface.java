package org.example.storage;

public interface KvInterface {
    boolean setIfAbsent(String key, byte[] value);
    boolean compareAndSet(String key, byte[] oldValue, byte[] newValue);
    byte[] get(String key);
    void set(String key, byte[] value);
    long incrementAndGet();
    void delete(String key);
}
