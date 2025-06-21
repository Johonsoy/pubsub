package org.example.core;

public class InMemoryVersion implements ReceivedVersion{
    private volatile long version;

    public InMemoryVersion(long version) {
        this.version = version;
    }

    @Override
    public long get() {
        return version;
    }

    @Override
    public void set(long version) {
        this.version = version;
    }
}
