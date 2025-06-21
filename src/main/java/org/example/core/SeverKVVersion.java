package org.example.core;

import org.example.storage.LongOnKv;

public class SeverKVVersion implements ReceivedVersion{

    private final LongOnKv version;

    public SeverKVVersion(LongOnKv version) {
        this.version = version;
    }

    @Override
    public long get() {
        return version.get();
    }

    @Override
    public void set(long version) {
        this.version.set(version);
    }
}
