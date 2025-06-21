package org.example.core;

import org.example.storage.LongOnKv;

public interface ReceivedVersion {
    long get();

    void set(long version);
}


