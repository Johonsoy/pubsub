package org.example.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class NamedThreadFactory implements ThreadFactory {

    private final String namePrefix;
    private final boolean daemon;
    private final AtomicLong threadNumber = new AtomicLong(0);

    public NamedThreadFactory(String namePrefix, boolean daemon) {
        this.namePrefix = namePrefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, namePrefix + "-" + threadNumber.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}
