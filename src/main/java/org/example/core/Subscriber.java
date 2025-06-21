package org.example.core;

import java.util.Set;
import java.util.UUID;

public class Subscriber<T> {
    private final String id = UUID.randomUUID().toString();
    private final ReceivedVersion version;
    private final Handler<T> handler;
    private final Set<String> topics;
    private volatile long pushedVersion;

    public interface Handler<T> {
        boolean handle(Subscriber.DataEvent<T> event);
    }

    public static class DataEvent<T> {
        private final long version;
        private final T data;
        private final String address;
        private final long timestamp;

        public DataEvent(long version, T data, String address, long timestamp) {
            this.version = version;
            this.data = data;
            this.address = address;
            this.timestamp = timestamp;
        }

        public T getData() {
            return data;
        }
    }

    public Subscriber(ReceivedVersion version, Handler<T> handler, Set<String> topics) {
        this.version = version;
        this.handler = handler;
        this.topics = topics;
        this.pushedVersion = version.get();
    }

    public String getId() {
        return id;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public long getPushedVersion() {
        return pushedVersion;
    }

    public boolean handlePush(DataEvent<T> event) {
        if (handler.handle(event)) {
            pushedVersion = event.version;
            version.set(pushedVersion);
            return true;
        }
        return false;
    }

    public void handleBreak(long lastTruncated) {
        pushedVersion = lastTruncated;
        version.set(pushedVersion);
    }
}
