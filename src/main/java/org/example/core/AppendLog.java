package org.example.core;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.example.storage.KvInterface;

import java.util.Iterator;

public class AppendLog {

    private final KvInterface kv;
    @Getter
    private final String prefix;

    public AppendLog(KvInterface kv, String prefix) {
        this.kv = kv;
        this.prefix = prefix;
    }

    public long append(byte[] data) {
        long version = kv.increment(prefix + ":version");
        kv.set(prefix + ":log:" + version, data);
        return version;
    }

    public long getStartVersion() {
        return 1;
    }

    public long getEndVersion() {
        byte[] version = kv.get(prefix + ":version");
        return version != null ? Long.parseLong(new String(version)) : 0;
    }

    public Iterator<Pair<Long, byte[]>> iterator() {
        return iterator(getStartVersion());
    }

    public Iterator<Pair<Long, byte[]>> iterator(long fromVersion) {
        return new Iterator<>() {
            long current = fromVersion;

            @Override
            public boolean hasNext() {
                return current <= getEndVersion();
            }

            @Override
            public Pair<Long, byte[]> next() {
                byte[] data = kv.get(prefix + ":log:" + current);
                return Pair.of(current++, data);
            }
        };
    }

    public void deleteTo(long version) {
        for (long i = getStartVersion(); i <= version; i++) {
            kv.delete(prefix + ":log:" + i);
        }
    }

}
