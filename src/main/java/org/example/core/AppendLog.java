package org.example.core;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.example.storage.KvInterface;
import org.example.storage.LongOnKv;
import org.example.utils.ConcurrentUtils;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@Slf4j
public class AppendLog {

    private final KvInterface kv;
    @Getter
    private final String prefix;

    private final LongOnKv nextVersion;
    private final LongOnKv lastPublished;

    private final LongOnKv firstVersion;

    private final String rootDir;

    public AppendLog(KvInterface kv, String prefix) {
        this.kv = kv;
        this.prefix = prefix;
        this.nextVersion = new LongOnKv(kv, prefix + ":version");
        this.lastPublished = new LongOnKv(kv, prefix + ":lastPublished");
        this.firstVersion = new LongOnKv(kv, prefix + ":firstVersion");
        this.rootDir = prefix + ":data:";
    }

    /**
     * 将数据附加到存储系统中
     * 此方法用于将一组字节数据附加到存储系统中，它通过版本控制来确保数据的正确性和顺序
     *
     * @param data 要附加的字节数组，代表待存储的数据
     * @return 返回当前操作的数据版本号，用于内部版本控制和一致性保证
     */
    public long append(byte[] data) {
        // 持续尝试直到数据成功附加
        for (; ; ) {
            // 获取当前已发布数据的最新版本
            long lastVersion = this.lastPublished.get();
            // 获取下一个期望的版本号
            long nextVersion = this.nextVersion.get();
            // 尝试设置数据，如果失败则重新尝试
            if (!setData(nextVersion, data)) {
                continue;
            }
            // 检查版本连续性，如果当前版本与上一个版本不连续，则尝试填充空缺版本
            if (nextVersion > lastVersion + 1) {
                // 再次确认，以防止并发修改
                lastVersion = this.lastPublished.get();
                if (nextVersion > lastVersion + 1) {
                    tryFillEmpty(lastVersion + 1, nextVersion);
                }
            }
            setEndVersion(nextVersion);
            return nextVersion;
        }
    }

    private void setEndVersion(long nextVersion) {
        for (;;) {
            long lastVersion = lastPublished.get();
            if (lastVersion >= nextVersion) {
                return;
            }
            if (lastPublished.compareAndSet(lastVersion, nextVersion));
        }
    }


    private void tryFillEmpty(long startVersion, long endVersion) {
        List<Long> mayBeEmpty = LongStream.range(startVersion, endVersion)
                .boxed()
                .toList();
        log.info("Try to fill empty from {} to end {} prefix = {}.", startVersion, endVersion, prefix);

        long expiredTime = System.currentTimeMillis() + 1000;
        while (!mayBeEmpty.isEmpty() && expiredTime > System.currentTimeMillis()) {
            mayBeEmpty.removeIf(version -> getData(version) != null);
            ConcurrentUtils.sleep(10, TimeUnit.MICROSECONDS);
        }
        mayBeEmpty.forEach(this::setEmpty);
    }

    private void setEmpty(Long aLong) {
        kv.set(rootDir + aLong, new AppendEntry(null, true).marshal());
    }

    private AppendEntry getData(Long version) {
        String key = rootDir + version;
        byte[] bytes = kv.get(key);
        if (bytes == null) {
            return null;
        }
        return AppendEntry.unmarshal(bytes);
    }

    private boolean setData(long nextVersion, byte[] data) {
        String key = rootDir + nextVersion;
        return kv.setIfAbsent(key, data);
    }

    public long getStartVersion() {
        return 1;
    }

    public long getEndVersion() {
        byte[] version = kv.get(prefix + ":lastPublished");
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
