package org.example.core;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.example.storage.KvInterface;
import org.example.storage.LongOnKv;
import org.example.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedChannelOnKv<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedChannelOnKv.class);
    private static final int MIN_RESERVED_COUNT = 64;
    private final AppendLog appendLog;
    private final Map<String, Subscriber<T>> subscriberMap = new ConcurrentHashMap<>();
    private final long maxReservedSeconds;
    private final AtomicBoolean suspend = new AtomicBoolean(false);
    private final ScheduledFuture<?> pushFuture;
    private final ScheduledFuture<?> gcFuture;
    private static final ScheduledExecutorService executorService =
            new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("DistributedChannelOnKv", true));
    private final KvInterface kv;
    private final String keyPrefix;

    public DistributedChannelOnKv(KvInterface kv, String keyPrefix, boolean autoGc, long maxReservedSeconds, long checkIntervalMillis) {
        this.kv = kv;
        this.keyPrefix = keyPrefix;
        this.maxReservedSeconds = maxReservedSeconds;
        this.appendLog = new AppendLog(kv, keyPrefix);

        this.pushFuture = executorService.scheduleWithFixedDelay(
                this::safePush, 0, checkIntervalMillis, TimeUnit.MILLISECONDS);
        if (autoGc) {
            this.gcFuture = executorService.scheduleWithFixedDelay(
                    this::safeGc, 0, 30, TimeUnit.SECONDS);
        } else {
            this.gcFuture = null;
        }
    }

    private Subscriber<T> createSubscriber(
            ReceivedVersion receivedVersion,
            Subscriber.Handler<T> subscriberHandler,
            Set<String> topics) {
        Subscriber<T> subscriber = new Subscriber<>(receivedVersion, subscriberHandler, topics);
        subscriberMap.put(subscriber.getId(), subscriber);
        return subscriber;
    }

    public Subscriber<T> createPersistSubscriber(
            LongOnKv version,
            Subscriber.Handler<T> subscriberHandler,
            Set<String> topics) {
        return createSubscriber(new SeverKVVersion(version), subscriberHandler, topics);
    }

    public Subscriber<T> createInMemorySubscriber(
            boolean consumeHistory,
            Subscriber.Handler<T> subscribeHandler,
            Set<String> topics) {
        long startVersion = consumeHistory ? appendLog.getStartVersion() - 1 : appendLog.getEndVersion();
        Subscriber<T> subscriber = new Subscriber<>(
                new InMemoryVersion(startVersion),
                subscribeHandler,
                topics);
        subscriberMap.put(subscriber.getId(), subscriber);
        return subscriber;
    }

    private void safePush() {
        try {
            if (suspend.get()) {
                return;
            }
            pushToSubscribers();
        } catch (Throwable e) {
            LogUtils.safeError(
                    LOGGER,
                    StringFormatUtils.format("推送失败, 前缀={}", this.appendLog.getPrefix()),
                    e);
        }
    }

    private boolean pushRange(
            Map<String, Subscriber<T>> subscriberMap,
            long fromVersion,
            long toVersion) {
        Map<String, Subscriber<T>> needPush = new ConcurrentHashMap<>();
        for (Subscriber<T> s : subscriberMap.values()) {
            if (s.getPushedVersion() == fromVersion - 1) {
                needPush.put(s.getId(), s);
            }
        }
        if (needPush.isEmpty()) {
            return true;
        }

        LOGGER.debug("开始推送从 {} 到 {}, 前缀={}", fromVersion, toVersion, appendLog.getPrefix());
        Iterator<Pair<Long, byte[]>> iterator = appendLog.iterator(fromVersion);
        while (iterator.hasNext() && !needPush.isEmpty()) {
            Pair<Long, byte[]> pair = iterator.next();
            PubData pubData = PubData.unmarshal(pair.getValue());
            if (!needPush.values().stream().anyMatch(s -> s.getTopics().contains(pubData.getTopic()))) {
                continue;
            }
            T obj = unmarshal(pubData.getData());
            Iterator<Subscriber<T>> subs = needPush.values().iterator();
            while (subs.hasNext()) {
                Subscriber<T> sub = subs.next();
                if (sub.getTopics().contains(pubData.getTopic())) {
                    if (!sub.handlePush(
                            new Subscriber.DataEvent<>(pair.getKey(), obj, pubData.getAddress(), pubData.getTimestamp()))) {
                        subs.remove();
                    }
                }
            }
            LOGGER.debug("已推送版本={}, 前缀={}", pair.getKey(), appendLog.getPrefix());
        }
        return true;
    }

    public void removeSubscriber(String id) {
        subscriberMap.remove(id);
    }

    private void pushToSubscribers() {
        Map<String, Subscriber<T>> subscriberMap = new ConcurrentHashMap<>(this.subscriberMap);
        if (subscriberMap.isEmpty()) {
            return;
        }
        final long lastTruncated = appendLog.getStartVersion() - 1;
        final long lastPublished = appendLog.getEndVersion();
        for (Subscriber<T> subscriber : subscriberMap.values()) {
            if (subscriber.getPushedVersion() < lastTruncated) {
                subscriber.handleBreak(lastTruncated);
            }
        }
        List<Long> pushedVersions = subscriberMap.values().stream()
                .map(Subscriber::getPushedVersion)
                .filter(v -> v != lastPublished)
                .distinct()
                .sorted()
                .toList();
        List<Pair<Long, Long>> pushGroups = new ArrayList<>();
        for (int i = 0; i < pushedVersions.size(); i++) {
            if (i < pushedVersions.size() - 1) {
                pushGroups.add(Pair.of(pushedVersions.get(i) + 1, pushedVersions.get(i + 1)));
            } else {
                pushGroups.add(Pair.of(pushedVersions.get(i) + 1, lastPublished));
            }
        }
        for (Pair<Long, Long> pushGroup : pushGroups) {
            if (!pushRange(subscriberMap, pushGroup.getLeft(), pushGroup.getRight())) {
                return;
            }
        }
    }

    public void close() {
        if (gcFuture != null) {
            gcFuture.cancel(false);
        }
        pushFuture.cancel(false);
    }

    private void safeGc() {
        try {
            if (maxReservedSeconds > 0) {
                gcUntil(System.currentTimeMillis() - maxReservedSeconds * 1000);
            }
        } catch (Throwable e) {
            LogUtils.safeError(
                    LOGGER,
                    StringFormatUtils.format("垃圾回收失败, 前缀={}", appendLog.getPrefix()),
                    e);
        }
    }

    public void gcUntil(long expireTs) {
        long deleteTo = 0;
        long endVersion = appendLog.getEndVersion();
        Iterator<Pair<Long, byte[]>> iterator = appendLog.iterator();
        while (iterator.hasNext()) {
            Pair<Long, byte[]> pair = iterator.next();
            if (pair.getKey() >= endVersion - MIN_RESERVED_COUNT) {
                break;
            }
            try {
                PubData pubData = PubData.unmarshal(pair.getValue());
                if (pubData.getTimestamp() >= expireTs) {
                    break;
                }
            } catch (Throwable e) {
                LOGGER.error(
                        "解析 PubData 失败, 前缀={}, 版本={}, 数据={}, 错误={}",
                        appendLog.getPrefix(),
                        pair.getKey(),
                        Base64.getEncoder().encodeToString(pair.getValue()),
                        e.getMessage());
            }
            deleteTo = Math.max(deleteTo, pair.getKey());
        }
        appendLog.deleteTo(deleteTo);
    }

    public long getLastPublished() {
        return appendLog.getEndVersion();
    }

    public void suspend() {
        suspend.set(true);
    }

    public void resume() {
        suspend.set(false);
    }

    protected byte[] marshal(T data) {
        return data.toString().getBytes(StandardCharsets.UTF_8);
    }

    protected T unmarshal(byte[] data) {
        return (T) new String(data, StandardCharsets.UTF_8);
    }

    public long publish(String topic, T data) {
        PubData pubData = new PubData(
                marshal(data),
                System.currentTimeMillis(),
                NetworkUtils.LOCAL_HOST,
                topic);
        return appendLog.append(pubData.marshal());
    }
}
