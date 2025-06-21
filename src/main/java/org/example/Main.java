package org.example;

import org.example.core.DistributedChannelOnKv;
import org.example.core.Subscriber;
import org.example.storage.InMemoryKv;
import org.example.storage.KvInterface;

import java.util.Set;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) throws InterruptedException {
        // 初始化 KV 存储
        KvInterface kv = new InMemoryKv();

        // 创建两个节点（模拟分布式环境）
        DistributedChannelOnKv<String> node1 = new DistributedChannelOnKv<>(
                kv, "channel:test", true, 3600, 100);
        DistributedChannelOnKv<String> node2 = new DistributedChannelOnKv<>(
                kv, "channel:test", true, 3600, 100);

        // 创建订阅者（node1）
        Set<String> topics = Set.of("news", "sports");
        Subscriber<String> subscriber1 = node1.createInMemorySubscriber(
                true,
                event -> {
                    System.out.println("订阅者1收到消息: " + event.getData());
                    return true;
                },
                topics);

        // 创建订阅者（node2）
        Subscriber<String> subscriber2 = node2.createPersistSubscriber(
                new LongOnKv(kv, "subscriber:2:version"),
                event -> {
                    System.out.println("订阅者2收到消息: " + event.getData());
                    return true;
                },
                Set.of("news"));

        // 等待领导者选举完成
        Thread.sleep(1000);

        // 发布消息（由领导者节点执行）
        if (node1.isLeader.get()) {
            node1.publish("news", "重大新闻：今天天气晴朗！");
            node1.publish("sports", "体育新闻：比赛取消。");
        } else if (node2.isLeader.get()) {
            node2.publish("news", "重大新闻：今天天气晴朗！");
            node2.publish("sports", "体育新闻：比赛取消。");
        }

        // 等待消息推送
        Thread.sleep(1000);

        // 清理
        node1.removeSubscriber(subscriber1.getId());
        node2.removeSubscriber(subscriber2.getId());
        node1.close();
        node2.close();
    }
}