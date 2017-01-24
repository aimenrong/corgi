package corgi.hub.core.mqtt.bean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.util.internal.ConcurrentSet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Created by Terry LIANG on 2017/1/17.
 */
public class VirtualTopicConsumerShard { // S类封装了机器节点的信息 ，如name、password、ip、port等

    private TreeMap<Long, Subscription> nodes; // 虚拟节点到真实节点的映射
    private TreeMap<Long, Subscription> treeKey; //key到真实节点的映射
    private Set<Subscription> shards = new ConcurrentSet<>(); // 真实机器节点
    private final int SUBSCRIPTION_NUM = 100; // 每个机器节点关联的虚拟节点个数
    private boolean flag = false;

    public VirtualTopicConsumerShard(Set<Subscription> shards) {
        super();
        this.shards = shards;
        init();
    }

    private void init() { // 初始化一致性hash环
        nodes = new TreeMap<Long, Subscription>();
        treeKey = new TreeMap<Long, Subscription>();
        Iterator<Subscription> it = shards.iterator();
        while (it.hasNext()) {
            Subscription shardInfo = it.next();
            for (int n = 0; n < SUBSCRIPTION_NUM; n++)
                // 一个真实机器节点关联NODE_NUM个虚拟节点
                nodes.put(hash("SHARD-" + shardInfo + "-SUBSCRIPTION-" + n), shardInfo);
        }
    }

    //增加一个主机
    public void addS(Subscription subscription) {
        if (!shards.contains(subscription)) {
            shards.add(subscription);
            System.out.println("增加主机" + subscription + "的变化：");
            for (int n = 0; n < SUBSCRIPTION_NUM; n++) {
                addS(hash("SHARD-" + subscription.getClientId() + "-NODE-" + n), subscription);
            }
            System.out.println("done");
        }
    }

    //添加一个虚拟节点进环形结构,lg为虚拟节点的hash值
    public void addS(Long lg, Subscription subscription) {
        SortedMap<Long, Subscription> tail = nodes.tailMap(lg);
        SortedMap<Long, Subscription> head = nodes.headMap(lg);
        Long begin = 0L;
        Long end = 0L;
        SortedMap<Long, Subscription> between;
        if (head.size() == 0) {
            between = treeKey.tailMap(nodes.lastKey());
            flag = true;
        } else {
            begin = head.lastKey();
            between = treeKey.subMap(begin, lg);
            flag = false;
        }
        nodes.put(lg, subscription);
        for (Iterator<Long> it = between.keySet().iterator(); it.hasNext(); ) {
            Long lo = it.next();
            if (flag) {
                treeKey.put(lo, nodes.get(lg));
                System.out.println("hash(" + lo + ") change to ->" + tail.get(tail.firstKey()));
            } else {
                treeKey.put(lo, nodes.get(lg));
                System.out.println("hash(" + lo + ") change to ->" + tail.get(tail.firstKey()));
            }
        }
    }

    //删除真实节点是s
    public void deleteS(Subscription Subscription) {
        if (Subscription == null) {
            return;
        }
        System.out.println("Delete subscription " + Subscription + "'s change：");
        for (int i = 0; i < SUBSCRIPTION_NUM; i++) {
            //定位s节点的第i的虚拟节点的位置
            SortedMap<Long, Subscription> tail = nodes.tailMap(hash("SHARD-" + Subscription + "-SUBSCRIPTION-" + i));
            SortedMap<Long, Subscription> head = nodes.headMap(hash("SHARD-" + Subscription.getClientId() + "-SUBSCRIPTION-" + i));
            Long begin = 0L;
            Long end = 0L;

            SortedMap<Long, Subscription> between;
            if (head.size() == 0) {
                between = treeKey.tailMap(nodes.lastKey());
                end = tail.firstKey();
                tail.remove(tail.firstKey());
                nodes.remove(tail.firstKey());//从nodes中删除s节点的第i个虚拟节点
                flag = true;
            } else {
                begin = head.lastKey();
                end = tail.firstKey();
                tail.remove(tail.firstKey());
                between = treeKey.subMap(begin, end);//在s节点的第i个虚拟节点的所有key的集合
                flag = false;
            }
            for (Iterator<Long> it = between.keySet().iterator(); it.hasNext(); ) {
                Long lo = it.next();
                if (flag) {
                    treeKey.put(lo, tail.get(tail.firstKey()));
                    System.out.println("hash(" + lo + ") change to ->" + tail.get(tail.firstKey()));
                } else {
                    treeKey.put(lo, tail.get(tail.firstKey()));
                    System.out.println("hash(" + lo + ") change to ->" + tail.get(tail.firstKey()));
                }
            }
        }

    }

    public static void main(String[] args) {
        Subscription subscription1 = new Subscription();
        subscription1.setClientId("client1");
        subscription1.setTopic("topic1");
        Subscription subscription2 = new Subscription();
        subscription2.setClientId("client2");
        subscription2.setTopic("topic1");
        Set<Subscription> set = new ConcurrentSet<>();
        set.add(subscription1);
        set.add(subscription2);
        VirtualTopicConsumerShard virtualTopicConsumerShard = new VirtualTopicConsumerShard(set);
        virtualTopicConsumerShard.keyToNode("a");
        virtualTopicConsumerShard.keyToNode("b");
        virtualTopicConsumerShard.keyToNode("c");
        virtualTopicConsumerShard.keyToNode("d");
        virtualTopicConsumerShard.keyToNode("a");
        virtualTopicConsumerShard.keyToNode("1");
    }

    //映射key到真实节点
    public Subscription keyToNode(String key) {
        SortedMap<Long, Subscription> tail = nodes.tailMap(hash(key)); // 沿环的顺时针找到一个虚拟节点
        if (tail.size() == 0) {
            return null;
        }
        treeKey.put(hash(key), tail.get(tail.firstKey()));
        Subscription subscription = tail.get(tail.firstKey());
        System.out.println(treeKey.size());
        System.out.println(key + "（hash：" + hash(key) + "）connect to subscription->" + subscription);
        return subscription;
    }

    public int size() {
        return this.shards.size();
    }

    /**
     * MurMurHash算法，是非加密HASH算法，性能很高，
     * 比传统的CRC32,MD5，SHA-1（这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免）
     * 等HASH算法要快很多，而且据说这个算法的碰撞率很低.
     * http://murmurhash.googlepages.com/
     */
    private Long hash(String key) {

        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
            // for big-endian version, do this first:
            // finish.position(8-buf.remaining());
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }

}