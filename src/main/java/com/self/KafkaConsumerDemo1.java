package com.self;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author shaojieyue
 * @date 2021/08/26
 */

public class KafkaConsumerDemo1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.Deserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.Deserializer");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  "172.19.49.149:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
        //发送心跳的频率,建议为session.timeout.ms的1/3
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        //限制每个partition每次获取的数据量,默认1Mb
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);

        //它是用户指定的一个consumer成员ID。每个消费者组下这些ID必须是唯一的。
        // 一旦设置了该ID，该消费者就会被视为是一个静态成员（StaticMember）。
        // 静态成员配以较大的session超时设置能够避免因成员临时不可用（比如重启）而引发的Rebalance。
        // 由此可见，消费者组静态成员是2.3版本新引入的一个概念，主要是为了避免不必要的Rebalance。
//        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,null );

        //当定位不到offset时,读取数据的策略,默认从最新的消息开始消费
        //定位不到offset有两种情况
        //1: group是一个新的,还没有提交offset到kafka broker
        //2: kafka broker存储了该group的offset,但是,该offset的数据已经过期被kafka清除掉
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //当前客户端连接会话超时时间,超过这个时间,broker则认为该连接已经失效
        //就会把当前客户端连接移除掉,然后进行消费的Rebalance
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 9000);

        //是否自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //自动提交offset的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);


        //读取数据时的隔离级别
        //read_committed:仅仅返回已经做了事务提交的数据
        //read_uncommitted:返回所有的消息,甚至已经中止事务中的消息
        //两者的区分只是在于事务消息,read_uncommitted理论上是能读取到脏数据的
        //所以这里建议统一使用read_committed
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        //两次调用 poll()之间的最大间隔时间
        //假如超过这个时间,就会有可能进行Rebalance操作,请看以下逻辑
        //if(isNotBlank(group.instance.id)){
        //  1.consumer will stop sending heartbeats
        //  2.partitions will be reassigned after expiration of session.timeout.ms
        // }else{
        //  执行Rebalance
        // }
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        //每次获取消息条数,注意获取太多导致处理时间边长,有可能引起Rebalance
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        //吞吐量和实时性的取舍
        //默认1byte,也就是注重实时性
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
    }
}
