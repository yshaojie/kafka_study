package com.self;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author shaojieyue
 * @date 2021/08/25
 */
public class KafkaProductDemo1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.19.49.149:9092");
        //0:主副本无ack 1:主ack
        // all:主副本有ack 其中副本同步数量取决于ISR列表里面的broker数量
        // topic配置中的min.insync.replicas为最小同步数量,作为all情况下,数据可靠性的最低保证
        //min.insync.replicas默认值为1,这种情况下数据只保证在一个副本中存在,所以为了保证数据可靠
        //需要设置min.insync.replicas>1
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        //用来标识生产者,在打日志排除问题时需要
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "业务名-ip:port");
        //提高吞吐量的配置
        //每批发送的最大数据量,为了提高实时性,只要batch里面存在数据,kafkaProducer就会发送出去,
        //不管有没有达到batch.size
        //kafkaProducer会把同一topic-partition的数据放到一个batch里面
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 256*1024);

        //每次提交到broker的数据大小
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024*1024);

        //压缩数据
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        //拦截器,可以在发送前和发送后进行处理
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.self.MyProducerInterceptor");

        //幂等性设置,保证消息不重复发送
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        //发送失败重试次数
        config.put(ProducerConfig.RETRIES_CONFIG, 30);
        //重试发送前的等待时间
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        //每个连接同时发送请求并发数,>1 会造成数据先后顺序混乱
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        /**
         * kafkaProducer有一个线程死循环的把缓存的数据刷到kafka broker,这样会发送很多小包,影响吞吐量
         * 为了解决该问题,可以设置linger.ms,即延迟N ms来刷数据到broker,从而来尽量避免小包,但这样就会造成延迟问题.
         * 默认该值为0,即立刻发送
         * 数据量小的情况下linger.ms的值越大,延迟越高
         */
        config.put(ProducerConfig.LINGER_MS_CONFIG, 100000);

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(config);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord("example", "yue" + LocalTime.now()));
        }

        final Future<RecordMetadata> future = producer.send(new ProducerRecord("example", "yue" + LocalTime.now()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

            }
        });

        final RecordMetadata metadata = future.get();
        producer.flush();
    }
}
