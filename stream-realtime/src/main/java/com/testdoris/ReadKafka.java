package com.testdoris;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ReadKafka {
    public static void main(String[] args) {
        // 配置 Kafka 消费者属性
        Properties props = new Properties();
        // Kafka 服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092");
        // 消费者组 ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        // 键的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 值的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 自动偏移重置策略
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建 Kafka 消费者
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 订阅主题
            String topic = "realtime_log";
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("开始消费 Kafka 主题: " + topic);

            // 持续消费消息
            while (true) {
                // 拉取消息，超时时间为 100 毫秒
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息信息
                    System.out.printf("分区: %d, 偏移量: %d, 键: %s, 值: %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}

