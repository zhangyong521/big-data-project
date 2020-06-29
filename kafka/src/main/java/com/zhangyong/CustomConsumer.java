package com.zhangyong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-29 10:54
 * @PS: kafka生产者
 */
public class CustomConsumer {

    public static void main(String[] args) {
        Properties props = new Properties ();
        props.put ("bootstrap.servers", "hadoop201:9092");
        props.put ("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put ("enable.auto.commit", "false");//自动提交offset

        props.put ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<> (props);
        consumer.subscribe (Arrays.asList ("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll (100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf ("offset = %d, key = %s, value = %s%n", record.offset (), record.key (), record.value ());
            }
            consumer.commitSync ();
        }
    }
}
