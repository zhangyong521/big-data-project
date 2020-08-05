package com.zhangyong.kafka;

import org.apache.kafka.clients.producer.*;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-06-29 10:54
 * @PS: kafka生产者
 */
public class CustomProducer {

    /**
     * 不带回调函数的api
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void api1() {
        Properties props = new Properties ();
        props.put ("bootstrap.servers", "hadoop201:9092");//kafka集群，broker-list
        props.put ("acks", "all");
        props.put ("retries", 1);//重试次数
        props.put ("batch.size", 16384);//批次大小
        props.put ("linger.ms", 1);//等待时间
        props.put ("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<> (props);
        for (int i = 0; i < 100; i++) {
            producer.send (new ProducerRecord<> ("first", Integer.toString (i), Integer.toString (i)));
        }
        producer.close ();
    }

    /**
     * 带回调函数的API
     * 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，
     * 分别是RecordMetadata和Exception，如果Exception为null，
     * 说明消息发送成功，如果Exception不为null，说明消息发送失败。
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void api2() {
        Properties props = new Properties ();
        props.put ("bootstrap.servers", "hadoop201:9092");//kafka集群，broker-list
        props.put ("acks", "all");
        props.put ("retries", 1);//重试次数
        props.put ("batch.size", 16384);//批次大小
        props.put ("linger.ms", 1);//等待时间
        props.put ("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<> (props);
        for (int i = 0; i < 100; i++) {
            producer.send (new ProducerRecord<> ("first", Integer.toString (i), Integer.toString (i)), new Callback () {

                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println ("success->" + metadata.offset ());
                    } else {
                        exception.printStackTrace ();
                    }
                }
            });
        }
        producer.close ();
    }

    /**
     * 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void api3() throws ExecutionException, InterruptedException {
        Properties props = new Properties ();
        props.put ("bootstrap.servers", "hadoop201:9092");//kafka集群，broker-list
        props.put ("acks", "all");
        props.put ("retries", 1);//重试次数
        props.put ("batch.size", 16384);//批次大小
        props.put ("linger.ms", 1);//等待时间
        props.put ("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<> (props);
        for (int i = 0; i < 100; i++) {
            producer.send (new ProducerRecord<> ("first", Integer.toString (i), Integer.toString (i))).get ();
        }
        producer.close ();
    }

}
