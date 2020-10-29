package com.sxt.datasource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 9:56
 * @PS: 读取 Kafka 中的普通数据（String）
 *     启动kafka，使用生产者在t_zr生产数据
 *     kafka-console-producer.sh --broker-list hadoop201:9092,hadoop202:9092:hadoop203:9092 --topic t_zr
 */
object KafkaSource1 {
  def main(args: Array[String]): Unit = {
    //1.初始化
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEvn.setParallelism(1)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3.获取kafka的配置
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
    props.setProperty("group.id", "fink01")
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    //4.设置kafka为数据源
    val stream = streamEvn.addSource(new FlinkKafkaConsumer[String]("t_zr", new SimpleStringSchema(), props))

    stream.print()

    streamEvn.execute("kafka")
  }
}
