package com.sxt.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-14 9:05
 * @PS: flink作为字符串的形式写入kafka
 *     使用消费者查看是否正确：
 *    kafka-console-consumer.sh --bootstrap-server hadoop201:9092,hadoop202:9092,hadoop203:9092 --topic t_2020 --from-beginning
 */
object KafkaSinkByString {

  /**
   * 需求，把netcat数据源中每个单词写入kafka中
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //1.初始化
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEvn.setParallelism(1)

    //3.隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.读取数据netcat的数据，
    val stream = streamEvn.socketTextStream("hadoop201", 8888)

    //5.处理单词
    val words = stream.flatMap(_.split(" "))

    //6.把单词写入kafka
    words.addSink(new FlinkKafkaProducer[String]("hadoop201:9092,hadoop202:9092,hadoop203:9092", "t_2020", new SimpleStringSchema()))

    //启动
    streamEvn.execute("flink-kafka")
  }
}
