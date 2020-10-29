package com.sxt.sink

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 19:23
 * @PS: 基于 Kafka 的 Sink
 *     kafka-console-consumer.sh --bootstrap-server hadoop201:9092,hadoop202:9092,hadoop203:9092 --topic t_2020 --from-beginning --property print.key=true
 */
object KafkaSinkByKeyValue {
  def main(args: Array[String]): Unit = {
    //1.初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度、默认情况下每个任务的并行度为1
    streamEnv.setParallelism(1)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.读取netcat流中数据（实时流）
    val stream = streamEnv.socketTextStream("hadoop201", 8888)

    //转换计算
    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //kafka的生产配置
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")

    //将数据写入KAFKA，并且是KeyValue格式的数据
    result.addSink(new FlinkKafkaProducer[(String, Int)](
      "t_2020",
      new KafkaSerializationSchema[(String, Int)] { //自定义的匿名内部类
        override def serialize(t: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("t_2020", t._1.getBytes, (t._2 + "").getBytes())
        }
      },
      props,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    )

    streamEnv.execute("flink-kafka")

  }


}
