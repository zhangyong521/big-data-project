package com.sxt.datasource

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//3.导入隐式转换
import org.apache.flink.streaming.api.scala._

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 10:24
 * @PS: 读取 Kafka 中的KeyValue数据
 *      启动kafka，使用生产者在t_zr生产数据
 *      kafka-console-producer.sh --broker-list hadoop201:9092,hadoop202:9092:hadoop203:9092 --topic t_zr
 */
object KafkaSource2 {
  def main(args: Array[String]): Unit = {
    //1.初始化
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEnv.setParallelism(1)

    //4.获取kafka的配置信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
    props.setProperty("group.id", "fink02")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    //设置kafka的数据源
    val stream = streamEnv.addSource(new FlinkKafkaConsumer[(String,String)]("t_zr",new MyKafkaReader,props))

    //打印
    stream.print()

    //启动流式
    streamEnv.execute()
  }

  //自定义一个kafka类，从kafka中读取键值对数据
  class MyKafkaReader extends KafkaDeserializationSchema[(String, String)] {

    override def isEndOfStream(t: (String, String)): Boolean = {
      false
    }

    //反序列
    override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
      if (consumerRecord != null) {
        var key = "null"
        var value = "null"
        if (consumerRecord.key() != null)
          key = new String(consumerRecord.key(), "UTF-8")
        if (consumerRecord.value() != null)
          value = new String(consumerRecord.value(), "UTF-8")

        (key, value)
      } else { //如果kafka中的数据为空返回一个固定的二元组
        ("null", "null")
      }
    }

    //指定类型
    override def getProducedType: TypeInformation[(String, String)] = {
      createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
    }
  }

}
