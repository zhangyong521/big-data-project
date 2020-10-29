package com.sxt.datasource

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 11:08
 * @PS: 使用kafka生产key-value键值对数据用于KafkaSource2测试
 */
object CustomProducer {
  def main(args: Array[String]): Unit = {
    //4.获取kafka的配置信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    //随机生产数据
    val r = new Random()
    while (true) {
      val data = new ProducerRecord[String, String]("t_zr", "key" + r.nextInt(10), "value" + r.nextInt(100))
      producer.send(data)
      Thread.sleep(1000)

      //打印产生的数据
      println(data.key(), data.value())
    }

    producer.close()

  }
}
