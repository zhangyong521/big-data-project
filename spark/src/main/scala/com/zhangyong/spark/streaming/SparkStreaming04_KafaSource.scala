package com.zhangyong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-04 11:23
 * @PS: 自定义函数采集器
 */
object SparkStreaming04_KafaSource {
  def main(args: Array[String]): Unit = {
    //spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    //实时数据分析环境对象
    //采集周期,以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //从kafka中采集数据
    val KafkaStream = KafkaUtils.createStream(
      streamingContext,
      "hadoop201:2181",
      "zhangyong",
      Map("zhangyong" -> 3))

    //将采集的数据进行分解（扁平化）
    val wordDStream = KafkaStream.flatMap(t => t._2.split(" "))

    //将数据进行结构的转换方便统计分析
    val mapDStream = wordDStream.map((_, 1))

    //将转换化的数据进行聚合
    val wordToSumStream = mapDStream.reduceByKey(_ + _)

    //打印结构
    wordToSumStream.print()

    //不能停止采集程序
    //streamingContext.stop()

    //启动采集器
    streamingContext.start()

    //Driver等待采集器的执行
    streamingContext.awaitTermination();
  }
}
