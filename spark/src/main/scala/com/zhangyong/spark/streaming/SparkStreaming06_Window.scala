package com.zhangyong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-04 11:23
 * @PS: 滑动窗口
 */
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    val ints = List(1, 2, 3, 4, 5, 6)

    //滑动窗口函数
    val intses = ints.sliding(3, 3)
    for (list <- intses) {
      println(list.mkString(","))
    }
  }
}
