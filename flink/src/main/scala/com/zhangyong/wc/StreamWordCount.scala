package com.zhangyong.wc

import org.apache.flink.streaming.api.scala._

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-11 14:02
 * @PS: 流式处理WordCount
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //开启流式处理
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val streamDataSet = env.socketTextStream("hadoop201", 7777)

    val streamData = streamDataSet.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    streamData.print()

    //启动execute
    env.execute()
  }
}
