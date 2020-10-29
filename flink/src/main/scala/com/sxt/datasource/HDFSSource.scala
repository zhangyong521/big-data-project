package com.sxt.datasource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 9:10
 * @PS: 从HDFS中获取数据
 */
object HDFSSource {
  def main(args: Array[String]): Unit = {
    //1.初始化flink
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEvn.setParallelism(1)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.读取数据
    val stream = streamEvn.readTextFile("hdfs://hadoop201:9000/hello.txt")

    //5.转换数据
    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //6.打印结果
    result.print()

    //7.启动流式处理
    streamEvn.execute("wc")

  }

}
