package com.zhangyong.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-11 14:02
 * @PS: 在集群上运行流式处理WordCount
 */
object HadoopWordCount {
  def main(args: Array[String]): Unit = {
    // 从外部命令中获取参数
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收 socket 文本流
    val textDstream = env.socketTextStream(host, port)

    // flatMap 和 Map 需要引用的隐式转换
    val dataStream = textDstream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print()

    // 启动 executor，执行任务
    env.execute()
  }

}
