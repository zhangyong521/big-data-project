package com.sxt.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-12 15:02
 * @PS: flink的流式处理
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1、初始化
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //2、导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3、读取数据
    val stream = streamEnv.socketTextStream("hadoop201", 8888)

    //4、转换计算
    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)


    //5、打印结果到控制台
    result.print()

    //6、启动
    streamEnv.execute("wc")

  }

}
