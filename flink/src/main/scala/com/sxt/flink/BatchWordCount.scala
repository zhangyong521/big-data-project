package com.sxt.flink

import org.apache.flink.api.scala.ExecutionEnvironment


/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-12 15:22
 * @PS: 本地文件运行
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1、初始化
    val streamEnv = ExecutionEnvironment.getExecutionEnvironment
    //2、导入隐式转换
    import org.apache.flink.api.scala._
    //3、读取数据
    val dataURL = getClass.getResource("/hello.txt")
    val data = streamEnv.readTextFile(dataURL.getPath)
    //4、计算
    val result = data.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    //5、打印结果
    result.print()

  }
}
