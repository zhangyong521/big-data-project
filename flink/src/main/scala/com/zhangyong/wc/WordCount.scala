package com.zhangyong.wc

import org.apache.flink.api.scala._

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-11 12:37
 * @PS: 用flink写WordCount
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath="D:\\IntelliJ\\IdeaProjects\\big-data-project\\flink\\src\\main\\resources\\hello.txt"

    //读取数据
    val inputPathDataSet = env.readTextFile(inputPath)

    val wordCountDataSet = inputPathDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }

}
