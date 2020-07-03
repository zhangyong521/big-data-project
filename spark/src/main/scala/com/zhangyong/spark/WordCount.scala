package com.zhangyong.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-03 19:43
 * @PS: 使用IDEA工具完成Spark WordCount开发
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //读取文件，将文件内容一行一行的读取出来
    val lines: RDD[String] = sc.textFile("spark/in")
    //将一行一行的数据分解一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //为了统计方便，将单词数据进行结构化转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //对转换后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //将统计结果采集后打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()
    result.foreach(println)

    //关闭连接
    sc.stop()

  }
}
