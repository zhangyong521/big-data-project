package com.zhangyong.spark

import scala.util.parsing.json.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:10
 * @PS: 创建RDD
 */
object Spark41_JSON {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val json = sc.textFile("spark/in/user.json")
    val result = json.map(JSON.parseFull)
    result.foreach(println)

    //释放资源
    sc.stop()
  }
}
