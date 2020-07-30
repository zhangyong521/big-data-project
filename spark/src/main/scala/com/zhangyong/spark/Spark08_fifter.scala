package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: mapPartitionsWithIndex
 */
object Spark08_fifter {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(Array("xiaoming","xiaojiang","xiaohe","dazhi"))

    //过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
    val fifterRDD = listRDD.filter(_.contains("xiao"))
    fifterRDD.collect().foreach(println)

  }
}
