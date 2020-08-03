package com.zhangyong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: mapPartitionsWithIndex
 */
object Spark06_glom {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

    //将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    val glomRDD = listRDD.glom()
    glomRDD.collect().foreach(array => {
      println(array.mkString(","))
    })

  }
}
