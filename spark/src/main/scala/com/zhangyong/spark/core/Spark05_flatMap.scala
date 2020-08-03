package com.zhangyong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: mapPartitionsWithIndex
 */
object Spark05_flatMap {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(Array(List(1,2),List(3,4)))

    val flatMapRDD = listRDD.flatMap(datas=>datas)

    flatMapRDD.collect().foreach(println)
  }
}
