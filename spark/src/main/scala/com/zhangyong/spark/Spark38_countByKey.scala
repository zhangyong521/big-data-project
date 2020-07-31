package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 针对（K,V）类型的RDD，返回一个（K，Int）的map，表示每一个key对应的元素个数。
 */
object Spark38_countByKey {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.makeRDD(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val rdd1 = rdd.countByKey()

    println(rdd1)
  }

}
