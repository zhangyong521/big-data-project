package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: aggregateByKey的简化操作
 */
object Spark24_foldByKey {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val rdd2 = rdd1.foldByKey(0)(_+_)

    rdd2.collect().foreach(println)


  }

}
