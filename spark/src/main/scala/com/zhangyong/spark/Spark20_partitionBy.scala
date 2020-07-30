package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 对pairRDD进行分区操作，
 */
object Spark20_partitionBy {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbbb"),(3,"cccc"),(4,"ddd")),4)

    println("分区前：" + rdd1.partitions.size)

    val rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))

    rdd2.collect().foreach(println)
    println("分区后：" + rdd2.partitions.size)



  }

}
