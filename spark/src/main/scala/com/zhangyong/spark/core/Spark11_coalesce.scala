package com.zhangyong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: map算子
 */
object Spark11_coalesce {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
    val listRDD = sc.makeRDD(1 to 16, 4)

    println("缩减分区前：" + listRDD.partitions.size)

    val coalesceRDD = listRDD.coalesce(3)
    println("缩减分区后：" + coalesceRDD.partitions.size)

  }

}
