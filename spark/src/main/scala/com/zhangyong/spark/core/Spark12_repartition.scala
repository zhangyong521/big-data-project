package com.zhangyong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: map算子
 */
object Spark12_repartition {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //1. coalesce重新分区，可以选择是否进行shuffle过程。由参数shuffle: Boolean = false/true决定。
    //2. repartition实际上是调用的coalesce，默认是进行shuffle的。

    //根据分区数，重新通过网络随机洗牌所有数据。
    val listRDD = sc.makeRDD(1 to 16, 4)

    println("缩减分区前：" + listRDD.partitions.size)

    val coalesceRDD = listRDD.repartition(2)
    println("缩减分区后：" + coalesceRDD.partitions.size)

  }

}
