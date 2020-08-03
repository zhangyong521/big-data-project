package com.zhangyong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: mapPartitionsWithIndex
 */
object Spark09_sample {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(1 to 10)

    //以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
    //val sampleRDD = listRDD.sample(true,0.4,2)

    val sampleRDD = listRDD.sample(false,0.2,3)

    sampleRDD.collect().foreach(println)

  }
}
