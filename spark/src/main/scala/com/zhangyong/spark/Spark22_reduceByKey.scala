package com.zhangyong.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 将相同的可以聚合到一起，
 */
object Spark22_reduceByKey {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(List(("zhangyong",1),("zhangyong",3),("zhangsan",1),("zhangsan",1)))

    val rdd2 = rdd1.reduceByKey((x,y)=>x+y)

    rdd2.collect().foreach(println)


  }

}
