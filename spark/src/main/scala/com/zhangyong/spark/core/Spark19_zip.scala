package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 将两个RDD组合成Key/value形式的RDD
 */
object Spark19_zip {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(Array(1,2,3),3)
    val rdd2 = sc.parallelize(Array("a","b","c"),3)

    val rdd3 = rdd1.zip(rdd2)
    rdd3.collect().foreach(println)
  }

}
