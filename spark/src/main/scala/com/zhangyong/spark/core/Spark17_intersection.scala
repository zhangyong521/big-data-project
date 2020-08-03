package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 对源RDD和参数RDD求交集后返回一个新的RDD
 */
object Spark17_intersection {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(1 to 7)
    val rdd2 = sc.parallelize(5 to 10)

    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
  }

}
