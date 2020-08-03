package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 针对（K,V）类型的RDD，返回一个（K，Int）的map，表示每一个key对应的元素个数。
 */
object Spark40_persist {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.makeRDD(Array("zhangyong"))

    //将RDD转换为携带当前时间戳不做缓存
    val nocache = rdd.map(_.toString + System.currentTimeMillis)
    nocache.collect().foreach(println)

    //将RDD转换为携带当前时间戳并做缓存
    val cache = rdd.map(_.toString + System.currentTimeMillis).cache
    cache.collect().foreach(println)


  }

}
