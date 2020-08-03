package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: aggregateByKey的简化操作
 */
object Spark26_sortByKey {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((3, "aa"), (6, "cc"),(2,"bb"),(1,"dd")))


    //正序
    val rdd2 = rdd1.sortByKey(true)

    rdd2.collect().foreach(println)

    //倒序
    val rdd3 = rdd1.sortByKey(false)

    rdd3.collect().foreach(println)

  }

}
