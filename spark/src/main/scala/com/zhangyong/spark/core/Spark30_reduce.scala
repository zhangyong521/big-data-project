package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
 */
object Spark30_reduce {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.makeRDD(1 to 10, 2)
    val rdd3 = rdd1.reduce(_ + _)

    val rdd2 = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))
    val rdd4 = rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    println(rdd3)
    println(rdd4)
  }

}
