package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: groupByKey也是对每个key进行操作，但只生成一个sequence
 */
object Spark21_groupByKey {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val words = Array("one","two","two","three","three","three")


    val rdd2 = sc.parallelize(words).map(word=>(word,1))

    val rdd3 = rdd2.groupByKey()

    rdd3.collect().foreach(println)

    val rdd4 = rdd3.map(t=>(t._1,t._2.sum))

    rdd4.collect().foreach(println)





  }

}
