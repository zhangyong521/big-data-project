package com.zhangyong.spark.core
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:36
 * @PS: map算子
 */
object Spark13_sortBy {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD = sc.makeRDD(List(2, 1, 3, 4))
    //用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序
    val sortByRDD = listRDD.sortBy(x => x)
    //按照与3余数的大小排序
    val sortByRDD2 = listRDD.sortBy(x => x % 3)

    sortByRDD.collect().foreach(println)  //1234
    sortByRDD2.collect().foreach(println) //3142
  }

}
