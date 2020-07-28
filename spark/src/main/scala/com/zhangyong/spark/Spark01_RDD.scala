package com.zhangyong.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-28 17:10
 * @PS: 创建RDD
 */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运算（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //创建list的RDD集合
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //创建list的RDD集合，并且分区
    val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //使用读取本地文件
    val file = sc.textFile("spark/in")

    //使用读取本地文件并且分区
    val file2 = sc.textFile("spark/in", 2)

    //读取HDFS的文件
    val hdfs = sc.textFile("hdfs://hadoop201:9000/RELEASE")

    //将RRD数据保存到本第文件
    file.saveAsTextFile("spark/out")

    //打印遍历RDD的数据
    //listRDD.collect().foreach(println)
  }
}
