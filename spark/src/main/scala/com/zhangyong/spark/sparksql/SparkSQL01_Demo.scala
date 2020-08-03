package com.zhangyong.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-08-03 17:05
 * @PS: Idea的SparkSQL
 */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //2.创建SparkSQL连接

    /*val spark = new SparkSession(sparkConf)*/

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取数据
    val frame = spark.read.json("spark/in/user.json")

    //展示数据
    frame.show()

    //释放资源
    spark.stop()

  }
}
