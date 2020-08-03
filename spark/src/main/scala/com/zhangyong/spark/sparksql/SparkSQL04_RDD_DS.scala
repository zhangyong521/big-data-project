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
object SparkSQL04_RDD_DS {
  def main(args: Array[String]): Unit = {

    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //2.创建SparkSQL连接

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "mazi", 40)))

    /**
     * 进行转换之前，需要引入隐式转换规则
     * 这里的spark不是包名的含义，是SparkSession对象的名字
     */
    import spark.implicits._

    //RDD转化为DataSet
    val UserRDD = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS = UserRDD.toDS()
    val rdd1 = userDS.rdd

    rdd1.foreach(println)

    //释放资源
    spark.stop()

  }

  //转化为DS必须要
  case class User(id: Int, name: String, age: Int)
}

