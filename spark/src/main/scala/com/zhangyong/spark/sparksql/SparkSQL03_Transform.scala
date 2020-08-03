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
object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {

    //1.初始化spark配置信息并建立与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //2.创建SparkSQL连接

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (1, "mazi", 40)))

    /**
     * 进行转换之前，需要引入隐式转换规则
     * 这里的spark不是包名的含义，是SparkSession对象的名字
     */

    import spark.implicits._

    //转化为DF
    val df = rdd.toDF("id", "name", "age")

    //转化为DS
    val ds = df.as[User]

    //转化为DF
    val df1 = ds.toDF()

    //转化为RDD
    val rdd1 = df1.rdd

    rdd1.foreach(row => {
      //通过索引来拿数据
      println(row.getString(1))
    })

    //释放资源
    spark.stop()

  }

  //转化为DS必须要
  case class User(id: Int, name: String, age: Int)
}

