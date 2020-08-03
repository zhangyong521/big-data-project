package com.zhangyong.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-31 17:33
 * @PS: 读数据
 */
object ReadMysqlRDD {

  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    //2.创建SparkContext连接
    val sc = new SparkContext(config)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/rdd"
    val userName = "root"
    val passWd = "521521"

    //4.创建JDBCRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from rddtable where id >=? and id <= ?",
      1,
      10,
      1,
      rs => ( rs.getInt(1),rs.getString(2))
    )
    //5.打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    //6.关闭
    sc.stop()
  }
}
