package com.zhangyong.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-07-31 17:33
 * @PS: 写数据
 */
object WriteMysqlRDD {

  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息并建立与spark的连接
    val config = new SparkConf().setMaster("local[*]").setAppName("Practice")
    //2.创建SparkContext连接
    val sc = new SparkContext(config)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/rdd"
    val userName = "root"
    val passWd = "521521"

    val data = sc.makeRDD(List(("zhngyong",10),("zhngyong",40),("zhngyong",30)))


    data.foreachPartition(datas=>{
      //链接对象提出来，每次创建一次
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, userName, passWd)

      datas.foreach{
        case (username,age)=>{
          val sql = "insert into rddtable (name,age) values (?,?) "
          val statement = connection.prepareStatement(sql)
          statement.setString(1,username)
          statement.setInt(2,age)
          statement.executeUpdate()

          statement.close()
        }
      }
      connection.close()
    })

    //6.关闭
    sc.stop()
  }
}
