package com.sxt.operator

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 15:57
 * @PS: 自定义复函数
 */
case class StationLog(sid: String, var callOut: String, var callIn: String, callType: String, callTime: Long, duration: Long)

object RichFunctionClassTransformation {
  def main(args: Array[String]): Unit = {
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    streamEvn.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    //读取数据
    val data = streamEvn.readTextFile(getClass.getResource("/station.log").getPath)
      .map(
        line => {
          val arr = line.split(",")
          new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
        }
      )

    //过滤那些通话成功的
    data.filter(_.callType.equals("success"))
      .map(new CallRichMapFunction())
      .print()

    streamEvn.execute()
  }

  //自定义的复函数类
  class CallRichMapFunction() extends RichMapFunction[StationLog, StationLog] {

    var conn: Connection = _
    var pst: PreparedStatement = _

    //生命周期管理，初始化的时候创建数据库连接
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "521521")
      pst = conn.prepareStatement("select name from t_phone where phone_number=?")
    }

    override def map(in: StationLog): StationLog = {
      //查询主叫用户名的名字
      pst.setString(1, in.callOut)
      val set1: ResultSet = pst.executeQuery()
      if (set1.next()) {
        in.callOut = set1.getString(1)
      }

      //查询被主叫用户的名字
      pst.setString(1, in.callIn)
      val set2: ResultSet = pst.executeQuery()
      if (set2.next()) {
        in.callIn = set2.getString(1)
      }
      in
    }

    //关闭连接
    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }

}
