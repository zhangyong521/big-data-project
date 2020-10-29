package com.sxt.sink

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.sxt.datasource.{MyCustomerSource, StationLog}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-15 8:31
 * @PS: 自定义sink
 *      从自定义的source中读取stationLog的数据，通过Flink写入MySQL数据库
 */
object CustomJdbcSink {

  //自定义一个Sink写入mysql
  class MyCustomSink extends RichSinkFunction[StationLog] {
    var conn: Connection = _
    var pst: PreparedStatement = _

    //生命周期管理，在Sink初始化的时候调用
    override def open(parameters: Configuration): Unit = {

      conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "521521")
      pst = conn.prepareStatement("insert into t_station_log(sid,call_out,call_in,call_type,call_time,duration) values (?,?,?,?,?,?)")

    }

    //把StationLog写入到表t_station_log
    override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
      pst.setString(1, value.sid)
      pst.setString(2, value.callOut)
      pst.setString(3, value.callIn)
      pst.setString(4, value.callType)
      pst.setLong(5, value.callTime)
      pst.setLong(6, value.duration)

      pst.executeUpdate()
    }


    //关闭资源
    override def close(): Unit = {
      pst.close()
      conn.close()
    }

  }

  def main(args: Array[String]): Unit = {
    //1.初始化
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    //2.设置并行度
    streamEvn.setParallelism(1)

    //3.隐式转换
    import org.apache.flink.streaming.api.scala._

    //获取数据源
    val data = streamEvn.addSource(new MyCustomerSource)

    //将数据写入mysql
    data.addSink(new MyCustomSink)

    //启动
    streamEvn.execute("fink-mysql")
  }
}
