package com.sxt.operator

import java.text.SimpleDateFormat
import java.util.Date

import com.sxt.datasource.StationLog
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 15:13
 * @PS: 采用自定义函数操作
 *      需求：按照指定的时间格式输出每个通话的拨号时间和结束时间
 */
object FunctionClassTransformation {
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

    //定义时间输出格式
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //过滤那些通话成功的
    data.filter(_.callType.equals("success"))
      .map(new CallMapFunction(format))
      .print()

    streamEvn.execute()
  }

  //自定义的函数类
  class CallMapFunction(format: SimpleDateFormat) extends MapFunction[StationLog, String] {

    override def map(t: StationLog): String = {
      val startTime = t.callTime
      val endTime = t.callTime + t.duration * 1000
      "主叫号码:" + t.callOut + ",被叫号码:" + t.callIn + ",呼叫起始时间:" + format.format(new Date(startTime)) + ",呼叫结束时间:" + format.format(new Date(endTime))
    }

  }

}
