package com.sxt.datasource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 9:26
 * @PS: 基于集合的source
 */

/**
 * 通信基站日志数据
 *
 * @param sid      基站ID
 * @param callOut  主叫号码
 * @param callIn   被叫号码
 * @param callType 通话类型eg:呼叫失败(fail)，占线(busy),拒接（barring），接通
 *                 (success): * @param callTime 呼叫时间戳，精确到毫秒
 * @Param duration 通话时长 单位：秒
 */
case class StationLog(sid: String, callOut: String, callIn: String, callType: String, callTime: Long, duration: Long)

object CollectionSource {
  def main(args: Array[String]): Unit = {
    //1.初始化
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEvn.setParallelism(1)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.读取数据
    val dataStream = streamEvn.fromCollection(Array(
      new StationLog("001", "186", "189", "busy", 1577071519462L, 0),
      new StationLog("002", "186", "188", "busy", 1577071520462L, 0),
      new StationLog("003", "183", "188", "busy", 1577071521462L, 0),
      new StationLog("004", "186", "188", "success", 1577071522462L, 32)
    ))

    //5.打印
    dataStream.print()

    //6.开启流式
    streamEvn.execute()

  }
}
