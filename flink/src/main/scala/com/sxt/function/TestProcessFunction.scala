package com.sxt.function

import com.sxt.datasource.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 18:59
 * @PS: ProcessFunction 是一个低层次的流处理操作，允许返回所有 Stream 的基础构建模块:
 *      需求：监控每一个手机号，如果在5秒内呼叫它的通话都是失败的，发出警告信息
 *      在5秒中内只要有一个呼叫不是fail则不用警告
 */
object TestProcessFunction {

  def main(args: Array[String]): Unit = {
    val StreamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    StreamEvn.setParallelism(1)

    import org.apache.flink.streaming.api.scala._

    val data = StreamEvn.socketTextStream("hadoop201", 8888)
      .map(line => {
        val arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //处理数据
    data.keyBy(_.callOut)
      .process(new MonitorCallFail())
      .print()

    StreamEvn.execute()
  }

  class MonitorCallFail() extends KeyedProcessFunction[String, StationLog, String] {
    //使用一个状态记录时间
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

    override def processElement(i: StationLog, context: KeyedProcessFunction[String, StationLog, String]#Context, collector: Collector[String]): Unit = {
      //从状态中取得时间
      val time = timeState.value()
      if (i.callType.equals("fail") && time == 0) { //表示第一次发现呼叫当前手机号是失败的
        //获取当前时间，并注册定时器
        val nowTime = context.timerService().currentProcessingTime()
        var onTime = nowTime + 5000L //5秒后触发
        context.timerService().registerEventTimeTimer(onTime)
        timeState.update(onTime)
      }
      if (!i.callType.equals("fail") && time != 0) { //表示有呼叫成功了，可以取消触发器
        context.timerService().deleteProcessingTimeTimer(time)
        timeState.clear()
      }
    }

    //时间到了，执行触发器，发出警告
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
      val warnStr = "触发时间:" + timestamp + " 手机号:" + ctx.getCurrentKey
      out.collect(warnStr)
      timeState.clear()
    }

  }

}
