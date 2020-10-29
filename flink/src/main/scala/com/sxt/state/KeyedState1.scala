package com.sxt.state

import com.sxt.operator.StationLog
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-17 12:22
 * @PS: Keyed State 案例(第一种方式)
 *      案例需求：计算每个手机的呼叫间隔时间，单位是毫秒。
 */
object KeyedState1 {
  def main(args: Array[String]): Unit = {
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    streamEvn.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val data = streamEvn.readTextFile(getClass.getResource("/station.log").getPath)
      .map(
        line => {
          val arr = line.split(",")
          new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
        }
      )

    data.keyBy(_.callIn) //按照呼叫手机号分组
      .flatMap(new CallIntervalFunction)
      .print()

    streamEvn.execute()

  }

  class CallIntervalFunction() extends RichFlatMapFunction[StationLog, (String, Long)] {
    //定义一个保存前一条呼叫的数据的转态对象
    private var preData: ValueState[StationLog] = _

    override def open(parameters: Configuration): Unit = {
      preData = getRuntimeContext.getState(new ValueStateDescriptor[StationLog]("pre", classOf[StationLog]))
    }

    override def flatMap(in: StationLog, collector: Collector[(String, Long)]): Unit = {
      val preCallTime = preData.value()
      if (preCallTime == null || preCallTime == 0) {
        preData.update(in)
      } else { //如果状态中有值则计算时间间隔
        var interval = in.callTime - preCallTime.callTime
        collector.collect((in.callIn, interval))
      }
    }
  }

}
