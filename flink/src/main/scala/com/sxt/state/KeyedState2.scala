package com.sxt.state

import com.sxt.operator.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-17 16:51
 * @PS: Keyed State 案例(第二种方式)
 *      案例需求：计算每个手机的呼叫间隔时间，单位是毫秒。
 */
object KeyedState2 {
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
      .flatMapWithState[(String, Long), StationLog] {
        case (in: StationLog, None) => (List.empty, Some(in)) //如果状态中没有。则存存入
        case (in: StationLog, pre: Some[StationLog]) => { //如果状态中有值则计算时间间隔
          val interval = math.abs(in.callTime - pre.get.callTime)
          (List((in.callIn, interval)), Some(in))
        }
      }
      .print()

    streamEvn.execute()

  }
}
