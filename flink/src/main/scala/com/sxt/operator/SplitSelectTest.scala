package com.sxt.operator

import com.sxt.datasource.MyCustomerSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 14:20
 * @PS: Split-Select算子测试
 */
object SplitSelectTest {
  def main(args: Array[String]): Unit = {
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    streamEvn.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val data = streamEvn.addSource(new MyCustomerSource)

    val stream = data.split(
      log => {
        if (log.callType.equals("success")) Seq("Success") else Seq("No Success")
      }
    )
    val stream1 = stream.select("Success")
    val stream2 = stream.select("No Success")

    stream1.print("通话成功")
    stream2.print("通话不成功")

    streamEvn.execute("Split-Select")
  }


}
