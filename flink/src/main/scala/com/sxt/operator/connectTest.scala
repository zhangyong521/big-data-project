package com.sxt.operator

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 10:06
 * @PS: connect算子测试
 */
object connectTest {
  def main(args: Array[String]): Unit = {
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    streamEvn.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val data1 = streamEvn.fromElements(("a", 1), ("c", 1), ("d", 1), ("f", 1))
    val data2 = streamEvn.fromElements(1, 2, 3, 4)

    val data = data1.connect(data2)

    val stream = data.map(
      //第一个处理函数
      t1 => {
        (t1._1, t1._2)
      },
      t2 => {
        (t2, 0)
      }
    )
    stream.print()

    streamEvn.execute()
  }

}
