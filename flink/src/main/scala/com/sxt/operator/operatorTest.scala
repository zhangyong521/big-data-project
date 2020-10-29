package com.sxt.operator

import com.sxt.datasource.MyCustomerSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-15 17:01
 * @PS: 算子练习
 *
 *      需求：从自定义的数据源中获取日志，统计每个基站通话时间的总时长
 */
object operatorTest {
  def main(args: Array[String]): Unit = {

    //1.创建连接
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.设置并行度
    env.setParallelism(1)
    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //4.获取数据源
    val data = env.addSource(new MyCustomerSource)

    val result = data.filter(_.callType.equals("success"))
      .map(log => {
        (log.sid, log.duration)
      }) //转化为二元组
      .keyBy(0)
      .reduce((t1, t2) => {
        val duration = t2._2
        (t1._1, duration)
      })

    result.print()

    env.execute("打印")
  }

}
