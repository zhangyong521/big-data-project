package com.sxt.datasource


import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 11:25
 * @PS: 自定义Source
 */

/**
 * 自定义的Source，需求：每隔两秒钟，生成10条随机基站日志数据
 * 写一个实现SourceFunction的接口
 */
class MyCustomerSource extends SourceFunction[StationLog] {
  var flag = true

  /**
   * 主要方法，启动一个Source，并且从Source中返回的数据
   * 如果run方法停止，则数据流终止
   * 大部分情况下，都需要在这个run方法中实现一个循环，，这样就可以循环产生数据了
   *
   * @param sourceContext
   */
  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {
    val r = new Random()
    val types = Array("fail", "busy", "barring", "success")

    while (flag) { //如果留没有终止，继续获取数据
      1.to(10).map(i => {
        var callOut = "1860000%04d".format(r.nextInt(10000)) //主叫号码
        var callIn = "1890000%04d".format(r.nextInt(10000)) //被叫号码
        //生成一条通话数据
        new StationLog("station_" + r.nextInt(10), callOut, callIn, types(r.nextInt(4)), System.currentTimeMillis(), r.nextInt(10))
      }).foreach(sourceContext.collect(_)) //发送数据

      Thread.sleep(2000) //每个两秒发送一次数据
    }
  }

  //终止数据
  override def cancel(): Unit = {
    flag = false
  }

}

object CustomerSource {
  def main(args: Array[String]): Unit = {

    //1.初始化
    val StreamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //2.设置并行度
    StreamEnv.setParallelism(1)
    //3.隐式转换
    import org.apache.flink.streaming.api.scala._
    //4.读取数据
    val stream = StreamEnv.addSource(new MyCustomerSource)
    //5.打印数据
    stream.print()
    //6.启动流
    StreamEnv.execute("自定义Source")

  }
}
