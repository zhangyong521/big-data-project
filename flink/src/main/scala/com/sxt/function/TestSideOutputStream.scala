package com.sxt.function

import com.sxt.datasource.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 19:46
 * @PS:
 */
object TestSideOutputStream {

  import org.apache.flink.streaming.api.scala._

  //侧输出流首先需要定义一个流的标签
  var notSuccessTag = new OutputTag[StationLog]("not_success")


  def main(args: Array[String]): Unit = {
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment
    streamEvn.setParallelism(1)

    val data = streamEvn.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        val arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    //得到主流
    val mainStream = data.process(new CreateSideOutputStream(notSuccessTag))

    //得到测流
    val sideOutput = mainStream.getSideOutput(notSuccessTag)
    mainStream.print("通话成功")
    sideOutput.print("通话未成功")

    streamEvn.execute()

  }

  class CreateSideOutputStream(tag: OutputTag[StationLog]) extends ProcessFunction[StationLog, StationLog] {

    override def processElement(i: StationLog, context: ProcessFunction[StationLog, StationLog]#Context, collector: Collector[StationLog]): Unit = {
      if (i.callType.equals("success")) { //输出主流
        collector.collect(i)
      } else { //输出测流
        context.output(tag, i)
      }
    }

  }

}
