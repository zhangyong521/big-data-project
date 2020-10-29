package com.sxt.sink

import com.sxt.datasource.{MyCustomerSource, StationLog}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 16:23
 * @PS: 基于 HDFS 的 Sink
 */
object HDFSFileSink {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._

    val stream = streamEnv.addSource(new MyCustomerSource)
    //创建一个文件滚动规则
    val rolling: DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.create()
      .withInactivityInterval(2000) //不活动的间隔时间。
      .withRolloverInterval(2000) //每隔两秒生成一个文件 ，重要
      .build()

    //创建一个HDFS Sink
    val hdfsSink = StreamingFileSink.forRowFormat[StationLog](
      new Path("hdfs://hadoop201:9000/MySink001/"),
      new SimpleStringEncoder[StationLog]("UTF-8"))
      .withBucketCheckInterval(1000) //检查分桶的间隔时间
      .withRollingPolicy(rolling)
      .build()

    stream.addSink(hdfsSink)

    streamEnv.execute()

  }
}
