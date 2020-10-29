package com.sxt.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-13 18:46
 * @PS: 基于 Redis 的 Sink
 */
object RedisFileSink {
  def main(args: Array[String]): Unit = {
    //1.初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //2.设置并行度
    streamEnv.setParallelism(1)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.读取数据
    val stream = streamEnv.socketTextStream("hadoop201", 8888)

    //5.转换数据
    val result = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //6.连接redis的配置
    val config = new FlinkJedisPoolConfig.Builder().setDatabase(1).setHost("hadoop201").setPort(6379).build()

    //7.写入redis
    result.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {

      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "t_wc")

      override def getKeyFromData(t: (String, Int)): String = {
        t._1 //单词
      }

      override def getValueFromData(t: (String, Int)): String = {
        t._2 + "" //单词出现的次数
      }

    }))

    streamEnv.execute("flink-redis")

  }

}
