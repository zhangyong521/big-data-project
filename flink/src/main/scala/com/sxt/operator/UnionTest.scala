package com.sxt.operator

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author: 张勇
 * @Blog: https://blog.csdn.net/zy13765287861
 * @Version: 1.0
 * @Date: 2020-10-16 9:15
 * @PS: Union算子的使用
 */
object UnionTest {
  def main(args: Array[String]): Unit = {
    //1.创建连接
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //2.设置并行度
    streamEnv.setParallelism(1)
    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.数据源(创建三组数据)
    val stream1 = streamEnv.fromElements(("a",1),("b",2),("c",3))
    val stream2 = streamEnv.fromElements(("d",1),("e",2),("f",3))
    val stream3 = streamEnv.fromElements(("d",1),("g",2),("h",3))

    val stream = stream1.union(stream2,stream3)

    stream.print()

    streamEnv.execute()
  }

}
