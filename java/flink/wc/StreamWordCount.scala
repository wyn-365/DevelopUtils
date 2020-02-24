package com.wang.wc
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//流处理
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理环境执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //创建流数据socket
    val dataStream = env.socketTextStream("localhost",7777)

    //处理数据
    val wordCountDataStream = dataStream.flatMap(_.split(""))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    //启动
    env.execute("streaming job")

  }
}
