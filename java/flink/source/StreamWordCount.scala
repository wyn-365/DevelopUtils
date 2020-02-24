package com.wang.source

import org.apache.flink.streaming.api.scala._

/**
 * 自定义source
 * 从自定义的集合中 读取source
 * 温度传感器读取数据
 */

case class SensorReading(id:String,timestamp:Long,temperature:Double)

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.自定义source 从自定义集合中读取source
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1",1506773471,35.800183236589),
      SensorReading("sensor_2",1506773471,35.800183236589),
      SensorReading("sensor_3",1506773471,35.800183236589),
      SensorReading("sensor_4",1506773471,35.800183236589),
      SensorReading("sensor_5",1506773471,35.800183236589)
    ))

    //2.从文件中读取数据
    val stream2 = env.readTextFile("a.txt")

    stream1.print("stream1").setParallelism(1)

    env.execute("source test")
  }

}
