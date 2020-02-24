package com.wang.transform

/**
 * 各种算子的操作
 */

import com.wang.source.SensorReading
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//为程序设置并行度
    //读取文件
    val streamFromFile = env.readTextFile("D:\\APP\\IDEA\\workplace\\HelloFlink\\src\\main\\resources\\sensor.txt")

    val dataStream = streamFromFile.map(data=>{
      val dataArray = data.split(",")//trim去掉空格 有的话
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    //按照id传感器分组 把温度求和
    //.keyBy(0) //keyby之后才能聚合操作
    //  .sum(2)
    .keyBy("id")//没有办法分布式计算，在一个上进行聚合，没有好好利用资源
        .reduce((x,y)=> SensorReading(x.id,x.timestamp+1,y.temperature+10))


    dataStream.print()

    env.execute("TransformTest")
  }

}
