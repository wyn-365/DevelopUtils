package com.wang.window

import com.wang.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    //1.老老实实换成socket
    val stream = env.socketTextStream("192.168.52.206",7777)

    //2.转换操作
    val dataStream = stream.map(data=>{
      val dataArray = data.split(",")//trim去掉空格 有的话
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      .assignAscendingTimestamps(_.timestamp*1000)//升序数据
      .assignTimestampsAndWatermarks(new MyAssigner())

    //3.统计输出10s内的最小的温度值
    val minTempPerWindowStream = dataStream
      .map(data => (data.id,data.temperature))
      .keyBy(_._1)//id
      .timeWindow(Time.seconds(10))
      .reduce((data1,data2) => (data1._1,data1._2.min(data2._2)))//取最小值 增量聚合

    //打印输出
      minTempPerWindowStream.print("min temp")
      dataStream.print("datastream")

    env.execute("window test")
  }
}


class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{

  val bound = 60000
  var maxTs = Long.MinValue
  override def getCurrentWatermark: Watermark = new Watermark(maxTs-bound)

  override def extractTimestamp(element: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(element.timestamp*1000)
    element.timestamp*1000
  }
}