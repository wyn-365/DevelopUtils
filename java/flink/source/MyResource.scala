package com.wang.source
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * 自定义source
 */

object MyResource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.addSource(new SensorSource())

    stream1.print("stream1").setParallelism(1)

    env.execute("source test")
  }

}

//自定义SensorSource
class SensorSource() extends SourceFunction[SensorReading]{

  //定义一个flag 表示数据源是否正常运行
  var running:Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //生产数据 随机数生成器
    val rand = new Random()

    var curTemp = 1.to(10).map(
      i => ("sensor_"+i,60+rand.nextGaussian() * 20) //高斯分布随机数
    )

    //循环产生数据流
    while(running){
      //更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2+rand.nextGaussian())
      )

      //时间戳
      val culTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,culTime,t._2))//一条一条发送数据
      )

      //隔时间发送
      Thread.sleep(500)
    }


  }

  override def cancel(): Unit = {
    running = false//取消数据的生成
  }
}