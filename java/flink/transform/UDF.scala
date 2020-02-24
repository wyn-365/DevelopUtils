package com.wang.transform

import com.wang.source.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * UDF
 *
 */
object UDF {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //为程序设置并行度
    //读取文件
    val streamFromFile = env.readTextFile("D:\\APP\\IDEA\\workplace\\HelloFlink\\src\\main\\resources\\sensor.txt")

    val dataStream = streamFromFile.map(data=>{
      val dataArray = data.split(",")//trim去掉空格 有的话
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })


    //传入一个函数函数
    dataStream.filter(new MyFilter()).print()


    env.execute("SplitAndSelect")
  }
}

//udf
class MyFilter()extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

//Rich udf
class MyMapper() extends RichMapFunction[SensorReading,String]{
  override def map(in: SensorReading): String = {
    "flink"
  }

  //创阿金的时候后执行  比如说hdfs等数据源的连接
  override def open(parameters: Configuration): Unit = super.open(parameters)
}