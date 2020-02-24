package com.wang.transform
import com.wang.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 切分流数据  多流转换算子
 *
 */
object SplitAndSelect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //为程序设置并行度
    //读取文件
    val streamFromFile = env.readTextFile("D:\\APP\\IDEA\\workplace\\HelloFlink\\src\\main\\resources\\sensor.txt")

    val dataStream = streamFromFile.map(data=>{
      val dataArray = data.split(",")//trim去掉空格 有的话
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    val aggStream = dataStream.keyBy("id")
      .reduce((x,y)=> SensorReading(x.id,x.timestamp+1,y.temperature+10))

    //1.多流  按照温度的高低进行分流
    val splitStream = dataStream.split(data =>{
      if(data.temperature > 30) Seq("high") else Seq("low")
    })

    val high = splitStream.select("high")
    val low = splitStream.select("low")

    val all = splitStream.select("high","low")


    //2.合并成两条流
    val warning = high.map(data =>(data.id,data.temperature))
    val conectedStream = warning.connect(low)
    val coMapDataStream = conectedStream.map(
      warningData =>(warningData._1,warningData._2,"warning"),
      lowData =>(lowData.id,"healthy")
    )

    //3.数据结构要一样类型相同
    val unionStream = high.union(low)


    coMapDataStream.print("coMapDataStream")
    high.print("high")
    high.print("low")
    high.print("all")
   //P20 16:31


    env.execute("SplitAndSelect")
  }
}
