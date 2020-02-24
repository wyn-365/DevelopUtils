package com.wang.wc

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 批处理wordcount程序
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个只需选哪个环境
     val env = ExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val inputPath = "D:\\APP\\IDEA\\workplace\\HelloFlink\\src\\main\\resources\\a.txt"
    val inputDataSet = env.readTextFile(inputPath)

    //切分得到数据
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
    wordCountDataSet.writeAsCsv("D:\\APP\\IDEA\\workplace\\HelloFlink\\src\\main\\resources\\result","\n")
    env.execute()
  }
}
