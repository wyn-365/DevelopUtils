package com.wang.sparkmongo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by zx on 2017/10/8.
  * https://docs.mongodb.com/spark-connector/current/
  * https://docs.mongodb.com/spark-connector/current/scala-api/
  */
object MongoSparkRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("MongoSparkRDD")
      .setMaster("local[*]")
      //优先级是readPreference=secondaryPreferred
      //可以从从节点里面读取
      .set("spark.mongodb.input.uri", "mongodb://192.168.52.203:27200,192.168.52.204:27200,192.168.52.205:27200/mobike.bikes?readPreference=secondaryPreferred")
      //.set("spark.mongodb.output.uri", "mongodb://xiaoniu:123568@192.168.52.200:27017/mobike.reslut")
      //也可以写入mysql
    //创建sparkcontext(RDD,SparkCore)
    val sc = new SparkContext(conf)

    val docsRDD: MongoRDD[Document] = MongoSpark.load(sc)

//    val filtered: RDD[Document] = docsRDD.filter(doc => {
//      val age = doc.get("age")
//      if (age == null) {
//        false
//      } else {
//        val ageDouble = age.asInstanceOf[Double]
//        ageDouble >= 31
//      }
//    })

    //先过滤，filteredRDD，缓存（cache）

    //val pv = docsRDD.count()

    //val uv = docsRDD.map(doc => {
     // doc.getString("openid")
   // }).distinct().count()


    //println("pv: " + pv + " uv: " + uv)

    val r = docsRDD.collect()

    println(r.toBuffer) //打印一下

    //val filtered = docsRDD.withPipeline(Seq(Document.parse("{ $match: { age : { $gt : 31 } } }")))

    //println(filtered.collect().toBuffer)
    //val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{age: $i}")))

    //val r = filtered.collect()

    //println(r.toBuffer)

    //将计算好的结果保存到mongo中
    //MongoSpark.save(filtered)

    sc.stop()

    //    val spark = SparkSession.builder()
    //      .master("local")
    //      .appName("MongoSparkConnectorIntro")
    //      .config("spark.mongodb.input.uri", "mongodb://192.168.1.13:27200/niu.bi")
    //      //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
    //      .getOrCreate()
    //
    //    val df: DataFrame = MongoSpark.load(spark)
    //
    //    df.show()


  }
}
