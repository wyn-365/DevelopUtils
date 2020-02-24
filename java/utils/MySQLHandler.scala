package com.wang.dmp.utils

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

object MySQLHandler {


  def save2db(resultDF: DataFrame,tblName:String,partition:Int = 2)={
    resultDF.coalesce(partition).write.mode(SaveMode.Overwrite)jdbc(
      ConfigHandler.url,
      tblName,
      ConfigHandler.dbProps
    )
  }

  //事务插入数据 一次性
  def saveBusinessTag(list: ListBuffer[(String,String)])={
      DB.localTx{implicit session =>{ //list
          list.foreach(tp =>{ //tuple
            SQL("replace into business_dict_30 values(?,?)").bind(tp._1,tp._2).update().apply()
          })
      }}
  }

  //查询商圈数据库
  def findBusinessBy(geoHashCode:String) ={
    val list = DB.readOnly{ implicit session =>
      SQL("SELECT business from business_dict_30 where geo_hash_code=?")
        .bind(geoHashCode)
        .map(rs => rs.string("business"))
        .list().apply()
    }
    if(list.size !=0){
      list.head
    }else{
      null
    }
  }
}
