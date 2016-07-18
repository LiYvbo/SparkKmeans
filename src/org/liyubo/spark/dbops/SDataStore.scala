package org.liyubo.spark.dbops

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liyubo on 2016/7/12.
  */
object SDataStore {
   def main(args: Array[String]) {
     val conf = new SparkConf()
     conf.setAppName("4. Data Restore").setMaster("local")
     val sc = new SparkContext(conf)

     // 创建数据库连接
     val dbc = "jdbc:mysql://localhost:3306/field_monitor?user=root&password=19911112"
     classOf[com.mysql.jdbc.Driver]
     val conn = DriverManager.getConnection(dbc)
     val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

     //读取本地数据源，并拼接字符串
     val rdd = sc.textFile("C:\\TempFolder\\testdata\\data\\sensor.txt").collect()
     //清空原始数据表
     var delete = conn.prepareStatement("TRUNCATE source")
     delete.execute()
     for(i <- rdd){
       try{
         val source = i.replace(" ",",")
         val insert = conn.prepareStatement("INSERT INTO source(light,temp,humi,soiltemp,soilhumi) VALUES ("+source+")")
         insert.executeUpdate
       }
     }
     conn.close()
   }
 }
