package org.liyubo.spark.dbops

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by liyubo on 2016/7/12.
 */
object MDDataStore {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("4. MD Data Restore").setMaster("local")
    val sc = new SparkContext(conf)

    // 创建数据库连接
    val dbc = "jdbc:mysql://localhost:3306/field_monitor?user=root&password=19911112"
    classOf[com.mysql.jdbc.Driver]
    val conn = DriverManager.getConnection(dbc)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

    //读取本地数据源，并拼接字符串
    //处理聚类0结果
    val rdd0 = sc.textFile("C:\\TempFolder\\testdata\\result-md\\dateset-0.txt").collect()

    //先删除
    var delete = conn.prepareStatement("delete from result_md where type=0")
    delete.execute()
    for(i<- 1 until(rdd0.length)){
      //去掉第一行信息行，以及每一行的首尾括号
      val type_0 = rdd0(i).replace("[","(").replace("]","")             //形如：111,2222,3333,333,222
      // 执行插入
      try {
        //后增加
        val insert = conn.prepareStatement("INSERT INTO result_md(light,temp,humi,soiltemp,soilhumi,type)  VALUES "+type_0+" ,0) ")
        insert.executeUpdate
      }
    }

    //处理聚类1结果
    val rdd1 = sc.textFile("C:\\TempFolder\\testdata\\result-md\\dateset-1.txt").collect()
    //先删除
    delete = conn.prepareStatement("delete from result_md where type=1")
    delete.execute()
    for(i<- 1 until(rdd1.length)){
      //去掉第一行信息行，以及每一行的首尾括号
      val type_1 = rdd1(i).replace("[","(").replace("]","")             //形如：111,2222,3333,333,222
      // 执行插入
      try {
        //后增加
        val insert = conn.prepareStatement("INSERT INTO result_md(light,temp,humi,soiltemp,soilhumi,type)  VALUES "+type_1+" ,1) ")
        insert.executeUpdate
      }

    }

    //处理聚类2结果
    val rdd2 = sc.textFile("C:\\TempFolder\\testdata\\result-md\\dateset-2.txt").collect()
    //先删除
    delete = conn.prepareStatement("delete from result_md where type=2")
    delete.execute()
    for(i<- 1 until(rdd2.length)){
      //去掉第一行信息行，以及每一行的首尾括号
      val type_2 = rdd2(i).replace("[","(").replace("]","")             //形如：111,2222,3333,333,222
      // 执行插入
      try {
        //后增加
        val insert = conn.prepareStatement("INSERT INTO result_md(light,temp,humi,soiltemp,soilhumi,type)  VALUES "+type_2+" ,2) ")
        insert.executeUpdate
      }

    }
    conn.close()

  }
}
