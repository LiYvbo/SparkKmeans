package org.liyubo.spark.data

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liyubo on 2016/6/7.
 * 读取原始的传感器数据文件，包含了大量的混杂信息，取出需要的传感器环境信息列，并将其格式化后存入到文件中
 * 输入：指定的原始文件路径
 * 输出：格式化后的数据文件
 */
object DataFormat {

  /**
   * 主测试函数
   */
  def main(args: Array[String]) {
    val appname = "1. Format Data from Sensors"
    val master = "local"
    val path = "C:\\TempFolder\\testdata\\tb_sensorinfo_1.txt"

    //调用SC函数
    val sc = this.getSparkContext(appname,master)
    this.getDataSet(sc,path)
  }

  /**
   * 配置程序，并返回SparkContext
   * @param appname
   * @param master
   * @return
   */
  def getSparkContext(appname:String,master:String):SparkContext={
    //Spark程序的设置信息
    val conf = new SparkConf().setAppName(appname)
                              .setMaster(master)
    //创建SparkContext
    val sc = new SparkContext(conf)
    return sc
  }

  /**
   * 加载文件并提取数据
   * @param sc,path
   * @return
   */
  def getDataSet(sc:SparkContext,path:String):Unit = {
    val lines = sc.textFile(path)
    //取出相关的几列数据
    val split = lines.collect()
    val sensor = Array.ofDim[Double](split.length,5)
    val sensor_format = Array.ofDim[Double](split.length,5)
    val max = Array.ofDim[Double](5)
    val min = Array.ofDim[Double](5)
    for(i <- 0 until split.length){
      var record = split.apply(i)
      var rep_line = record.split("\t")
      var light = rep_line.apply(3).toDouble
      var air_temp = rep_line.apply(4).toDouble
      var air_humi = rep_line.apply(5).toDouble
      var soil_temp = rep_line.apply(6).toDouble
      var soil_humi = rep_line.apply(7).toDouble

      //将传感器数据全部加入二维数组
      sensor(i)(0) = light
      sensor(i)(1) = air_temp
      sensor(i)(2) = air_humi
      sensor(i)(3) = soil_temp
      sensor(i)(4) = soil_humi
//      println(light+"\t"+air_temp+"\t"+air_humi+"\t"+soil_temp+"\t"+soil_humi)
    }

    //打印传感器数据数组测试
//    for(i <- 0 until(sensor.length)){
//      for(j <- 0 until(sensor(i).length)){
//        print(sensor(i)(j)+"\t")
//
//      }
//      //分别对每一列数据进行规格化
//      println()
//    }

//    println("每一列的最大值和最小值")

    //获取列的最大值和最小值
    for(col <- 0 until 5){
      //获取最大最小元素
      for(row <- 0 until(split.length)) {
        //获取最大的元素
        if (max(col) <= sensor(row)(col)) {
          max(col) = sensor(row)(col)
        }
        //获取最小的元素
        if (min(col) >= sensor(row)(col)) {
          min(col) = sensor(row)(col)
        }
      }
    }

    //按列规格化数据,数据
    for(col <- 0 until 5){
      for(row <- 0 until(split.length)) {
        sensor_format(row)(col) = ((sensor(row)(col)-min(col))/(max(col)-min(col))).formatted("%.4f").toDouble
      }
    }

//    打印测试最大值和最小值
//    for(i <- 0 until 5){
//      print(max(i) + "\t" +min(i))
//      println()
//    }

//    打印格式化的传感器数据数组和写入文件存起来
//    for(row <- 0 until(sensor_format.length)){
//      for(col <- 0 until(sensor_format(row).length)){
//        print(sensor_format(row)(col)+"\t")
//      }
//      println()
//    }


    println("========================== 开始写入 =======================================")
    val writer = new PrintWriter(new File("C:\\TempFolder\\testdata\\sensor.txt"))

    for (row <- 0 until (sensor_format.length)) {
      for (col <- 0 until (sensor_format(row).length)) {
        writer.write(sensor_format(row)(col) + "\t")
      }
      writer.write("\n")
    }
    writer.close
    println("=========================写入完成！========================================")
    sc.stop
  }
}