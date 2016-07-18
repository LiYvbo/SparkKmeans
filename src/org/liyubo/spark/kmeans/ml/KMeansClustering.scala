package org.liyubo.spark.kmeans.ml

import java.io.{File, PrintWriter}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
 * Created by liyubo on 2016/6/13.
 * 基于Spark的机器学习库进行聚类测试，并将结果输出到文件
 * 输入：格式化后的传感器数据文件
 * 输出：分类好的数据文件和本次运行的聚类信息
 */

object KMeansClustering {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setAppName("2. ML K-means:").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val numClusters = 3
    val numIterations = 30
    val clusters = KMeans.train(parsedData,numClusters,numIterations)

    //修改输出形式，聚类数为3，分别为类别：0，1，2
    //计算测试数据分别属于那个簇类
    val collect = parsedData.map(v => clusters.predict(v)).collect

    var type0 = new ListBuffer[Vector]()
    var type1 = new ListBuffer[Vector]()
    var type2 = new ListBuffer[Vector]()
    //分别对数据进行聚类判断
    for(i <- 0 until(parsedData.collect().size)){
      if(clusters.predict(parsedData.collect().apply(i))==0){
        type0 += parsedData.collect().apply(i)
      }else if(clusters.predict(parsedData.collect().apply(i))==1){
        type1 += parsedData.collect().apply(i)
      }else if(clusters.predict(parsedData.collect().apply(i))==2){
        type2 += parsedData.collect().apply(i)
      }
    }

      /**
       * 在命令行输出，并将聚类结果存入文件
       */
    val result0 = new PrintWriter(new File("C:\\TempFolder\\testdata\\result-ml\\dateset-0.txt"))
    println("=================聚类0包含元素======================")
    result0.write("=================聚类0包含元素======================\n")
    for(i <-0 until(type0.size)){
      println(type0(i))
      result0.write(type0(i)+"\n")
    }
    result0.close

    val result1 = new PrintWriter(new File("C:\\TempFolder\\testdata\\result-ml\\dateset-1.txt"))
    println("=================聚类1包含元素======================")
    result1.write("=================聚类1包含元素======================\n")
    for(i <-0 until(type1.size)){
      println(type1(i))
      result1.write(type1(i)+"\n")
    }
    result1.close

    val result2 = new PrintWriter(new File("C:\\TempFolder\\testdata\\result-ml\\dateset-2.txt"))
    println("=================聚类2包含元素======================")
    result2.write("=================聚类2包含元素======================\n")
    for(i <-0 until(type2.size)){
      println(type2(i))
      result2.write(type2(i)+"\n")
    }
    result2.close

    val info = new PrintWriter(new File("C:\\TempFolder\\testdata\\result-ml\\cluster-info.txt"))
    //打印出中心点
    println("聚类中心:")
    info.write("聚类中心:"+"\n")
    for (i<-0 until clusters.clusterCenters.size) {
      println(clusters.clusterCenters(i));
      info.write(clusters.clusterCenters(i)+"\n")
    }

    val wssse = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = "+ wssse)
    info.write("Within Set Sum of Squared Errors = "+ wssse)
    info.close
    sc.stop()
  }
}