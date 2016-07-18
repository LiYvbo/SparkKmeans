package org.liyubo.spark.kmeans.optimistic

import breeze.linalg.{min, max}
import breeze.numerics.sqrt
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
 * Created by liyubo on 2016/6/16.
 * 按照论文所述算法进行聚类，将格式化后的传感器数据文件，聚为3类，并将结果输出到文件
 * 输入：格式化后的传感器数据文件路径
 * 输出：聚类结果的文件和聚类过程的信息
 */
object KMeansModified {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    /**
     * 程序基本设置
     */
    val conf = new SparkConf().setAppName("3. Optimization of K-Means").setMaster("local")
    val sc = new SparkContext(conf)

    /**
     * 读入格式化的文件并将单条数据转成Vector类型，多条数据组成Array类型----parseData[Vector]
     */
    val rdd = sc.textFile(args(0))
    var parsedDatardd = rdd.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    var parsedData = rdd.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).collect()

    /**
     * 实现优化的K-Means聚类过程
     */
    //定义三个List来存放聚类的向量点
    var type_0 = new ListBuffer[Vector]()
    var type_1 = new ListBuffer[Vector]()
    var type_2 = new ListBuffer[Vector]()

    //1. 定义一个常量WIDTH,其中，5为向量的维度
    val WIDTH: Double = sqrt(5) / 3


    var label_0 = 0.0
    var label_1 = WIDTH
    var label_2 = WIDTH * 2

    var flag = true
//    while(flag) {
      //2. 定义3个区间，用于实现聚类的个数和范围
      var intval_0 = Map[String, Double]("min" -> label_0, "max" -> WIDTH)
      var intval_1 = Map[String, Double]("min" -> label_1, "max" -> WIDTH * 2)
      var intval_2 = Map[String, Double]("min" -> label_2, "max" -> WIDTH * 3)

      //3. 访问整个数据集，对于每个数据点，计算出e=min{dpcl,dpcw}。其中，cw为最靠近该点的集群中心，Cl为其他的族群中心。
      val clusters = KMeans.train(parsedDatardd, 3, 1)
      //获取初次聚类的结果
      var center_0: Vector = clusters.clusterCenters(0)
      var center_1: Vector = clusters.clusterCenters(1)
      var center_2: Vector = clusters.clusterCenters(2)

      var closed_center: Vector = null //最靠近该点的族群中心
      var closed_center_away: Vector = null //次之靠近该点的族群中心
      var value_0: Double = 0.0
      var value_1: Double = 0.0
      var value_2: Double = 0.0
      //4. 初始化聚类，对于每个点将其映射到指定区间
      for (i <- 0 until (parsedData.size)) {
        if (!center_0.equals(center_1) && !center_0.equals(center_2) && !center_1.equals(center_2)) {
          value_0 = Vectors.sqdist(parsedData.apply(i), center_0)
          value_1 = Vectors.sqdist(parsedData.apply(i), center_1)
          value_2 = Vectors.sqdist(parsedData.apply(i), center_2)
          //4-1: 获取了距离每个点最近的族群中心和次之的族群中心点坐标
          if (value_0 > value_1 && value_1 > value_2) {
            closed_center = center_2
            closed_center_away = center_1
          } else if (value_0 > value_2 && value_2 > value_1) {
            closed_center = center_1
            closed_center_away = center_2
          } else if (value_1 > value_2 && value_2 > value_0) {
            closed_center = center_0
            closed_center_away = center_2
          } else if (value_1 > value_0 && value_0 > value_2) {
            closed_center = center_2
            closed_center_away = center_0
          } else if (value_2 > value_1 && value_1 > value_0) {
            closed_center = center_0
            closed_center_away = center_1
          } else if (value_2 > value_0 && value_0 > value_1) {
            closed_center = center_1
            closed_center_away = center_0
          }

          //4-2：计算每个点要跳到其他族群所需要的距离e=min(dpcl-dpcw)
          var e = sqrt(Vectors.sqdist(parsedData.apply(i), closed_center_away))-sqrt(Vectors.sqdist(parsedData.apply(i), closed_center))
          //4-3：判断数据点属于哪一个区间，将e映射到不同的区间
          if (e >= intval_0("min") && e <= intval_0("max")) {
            type_0 += parsedData.apply(i)
          } else if (e >= intval_1("min") && e <= intval_1("max")) {
            type_1 += parsedData.apply(i)
          } else if (e >= intval_2("min") && e <= intval_2("max")) {
            type_2 += parsedData.apply(i)
          }
        }
      }

      //5. 对于每个分类好的族群，计算新的聚类中心Cj和其与原聚类中心的偏差D=max(|CjCj'|)
      //5-1: 对于每一次分组结果，转成RDD，然后将其进行训练出结果
      var parsedData0 = sc.parallelize(type_0.toList)
      var parsedData1 = sc.parallelize(type_1.toList)
      var parsedData2 = sc.parallelize(type_2.toList)

      var clusters_0 = KMeans.train(parsedData0, 1, 1)
      var clusters_1 = KMeans.train(parsedData1, 1, 1)
      var clusters_2 = KMeans.train(parsedData2, 1, 1)

      //5-2: 计算新的聚类中心
      var new_center_0 = clusters_0.clusterCenters(0)
      var new_center_1 = clusters_1.clusterCenters(0)
      var new_center_2 = clusters_2.clusterCenters(0)

      //5-3: 计算新旧中心的偏差
      var a = min(Vectors.sqdist(new_center_0, center_0),Vectors.sqdist(new_center_0, center_1),Vectors.sqdist(new_center_0, center_2))
      var b = min(Vectors.sqdist(new_center_1, center_0),Vectors.sqdist(new_center_1, center_1),Vectors.sqdist(new_center_1, center_2))
      var c = min(Vectors.sqdist(new_center_2, center_0),Vectors.sqdist(new_center_2, center_1),Vectors.sqdist(new_center_2, center_2))
      var D = max(a,b,c)
      D = 2*sqrt(D)
      //5-4: 更新区间的标签
      label_0 = label_0 - D
      label_1 = label_1 - D
      label_2 = label_2 - D
      //5-5: 检出那些使标签值小于0的点，对于那些标签仍大于0的值，保持起数据，并将其分配到现有的族群中。对于那些标签小于0的值，从新进行分配
      //插入最终的List分类
      val class_0 = new ListBuffer[Vector]
      val class_1 = new ListBuffer[Vector]
      val class_2 = new ListBuffer[Vector]
      //5-6:不在跳出域的数值，将其压入最终的分组
      if (label_0 > 0) {
        class_0.++=(type_0)
        println("调用1！！！")
      } else if (label_1 > 0) {
        class_1.++=(type_1)
        println("调用2！！！")
      } else if (label_2 > 0) {
        class_2.++=(type_2)
        println("调用3！！！")
      } else if (label_0 > 0 && label_1 > 0 && label_2 > 0) {
        //所有结果均分类完成后，准备终止程序
        //打印出所有的结果，并退出程序
        println("跳出循环！！！")
        flag = false
      }

      else if (label_0 < 0 && label_1 < 0 && label_2 < 0) {
        //所有结果均分类完成后，准备终止程序
        //打印出所有的结果，并退出程序
        println("全部小于0,跳出循环！！！")
        flag = false
      }

      //5-7：将跳出域中的数值压入到重新检索的List中
      var parsedData_next = new ListBuffer[Vector]()
      if (label_0 < 0) {
        parsedData_next.++=(type_0)
      } else if (label_1 < 0) {
        parsedData_next.++=(type_1)
      } else if (label_2 < 0) {
        parsedData_next.++=(type_2)
      }

      parsedData = parsedData_next.toArray
      parsedDatardd = sc.makeRDD(parsedData)
    }
//  }
}